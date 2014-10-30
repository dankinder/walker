package dnscache

import (
	"net"
	"sync"
	"time"

	"github.com/dropbox/godropbox/container/lrucache"
)

//TODO:
//  - use a time-based cache instead of entry-capped, since we know we'll
//    need most of the recently-accessed domains and few of the aging entries
//  - consider not caching failures or doing any blacklisting here; the more
//    likely usecase will be to retry a few times (in which case we don't want
//    caching) and then not bother crawling this host at all

// Dial wraps the given dial function with Caching of DNS resolutions. When a
// hostname is found in the cache it will call the provided dial with the IP
// address instead of the hostname, so no DNS lookup need be performed. It will
// also cache DNS failures.
//
// If the given wrappedDial is nil, net.Dial will be automatically used.
func Dial(wrappedDial func(network, addr string) (net.Conn, error), maxEntries int) func(network, addr string) (net.Conn, error) {
	if wrappedDial == nil {
		wrappedDial = net.Dial
	}
	c := &dnsCache{
		wrappedDial: wrappedDial,
		cache:       lrucache.New(maxEntries),
	}
	return c.cachingDial
}

// dnsCache wraps a net.Dial-type function with it's own version that will
// cache DNS entries in an LRU cache.
type dnsCache struct {
	wrappedDial func(network, address string) (net.Conn, error)
	cache       *lrucache.LRUCache
	mu          sync.RWMutex
}

type hostrecord struct {
	ipaddr      string
	blacklisted bool
	err         error
	lastQuery   time.Time
}

func (c *dnsCache) cachingDial(network, addr string) (net.Conn, error) {
	mapEntryName := network + addr
	c.mu.RLock()
	if entry, ok := c.cache.Get(mapEntryName); ok {
		record := entry.(hostrecord)
		lastQueryTime := record.lastQuery
		if time.Since(lastQueryTime) > 5*time.Minute {
			c.mu.RUnlock()
			c.cacheHost(network, addr)
			c.mu.RLock()
			entry, _ = c.cache.Get(mapEntryName)
			record = entry.(hostrecord)
		}
		resolvedAddr := record.ipaddr
		if record.blacklisted {
			returnErr := record.err
			c.mu.RUnlock()
			return nil, returnErr
		} else {
			c.mu.RUnlock()
			return c.wrappedDial(network, resolvedAddr)
		}
	} else {
		c.mu.RUnlock()
		return c.cacheHost(network, addr)
	}
}

// cacheHost caches the DNS lookup for this host, overwriting any entry
// that may have previously existed.
func (c *dnsCache) cacheHost(network, addr string) (net.Conn, error) {
	mapEntryName := network + addr
	newConn, err := c.wrappedDial(network, addr)
	queryTime := time.Now()
	c.mu.Lock()
	if err != nil {
		c.cache.Set(mapEntryName, hostrecord{
			ipaddr:      "",
			blacklisted: true,
			err:         err,
			lastQuery:   queryTime,
		})
		c.mu.Unlock()
		return nil, err
	} else {
		remoteipaddr := newConn.RemoteAddr().String()
		c.cache.Set(mapEntryName, hostrecord{
			ipaddr:      remoteipaddr,
			blacklisted: false,
			err:         nil,
			lastQuery:   queryTime,
		})
		c.mu.Unlock()
		return newConn, nil
	}
}

// get returns the hostrecord associated with the passed network:address, if it exists.
// The second return value represents whether the record exists.
func (c *dnsCache) get(network, addr string) (hostrecord, bool) {
	key := network + addr
	c.mu.RLock()
	valinterface, ok := c.cache.Get(key)
	if valinterface == nil {
		c.mu.RUnlock()
		return hostrecord{}, ok
	}
	val := valinterface.(hostrecord)
	c.mu.RUnlock()
	return val, ok
}
