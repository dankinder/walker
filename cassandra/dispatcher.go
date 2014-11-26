package cassandra

import (
	"container/heap"
	"fmt"
	"math"
	"sync"
	"time"

	"code.google.com/p/log4go"
	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

// Dispatcher analyzes what we've crawled so far (generally on a per-domain
// basis) and updates the database. At minimum this means generating new
// segments to crawl in the `segments` table, but it can also mean updating
// domain_info if we find out new things about a domain.
//
// This dispatcher has been designed to run simultaneously with the
// fetchmanager. Fetchers and dispatchers claim domains in Cassandra, so the
// dispatcher can operate on the domains not currently being crawled (and vice
// versa).
type Dispatcher struct {
	cf *gocql.ClusterConfig
	db *gocql.Session

	domains chan string   // For passing domains to generate to worker goroutines
	quit    chan struct{} // Channel to close to stop the dispatcher (used by `Stop()`)

	// synchronizes when all generator routines have exited, so
	// `StopDispatcher()` can wait until all processing is done
	finishWG sync.WaitGroup

	// synchronizes generators that are currently working, so we can wait for
	// them to finish before we start a new domain iteration
	generatingWG sync.WaitGroup

	// do not dispatch any link that has been crawled within this amount of
	// time; set by dispatcher.min_link_refresh_time config parameter
	minRecrawlDelta time.Duration

	// Age at at which an active_fetcher cache entry is considered stale
	activeFetcherCachetime time.Duration

	// Sleep this long between domain iterations;
	// set by dispatcher.dispatch_interval config parameter
	dispatchInterval time.Duration
}

func (d *Dispatcher) StartDispatcher() error {
	log4go.Info("Starting CassandraDispatcher")
	d.cf = GetConfig()
	var err error
	d.db, err = d.cf.CreateSession()
	if err != nil {
		return fmt.Errorf("Failed to create cassandra session: %v", err)
	}

	d.quit = make(chan struct{})
	d.domains = make(chan string)

	d.minRecrawlDelta, err = time.ParseDuration(walker.Config.Dispatcher.MinLinkRefreshTime)
	if err != nil {
		panic(err) //Not going to happen, parsed in config
	}
	ttl, err := time.ParseDuration(walker.Config.Fetcher.ActiveFetchersTtl)
	if err != nil {
		panic(err) //Not going to happen, parsed in config
	}

	d.dispatchInterval, err = time.ParseDuration(walker.Config.Dispatcher.DispatchInterval)
	if err != nil {
		panic(err) // Should not happen since it is parsed at config load
	}
	d.activeFetcherCachetime = time.Duration(float32(ttl) * walker.Config.Fetcher.ActiveFetchersCacheratio)

	for i := 0; i < walker.Config.Dispatcher.NumConcurrentDomains; i++ {
		d.finishWG.Add(1)
		go func() {
			d.generateRoutine()
			d.finishWG.Done()
		}()
	}

	d.domainIterator()
	return nil
}

func (d *Dispatcher) StopDispatcher() error {
	log4go.Info("Stopping CassandraDispatcher")
	close(d.quit)
	d.finishWG.Wait()
	d.db.Close()
	return nil
}

func (d *Dispatcher) cleanStrandedClaims(tok gocql.UUID) {
	tag := "cleanStrandedClaims"
	var err error
	db := d.db
	iter := db.Query(`SELECT dom FROM domain_info WHERE claim_tok = ?`, tok).Iter()
	var domain string
	ecount := 0
	for iter.Scan(&domain) && ecount < 5 {
		err = db.Query(`DELETE FROM segments WHERE dom = ?`, domain).Exec()
		if err != nil {
			log4go.Error("%s failed to DELETE from segments: %v", tag, err)
			ecount++
		}

		err = db.Query(`UPDATE domain_info
						SET 
							claim_tok = 00000000-0000-0000-0000-000000000000,
							dispatched = false
						WHERE dom = ?`, domain).Exec()
		if err != nil {
			log4go.Error("%s failed to UPDATE domain_info: %v", tag, err)
			ecount++
		}
	}
	err = iter.Close()
	if err != nil {
		log4go.Error("%s failed to find domain: %v", tag, err)
	}
}

func (d *Dispatcher) buildActiveFetchersCache() map[gocql.UUID]time.Time {
	mp := map[gocql.UUID]time.Time{}
	for {
		iter := d.db.Query(`SELECT tok FROM active_fetchers`).Iter()
		var uuid gocql.UUID
		now := time.Now()
		for iter.Scan(&uuid) {
			mp[uuid] = now
		}
		err := iter.Close()
		if err == nil {
			return mp
		}

		log4go.Error("Failed to read active_fetchers table: %v", err)
		time.Sleep(time.Second)
	}
}

func (d *Dispatcher) updateActiveFetchersCache(qtok gocql.UUID, mp map[gocql.UUID]time.Time) {
	// We have to loop until we get a good read of active_fetchers. We can't
	// risk accidentally identifying a running fetcher as dead.
	for {
		delete(mp, qtok)
		var tok gocql.UUID
		iter := d.db.Query(`SELECT tok FROM active_fetchers WHERE tok = ?`, qtok).Iter()
		for iter.Scan(&tok) {
			mp[tok] = time.Now()
		}
		err := iter.Close()
		if err == nil {
			return
		}

		log4go.Error("Failed to read active_fetchers: %v", err)
		time.Sleep(time.Second)
	}
}

func (d *Dispatcher) domainIterator() {
	goodToks := d.buildActiveFetchersCache()
	zeroTok := gocql.UUID{}

	for {
		log4go.Debug("Starting new domain iteration")
		domainiter := d.db.Query(`SELECT dom, dispatched, claim_tok, excluded FROM domain_info`).Iter()

		var domain string
		var dispatched bool
		var claimTok gocql.UUID
		var excluded bool
		removeToks := map[gocql.UUID]bool{}
		for domainiter.Scan(&domain, &dispatched, &claimTok, &excluded) {
			if d.quitSignaled() {
				close(d.domains)
				return
			}

			if !dispatched && !excluded {
				d.domains <- domain
			} else if claimTok != zeroTok && !removeToks[claimTok] {
				// remove dead fetchers
				readTime, present := goodToks[claimTok]
				if !present || readTime.Before(time.Now().Add(-d.activeFetcherCachetime)) {
					d.updateActiveFetchersCache(claimTok, goodToks)
					_, present := goodToks[claimTok]
					if !present {
						removeToks[claimTok] = true
						go d.cleanStrandedClaims(claimTok)
					}
				}
			}
		}

		if err := domainiter.Close(); err != nil {
			log4go.Error("Error iterating domains from domain_info: %v", err)
		}
		d.generatingWG.Wait()

		// Check for quit signal right away, otherwise if there are no domains
		// to claim and the dispatchInterval is 0, then the dispatcher will
		// never quit
		if d.quitSignaled() {
			close(d.domains)
			return
		}

		endSleep := time.Now().Add(d.dispatchInterval)
		for time.Now().Before(endSleep) {
			if d.quitSignaled() {
				close(d.domains)
				return
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// quitSignaled returns true if a value was passed down the quit channel. This
// should only be called once.
func (d *Dispatcher) quitSignaled() bool {
	select {
	case <-d.quit:
		log4go.Debug("Domain iterator signaled to stop")
		return true
	default:
		return false
	}
}

func (d *Dispatcher) generateRoutine() {
	for domain := range d.domains {
		d.generatingWG.Add(1)
		if err := d.generateSegment(domain); err != nil {
			log4go.Error("error generating segment for %v: %v", domain, err)
		}
		d.generatingWG.Done()
	}
	log4go.Debug("Finishing generateRoutine")
}

//
// Some mathy type functions used in generateSegment
//
func imin(l int, r int) int {
	if l < r {
		return l
	} else {
		return r
	}
}

func round(f float64) int {
	abs := math.Abs(f)
	sign := f / abs
	floor := math.Floor(abs)
	if abs-floor >= 0.5 {
		return int(sign * (floor + 1))
	} else {
		return int(sign * floor)
	}
}

//
// Cell captures all the information for a link in the generateSegments method.
// Every cell generated in that method shares the same domain (hence we don't
// store the domain in the struct).
//
type cell struct {
	subdom, path, proto string
	crawl_time          time.Time
	getnow              bool
}

// 2 cells are equivalent if their full link renders to the same string.
func (c *cell) equivalent(other *cell) bool {
	return c.path == other.path &&
		c.subdom == other.subdom &&
		c.proto == other.proto
}

//
// PriorityUrl is a heap of URLs, where the next element Pop'ed off the list
// points to the oldest (as measured by LastCrawled) element in the list. This
// class is designed to be used with the container/heap package. This type is
// currently only used in generateSegments
//
type PriorityUrl []*walker.URL

func (pq PriorityUrl) Len() int {
	return len(pq)
}

func (pq PriorityUrl) Less(i, j int) bool {
	return pq[i].LastCrawled.Before(pq[j].LastCrawled)
}

func (pq PriorityUrl) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityUrl) Push(x interface{}) {
	*pq = append(*pq, x.(*walker.URL))
}

func (pq *PriorityUrl) Pop() interface{} {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}

// generateSegment reads links in for this domain, generates a segment for it,
// and inserts the domain into domains_to_crawl (assuming a segment is ready to
// go)
func (d *Dispatcher) generateSegment(domain string) error {
	log4go.Info("Generating a crawl segment for %v", domain)

	//
	// Three lists to hold the 3 link types
	//
	var getNowLinks []*walker.URL    // links marked getnow
	var uncrawledLinks []*walker.URL // links that haven't been crawled
	var crawledLinks PriorityUrl     // already crawled links, oldest links out first
	heap.Init(&crawledLinks)

	// cell push will push the argument cell onto one of the three link-lists.
	// logs failure if CreateURL fails.
	var now = time.Now()
	var limit = walker.Config.Dispatcher.MaxLinksPerSegment
	cell_push := func(c *cell) {
		u, err := walker.CreateURL(domain, c.subdom, c.path, c.proto, c.crawl_time)
		if err != nil {
			log4go.Error("CreateURL: " + err.Error())
			return
		}

		if c.getnow {
			getNowLinks = append(getNowLinks, u)
		} else if c.crawl_time.Equal(walker.NotYetCrawled) {
			if len(uncrawledLinks) < limit {
				uncrawledLinks = append(uncrawledLinks, u)
			}
		} else {
			// Was this link crawled less than MinLinkRefreshTime?
			if c.crawl_time.Add(d.minRecrawlDelta).Before(now) {
				heap.Push(&crawledLinks, u)
			}
		}

		return
	}

	//
	// Do the scan, and populate the 3 lists
	//

	// Making this query consistency = One ensures that when we do this
	// potentially massive read, the cassandra nodes don't have to waste big
	// IO/Network verifying the data is consistent between a Quorum of nodes.
	// The only risk is: if a node is down and does not receive some link
	// writes, then comes back up and is read for this query it may be missing
	// some of the newly crawled links. This is unlikely and seems acceptable.
	q := d.db.Query(`SELECT subdom, path, proto, time, getnow
						FROM links WHERE dom = ?`, domain)
	q.Consistency(gocql.One)

	var start = true
	var finish = true
	var current cell
	var previous cell
	iter := q.Iter()
	for iter.Scan(&current.subdom, &current.path, &current.proto, &current.crawl_time, &current.getnow) {
		if start {
			previous = current
			start = false
		}

		// IMPL NOTE: So the trick here is that, within a given domain, the entries
		// come out so that the crawl_time increases as you iterate. So in order to
		// get the most recent link, simply take the last link in a series that shares
		// dom, subdom, path, and protocol
		if !current.equivalent(&previous) {
			cell_push(&previous)
		}

		previous = current

		if len(getNowLinks) >= limit {
			finish = false
			break
		}
	}
	// Check !start here because we don't want to push if we queried 0 links
	if !start && finish {
		cell_push(&previous)
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("error selecting links for %v: %v", domain, err)
	}

	//
	// Merge the 3 link types
	//
	var links []*walker.URL
	links = append(links, getNowLinks...)

	numRemain := limit - len(links)
	if numRemain > 0 {
		refreshDecimal := walker.Config.Dispatcher.RefreshPercentage / 100.0
		idealCrawled := round(refreshDecimal * float64(numRemain))
		idealUncrawled := numRemain - idealCrawled

		for i := 0; i < idealUncrawled && len(uncrawledLinks) > 0 && len(links) < limit; i++ {
			links = append(links, uncrawledLinks[0])
			uncrawledLinks = uncrawledLinks[1:]
		}

		for i := 0; i < idealCrawled && crawledLinks.Len() > 0 && len(links) < limit; i++ {
			links = append(links, heap.Pop(&crawledLinks).(*walker.URL))
		}

		for len(uncrawledLinks) > 0 && len(links) < limit {
			links = append(links, uncrawledLinks[0])
			uncrawledLinks = uncrawledLinks[1:]
		}

		for crawledLinks.Len() > 0 && len(links) < limit {
			links = append(links, heap.Pop(&crawledLinks).(*walker.URL))
		}
	}

	//
	// Got any links
	//
	if len(links) == 0 {
		log4go.Info("No links to dispatch for %v", domain)
		return nil
	}

	//
	// Insert into segments
	//
	for _, u := range links {
		log4go.Debug("Inserting link in segment: %v", u.String())
		dom, subdom, err := u.TLDPlusOneAndSubdomain()
		if err != nil {
			log4go.Error("generateSegment not inserting %v: %v", u, err)
			return err
		}
		err = d.db.Query(`INSERT INTO segments
			(dom, subdom, path, proto, time)
			VALUES (?, ?, ?, ?, ?)`,
			dom, subdom, u.RequestURI(), u.Scheme, u.LastCrawled).Exec()
		if err != nil {
			log4go.Error("Failed to insert link (%v), error: %v", u, err)
		}
	}

	//
	// Update dispatched flag
	//
	err := d.db.Query(`UPDATE domain_info SET dispatched = true WHERE dom = ?`, domain).Exec()
	if err != nil {
		return fmt.Errorf("error inserting %v to domains_to_crawl: %v", domain, err)
	}
	log4go.Info("Generated segment for %v (%v links)", domain, len(links))

	return nil
}
