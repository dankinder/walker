package cassandra

import (
	"container/heap"
	"fmt"
	"math"
	"strings"
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

	// which UUIDs are queued up to be removed (And mutex to protect it).
	removedToks      map[gocql.UUID]bool
	removedToksMutex sync.Mutex

	// map of active UUIDs -- i.e. fetchers that are still alive
	activeToks map[gocql.UUID]time.Time
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
	d.removedToks = make(map[gocql.UUID]bool)
	d.activeToks = make(map[gocql.UUID]time.Time)

	d.minRecrawlDelta, err = time.ParseDuration(walker.Config.Dispatcher.MinLinkRefreshTime)
	if err != nil {
		panic(err) //Not going to happen, parsed in config
	}
	ttl, err := time.ParseDuration(walker.Config.Fetcher.ActiveFetchersTTL)
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

	d.removedToksMutex.Lock()
	delete(d.removedToks, tok)
	d.removedToksMutex.Unlock()
}

func (d *Dispatcher) updateActiveFetchersCache(qtok gocql.UUID) {
	// We have to loop until we get a good read of active_fetchers. We can't
	// risk accidentally identifying a running fetcher as dead.
	delete(d.activeToks, qtok)
	for {
		var tok gocql.UUID
		iter := d.db.Query(`SELECT tok FROM active_fetchers WHERE tok = ?`, qtok).Iter()
		for iter.Scan(&tok) {
			d.activeToks[tok] = time.Now()
		}
		err := iter.Close()
		if err == nil {
			return
		}

		log4go.Error("Failed to read active_fetchers: %v", err)
		time.Sleep(time.Second)
	}
}

func (d *Dispatcher) fetcherIsAlive(claimTok gocql.UUID) bool {
	zeroTok := gocql.UUID{}
	if claimTok == zeroTok {
		return true
	}

	// If the token is already queued up to be removed, you must
	// return true here so that cleanStrandedClaims is not called
	d.removedToksMutex.Lock()
	removed := d.removedToks[claimTok]
	d.removedToksMutex.Unlock()
	if removed {
		return true
	}

	// remove dead fetchers
	readTime, present := d.activeToks[claimTok]
	if !present || readTime.Before(time.Now().Add(-d.activeFetcherCachetime)) {
		d.updateActiveFetchersCache(claimTok)
		_, present := d.activeToks[claimTok]
		if !present {
			d.removedToksMutex.Lock()
			d.removedToks[claimTok] = true
			d.removedToksMutex.Unlock()
			return false
		}
	}

	return true
}

func (d *Dispatcher) domainIterator() {
	for {
		log4go.Debug("Starting new domain iteration")
		domainiter := d.db.Query(`SELECT dom, dispatched, claim_tok, excluded FROM domain_info`).Iter()

		var domain string
		var dispatched bool
		var claimTok gocql.UUID
		var excluded bool
		for domainiter.Scan(&domain, &dispatched, &claimTok, &excluded) {
			if d.quitSignaled() {
				close(d.domains)
				return
			}

			if !dispatched && !excluded {
				d.domains <- domain
			} else if !d.fetcherIsAlive(claimTok) {
				go d.cleanStrandedClaims(claimTok)
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

// createInsertAllColumns produces an insert statement that will usable to clone a CQL row. Arguments are:
//   (a) the table that the cloned rows are coming from
//   (b) An iterator that points to the set of rows the user plans to copy
// and returns:
//   (a) a string that can be used as a CQL insert statement for all of the columns of itr.
//   (b) The name of the columns that are included in the insert statement.
//
func createInsertAllColumns(table string, itr *gocql.Iter) (string, []string) {
	cols := itr.Columns()
	colHeaders := []string{}
	questions := []string{}
	for _, c := range cols {
		colHeaders = append(colHeaders, c.Name)
		questions = append(questions, "?")
	}
	insert := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`,
		table,
		strings.Join(colHeaders, ","),
		strings.Join(questions, ","))
	return insert, colHeaders
}

// correctURLNormalization will verify that u is normalized. This method always returns the normalized link. If this
// method finds that it's argument url is NOT normalized then the Datastore will be updated to reflect the normalized
// link.
func (d *Dispatcher) correctURLNormalization(u *walker.URL) *walker.URL {
	c := u.NormalizedForm()
	if c == nil {
		return u
	}

	log4go.Debug("correctURLNormalization correcting %v --> %v", u, c)

	// Grab primary keys of old and new urls
	dom, subdom, path, proto, _, err := u.PrimaryKey()
	if err != nil {
		log4go.Error("correctURLNormalization error; can't get primary key for URL %v: %v", u.URL, err)
		return u
	}
	newdom, newsubdom, newpath, newproto, _, err := c.PrimaryKey()
	if err != nil {
		log4go.Error("correctURLNormalization error; can't get NEW primary key for URL %v: %v", u.URL, err)
		return u
	}

	// Create a new domain_info if needed. XXX: note that currently old domain_infos are left alone, since we
	// can't tell easily if they're still being used.
	if dom != newdom {
		log4go.Debug("correctURLNormalization adding domain_info entry for %q (derived from %q)", newdom, dom)
		// Grab all the data for the domain in question
		mp := map[string]interface{}{}
		itr := d.db.Query(`SELECT * FROM domain_info WHERE dom = ?`, dom).Iter()
		if !itr.MapScan(mp) {
			log4go.Error("correctURLNormalization error; Failed to select from domain_info for URL %v", u.URL)
			return u
		}
		err := itr.Close()
		if err != nil {
			log4go.Error("correctURLNormalization error; Failed to select from domain_info for URL %v: iter err %v",
				u.URL, err)
		}

		// Copy the data for old into new
		insert, colHeaders := createInsertAllColumns("domain_info", itr)
		vals := []interface{}{}
		mp["dom"] = newdom
		for _, head := range colHeaders {
			vals = append(vals, mp[head])
		}
		err = d.db.Query(insert, vals...).Exec()
		if err != nil {
			log4go.Error("correctURLNormalization error; Failed to insert into domain_info for URL %v: %v", u.URL, err)
			return u
		}
	}

	// Create read iterator
	read := `SELECT * FROM links WHERE dom = ? AND subdom = ? AND proto = ? AND path = ?`
	itr := d.db.Query(read, dom, subdom, proto, path).Iter()

	// Use the read iterator to fashion a generic insert statement to move all fields from one primary key
	// to another.
	insert, colHeaders := createInsertAllColumns("links", itr)

	// Now loop through the old rows, copying them (with slight modification) to the new rows NOTE: we do NOT hardcode
	// the column names in this algorithm in order to make this code resilient against  adding NON-PRIMARY-KEY columns.
	mp := map[string]interface{}{}
	for itr.MapScan(mp) {
		mp["dom"] = newdom
		mp["subdom"] = newsubdom
		mp["path"] = newpath
		mp["proto"] = newproto

		vals := []interface{}{}
		for _, head := range colHeaders {
			vals = append(vals, mp[head])
		}

		err := d.db.Query(insert, vals...).Exec()
		if err != nil {
			log4go.Error("correctURLNormalization error; Failed to insert for URL %v: %v", u.URL, err)
			return u
		}
	}
	err = itr.Close()
	if err != nil {
		log4go.Error("correctURLNormalization error; Failed to insert for URL %v: %v", u.URL, err)
		return u
	}

	// Now clobber the old rows
	del := `DELETE FROM links WHERE dom = ? AND subdom = ? AND proto = ? AND path = ?`
	err = d.db.Query(del, dom, subdom, proto, path).Exec()
	if err != nil {
		log4go.Error("correctURLNormalization error; Failed to delete for URL %v: %v", u.URL, err)
		return u
	}

	return c
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

		if walker.Config.Dispatcher.CorrectLinkNormalization {
			u = d.correctURLNormalization(u)
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
