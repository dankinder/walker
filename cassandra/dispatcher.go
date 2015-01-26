package cassandra

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
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

	// If true, this field signals that this dispatcher run should quit as soon as all
	// available work is done.
	oneShotIterations int
}

// StartDispatcher starts the dispatcher
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

	d.finishWG.Add(1)
	go func() {
		d.pollMaxPriority()
		d.finishWG.Done()
	}()

	d.domainIterator()
	return nil
}

func (d *Dispatcher) oneShot(iterations int) error {
	if iterations <= 0 {
		return fmt.Errorf("Argument to oneShot must be > 0")
	}
	d.oneShotIterations = iterations
	err := d.StartDispatcher()
	if err != nil {
		d.StopDispatcher()
		return err
	}

	return d.StopDispatcher()
}

// StopDispatcher stops the dispatcher.
func (d *Dispatcher) StopDispatcher() error {
	log4go.Info("Stopping CassandraDispatcher")
	close(d.quit)
	d.finishWG.Wait()
	d.db.Close()
	return nil
}

func (d *Dispatcher) pollMaxPriority() {
	// Set the loop interval
	loopPeriod, err := time.ParseDuration("60s")
	if err != nil {
		panic(err)
	}

	dispatch_interval, err := time.ParseDuration(walker.Config.Dispatcher.DispatchInterval)
	if err != nil {
		panic(err)
	}
	if loopPeriod < dispatch_interval {
		loopPeriod = dispatch_interval
	}

	// Loop forever
	timer := time.NewTimer(loopPeriod)
	max_priority := "max_priority"
	for {
		var err error
		start := time.Now()
		iter := d.db.Query(`SELECT priority FROM domain_info`).Iter()
		max := -1
		prio := 0
		scansPerQuit := 10
		count := 0
		for iter.Scan(&prio) {
			if prio > max {
				max = prio
			}
			count++
			if (count % scansPerQuit) == 0 {
				select {
				case <-d.quit:
					goto LOOP
				default:
				}
			}
		}
		err = iter.Close()
		if err != nil {
			log4go.Error("pollMaxPriority failed to fetch all priorities: %v", err)
			goto LOOP
		}
		if max < 0 {
			goto LOOP
		}

		err = d.db.Query("INSERT INTO walker_globals (key, val) VALUES (?, ?)", max_priority, max).Exec()
		if err != nil {
			log4go.Error("pollMaxPriority failed to insert into walker_globals: %v", err)
			goto LOOP
		}

	LOOP:
		timer.Reset(loopPeriod - time.Since(start))
		select {
		case <-d.quit:
			return
		case <-timer.C:
		}
	}
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
	iteration := 0
	for {
		iteration++
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
				if d.oneShotIterations == 0 {
					go d.cleanStrandedClaims(claimTok)
				} else {
					d.cleanStrandedClaims(claimTok)
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
		osi := d.oneShotIterations
		if (osi > 0 && iteration >= osi) || d.quitSignaled() {
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

// Cell captures all the information for a link in the generateSegments method.
// Every cell generated in that method shares the same domain (hence we don't
// store the domain in the struct).
type cell struct {
	subdom, path, proto string
	crawlTime           time.Time
	getnow              bool
	fnvText             int64
}

// equivalent checks if the full link string form of 2 cells could be the same
func (c *cell) equivalent(other *cell) bool {
	return c.path == other.path &&
		c.subdom == other.subdom &&
		c.proto == other.proto
}

// createInsertAllColumns produces an insert statement that will usable to clone a CQL row. Arguments are:
//   (a) the table that the cloned rows are coming from
//   (b) An iterator that points to the set of rows the user plans to copy
// and returns:
//   (a) a string that can be used as a CQL insert statement for all of the columns of itr.
//   (b) The name of the columns that are included in the insert statement.
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

func (d *Dispatcher) generateRoutine() {
	generator := &SegmentGenerator{DB: d.db}
	for domain := range d.domains {
		d.generatingWG.Add(1)
		if err := generator.Generate(domain); err != nil {
			log4go.Error("error generating segment for %v: %v", domain, err)
		}
		d.generatingWG.Done()
	}
	log4go.Debug("Finishing generateRoutine")
}

// SegmentGenerator is the dispatcher component for generating a segment of
// links for an individual domain. See the Generate() function.
type SegmentGenerator struct {
	// A DB handle for the generator to use. Should be provided when
	// constructing a SegmentGenerator
	DB *gocql.Session

	// do not dispatch any link that has been crawled within this amount of
	// time; set by dispatcher.min_link_refresh_time config parameter
	minRecrawlDelta time.Duration

	// How long do we wait before retrying a domain that didn't have any links.
	emptyDispatchRetryInterval time.Duration

	// the current domain being generated
	domain string

	// links marked getnow
	getNowLinks LinkList
	// links that haven't been crawled
	uncrawledLinks LinkList
	// already crawled links, oldest links out first
	crawledLinks LinkList

	// Count of the total number of links in this domain
	totalLinksCount int
	// Count of the links not yet crawled in this domain
	uncrawledLinksCount int

	// after analysis, the links we actually want to put in the segment
	linksToDispatch []*LinkInfo
}

// LinkList is a list of LinkInfos that implements sort.Interface, so we can
// easily sort and deduplicate it
type LinkList []*LinkInfo

func (l LinkList) Len() int           { return len(l) }
func (l LinkList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l LinkList) Less(i, j int) bool { return l[i].URL.Path < l[j].URL.Path }

// Uniq assumes this list of links is sorted and gets rid of identical links
func (l LinkList) Uniq() {
	deduped := make(LinkList, 0, len(l))
	if len(l) > 0 {
		deduped = append(deduped, l[0])
	}
	for _, link := range l {
		if !link.URL.EqualIgnoreLastCrawled(deduped[len(deduped)-1].URL) {
			deduped = append(deduped, link)
		}
	}
	if len(l) != len(deduped) {
		log4go.Debug("Deleted duplicate links after filter (%v => %v items)", len(l), len(deduped))
		log4go.Debug("List before deduplication:")
		for _, link := range l {
			log4go.Debug("%v", link.URL)
		}
		log4go.Debug("List after deduplication:")
		for _, link := range deduped {
			log4go.Debug("%v", link.URL)
		}
	}
	l = deduped
}

// reset zeroes instance data for another Generate run
func (sg *SegmentGenerator) reset() {
	var err error
	sg.minRecrawlDelta, err = time.ParseDuration(walker.Config.Dispatcher.MinLinkRefreshTime)
	if err != nil {
		panic(err)
	}
	sg.emptyDispatchRetryInterval, err = time.ParseDuration(walker.Config.Dispatcher.EmptyDispatchRetryInterval)
	if err != nil {
		panic(err)
	}

	sg.getNowLinks = []*LinkInfo{}
	sg.uncrawledLinks = []*LinkInfo{}
	sg.crawledLinks = []*LinkInfo{}
	sg.totalLinksCount = 0
	sg.uncrawledLinksCount = 0
	sg.linksToDispatch = []*LinkInfo{}
}

// Generate reads links in for this domain, generates a segment for it, and
// inserts the domain into domains_to_crawl (assuming a segment is ready to go)
func (sg *SegmentGenerator) Generate(domain string) error {
	sg.reset()
	sg.domain = domain

	if sg.dispatchedEmptyRecently() {
		log4go.Debug("Domain %v recently dispatched with no links, not generating segment again", domain)
		return nil
	}
	log4go.Info("Generating a crawl segment for %v", domain)

	if err := sg.collectLinks(); err != nil {
		return err
	}
	sg.filterLinksByDuplicateContent()
	sg.buildLinksToDispatch()
	if err := sg.insertSegment(); err != nil {
		return err
	}

	log4go.Info("Generated segment for %v (%v links)", domain, len(sg.linksToDispatch))
	return nil
}

// dispatchedEmptyRecently returns true if this given domain was dispatched
// empty (meaning no links were chosen to be crawled so no segment was
// generated) within the past dispatch_retry_interval (see walker.yaml). This
// indicates that should not bother trying to dispatch it again yet.
func (sg *SegmentGenerator) dispatchedEmptyRecently() bool {
	var lastDispatch, lastEmptyDispatch time.Time
	err := sg.DB.Query("SELECT last_dispatch, last_empty_dispatch FROM domain_info WHERE dom = ?",
		sg.domain).Scan(&lastDispatch, &lastEmptyDispatch)
	if err != nil {
		log4go.Error("Failed to read last_dispatch and last_empty_dispatch for %q: %v", sg.domain, err)
		return true
	}
	if lastEmptyDispatch.After(lastDispatch) && time.Since(lastEmptyDispatch) < sg.emptyDispatchRetryInterval {
		return true
	}
	return false
}

// collectLinks scans the links table for the current domain and populates our
// link lists
func (sg *SegmentGenerator) collectLinks() error {
	start := time.Now()

	// Making this query consistency = One ensures that when we do this
	// potentially massive read, the cassandra nodes don't have to waste big
	// IO/Network verifying the data is consistent between a Quorum of nodes.
	// The only risk is: if a node is down and does not receive some link
	// writes, then comes back up and is read for this query it may be missing
	// some of the newly crawled links. This is unlikely and seems acceptable.
	q := sg.DB.Query(`SELECT subdom, path, proto, time, getnow, fnv_txt
						FROM links WHERE dom = ?`, sg.domain)
	q.Consistency(gocql.One)

	var scanStarted = false
	var scanFinished = true
	var current cell
	var previous cell
	iter := q.Iter()
	for iter.Scan(&current.subdom, &current.path, &current.proto, &current.crawlTime, &current.getnow, &current.fnvText) {
		if !scanStarted {
			previous = current
			scanStarted = true
		}

		// IMPL NOTE: So the trick here is that, within a given domain, the entries
		// come out so that the crawlTime increases as you iterate. So in order to
		// get the most recent link, simply take the last link in a series that shares
		// dom, subdom, path, and protocol
		if !current.equivalent(&previous) {
			sg.cellPush(&previous)
		}

		previous = current

		if len(sg.getNowLinks) >= walker.Config.Dispatcher.MaxLinksPerSegment {
			scanFinished = false
			break
		}
	}
	// Check scanStarted here because we don't want to push if we queried 0 links
	if scanStarted && scanFinished {
		sg.cellPush(&previous)
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("error selecting links for %v: %v", sg.domain, err)
	}

	log4go.Debug("Collected links for %v in %v", sg.domain, time.Since(start))
	return nil
}

// cellPush will push the argument cell onto one of the three link-lists.
// logs failure if CreateURL fails. It also keeps track of total and uncrawled
// links by incrementing sg.linksCount and sg.uncrawledLinksCount
func (sg *SegmentGenerator) cellPush(c *cell) {
	sg.totalLinksCount++
	if c.crawlTime.Equal(walker.NotYetCrawled) {
		sg.uncrawledLinksCount++
	}

	u, err := walker.CreateURL(sg.domain, c.subdom, c.path, c.proto, c.crawlTime)
	if err != nil {
		log4go.Error("CreateURL: " + err.Error())
		return
	}

	if walker.Config.Dispatcher.CorrectLinkNormalization {
		u = sg.correctURLNormalization(u)
	}

	l := &LinkInfo{
		URL:                u,
		FnvTextFingerprint: c.fnvText,
	}

	if c.getnow {
		sg.getNowLinks = append(sg.getNowLinks, l)
	} else if c.crawlTime.Equal(walker.NotYetCrawled) {
		if len(sg.uncrawledLinks) < walker.Config.Dispatcher.MaxLinksPerSegment {
			sg.uncrawledLinks = append(sg.uncrawledLinks, l)
		}
	} else {
		// Was this link crawled less than MinLinkRefreshTime?
		if c.crawlTime.Add(sg.minRecrawlDelta).Before(time.Now()) {
			sg.crawledLinks = append(sg.crawledLinks, l)
		}
	}

	return
}

// correctURLNormalization will verify that u is normalized. This method always
// returns the normalized link. If this method finds that it's argument url is
// NOT normalized then the Datastore will be updated to reflect the normalized
// link.
func (sg *SegmentGenerator) correctURLNormalization(u *walker.URL) *walker.URL {
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
		itr := sg.DB.Query(`SELECT * FROM domain_info WHERE dom = ?`, dom).Iter()
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
		err = sg.DB.Query(insert, vals...).Exec()
		if err != nil {
			log4go.Error("correctURLNormalization error; Failed to insert into domain_info for URL %v: %v", u.URL, err)
			return u
		}
	}

	// Create read iterator
	read := `SELECT * FROM links WHERE dom = ? AND subdom = ? AND proto = ? AND path = ?`
	itr := sg.DB.Query(read, dom, subdom, proto, path).Iter()

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

		err := sg.DB.Query(insert, vals...).Exec()
		if err != nil {
			log4go.Error("correctURLNormalization error; Failed to insert for URL %v: %v", u.URL, err)
			return u
		}

		// MapScan will choke if you don't clear this map before re-using it.
		mp = map[string]interface{}{}
	}
	err = itr.Close()
	if err != nil {
		log4go.Error("correctURLNormalization error; Failed to insert for URL %v: %v", u.URL, err)
		return u
	}

	// Now clobber the old rows
	del := `DELETE FROM links WHERE dom = ? AND subdom = ? AND proto = ? AND path = ?`
	err = sg.DB.Query(del, dom, subdom, proto, path).Exec()
	if err != nil {
		log4go.Error("correctURLNormalization error; Failed to delete for URL %v: %v", u.URL, err)
		return u
	}

	return c
}

// filterLinksByDuplicateContent uses the raw data pulled in by collectLinks
// and filters links, ex. to cut out repeated query parameters that don't
// affect content
func (sg *SegmentGenerator) filterLinksByDuplicateContent() {
	start := time.Now()
	dupClusters := sg.buildDuplicateLinkClusters()
	removeableParams := sg.discoverRemoveableQueryParameters(dupClusters)
	sg.filterLinksWithRules(removeableParams)
	log4go.Debug("Filtered links for %v in %v", sg.domain, time.Since(start))
}

// Build clusters of links with duplicate content. One "Cluster" is a group
// of links with the same fingerprint, which is further grouped by
// subdomain + path (without query parameters). An example entry might be:
//		{
//			29034823084: map[string]LinkList{
//				"www/index.html": LinkList{
//					<LinkInfo representing www.test.com/index.html>,
//					<LinkInfo representing www.test.com/index.html?foo=bar>,
//				},
//				"www/": LinkList{
//					<LinkInfo representing www.test.com/>,
//					<LinkInfo representing www.test.com/?foo=bar>,
//				},
//			},
//		}
// In this example all four pages have the same textual content.
func (sg *SegmentGenerator) buildDuplicateLinkClusters() map[int64]map[string]LinkList {
	dupClusters := map[int64]map[string]LinkList{}
	for _, linkList := range []LinkList{sg.uncrawledLinks, sg.crawledLinks} {
		for _, l := range linkList {
			entry := dupClusters[l.FnvTextFingerprint]
			if entry == nil {
				entry = map[string]LinkList{}
				dupClusters[l.FnvTextFingerprint] = entry
			}
			subdom, err := l.URL.Subdomain()
			if err != nil {
				log4go.Error("Dispatcher creating query rules could not get subdomain: %v", err)
				continue
			}
			key := subdom + l.URL.Path
			entry[key] = append(entry[key], l)
		}
	}
	log4go.Fine("Duplicate cluster map created: %v", dupClusters)
	return dupClusters
}

// Discover which query parameters within a given cluster and path (path
// meaning subdomain+path) differ, so we know those query parameters don't
// affect content and can be deleted. Build a map identifying, for each
// path, which we can delete (ex. the parameter 'foo' in examples above)
func (sg *SegmentGenerator) discoverRemoveableQueryParameters(dupClusters map[int64]map[string]LinkList) map[string]map[string]bool {
	removeableParamsByPath := map[string]map[string]bool{}
	for _, linksByPath := range dupClusters {
		for path, links := range linksByPath {
			removeableParams := map[string]bool{}
			if len(links) <= 1 {
				continue
			}

			// Use the first link as baseline for comparison. Any differences
			// or absent parameters in remaining links will mark that parameter
			// as removeable.
			compareValues := links[0].URL.Query()
			for _, l := range links {
				currentValues := l.URL.Query()

				// First check that all parameters for this link are the same as the comparison
				for param, vals := range currentValues {
					if removeableParams[param] {
						continue
					}

					compareVals, ok := compareValues[param]
					if !ok || !stringListsEqual(vals, compareVals) {
						removeableParams[param] = true
						continue
					}
				}

				// Then see if this link is missing any parameters that are in the comparison
				for param := range compareValues {
					_, ok := currentValues[param]
					if !ok {
						removeableParams[param] = true
						continue
					}
				}
			}
			if len(removeableParams) > 0 {
				removeableParamsByPath[path] = removeableParams
				log4go.Debug("Created parameter removal for subdomain/path %v -- %v", path, removeableParams)
			}
		}
	}
	return removeableParamsByPath
}

// Filter all links, removing parameters, then sort these lists and remove
// links that are no longer unique. Ex. www.test.com/?foo=bar will turn
// into www.test.com/, duplicating the other link in the cluster, so one
// will be removed.
func (sg *SegmentGenerator) filterLinksWithRules(removeableParamsByPath map[string]map[string]bool) {
	for _, linkList := range []LinkList{sg.uncrawledLinks, sg.crawledLinks} {
		for _, l := range linkList {
			subdom, err := l.URL.Subdomain()
			if err != nil {
				log4go.Error("Dispatcher filtering links could not get subdomain: %v", err)
				continue
			}
			key := subdom + l.URL.Path
			removeableParams := removeableParamsByPath[key]
			vals := l.URL.Query()

			// Remove any parameters marked as removeable for this path; use a
			// boolean as a small optimization to prevent re-encoding query
			// parameters for URLs where no replacements were made, which is
			// most URLs
			paramReplaced := false
			for param := range removeableParams {
				vals.Del(param)
				paramReplaced = true
			}
			if paramReplaced {
				beforeFilter := l.URL.String()
				l.URL.RawQuery = vals.Encode()
				log4go.Debug("Dispatcher filtering parameters, turning %s => %s", beforeFilter, l.URL)
			}
		}
		sort.Sort(linkList)
		linkList.Uniq()
	}
}

// buildLinksToDispatch takes the final link lists, post-filtration, and
// produces a dispatch set (limiting to the requested dispatch size and so on)
func (sg *SegmentGenerator) buildLinksToDispatch() {
	start := time.Now()

	sg.linksToDispatch = append(sg.linksToDispatch, sg.getNowLinks...)

	// Create a priority structure out of already-crawled links so we recrawl
	// the oldest first.
	crawledPrioritized := &PriorityURL{}
	heap.Init(crawledPrioritized)
	for _, l := range sg.crawledLinks {
		heap.Push(crawledPrioritized, l)
	}

	// Since we filter query parameters and rewrite links, the crawled and
	// uncrawled lists could end up with identical links. We use this map to
	// deduplicate our final segment (keyed by full URL)
	alreadyAdded := map[string]bool{}

	limit := walker.Config.Dispatcher.MaxLinksPerSegment
	numRemain := limit - len(sg.linksToDispatch)
	if numRemain > 0 {
		refreshDecimal := walker.Config.Dispatcher.RefreshPercentage / 100.0
		idealCrawled := round(refreshDecimal * float64(numRemain))
		idealUncrawled := numRemain - idealCrawled

		for i := 0; i < idealUncrawled && len(sg.uncrawledLinks) > 0 && len(sg.linksToDispatch) < limit; i++ {
			l := sg.uncrawledLinks[0]
			sg.uncrawledLinks = sg.uncrawledLinks[1:]
			if alreadyAdded[l.URL.String()] {
				i--
				continue
			} else {
				sg.linksToDispatch = append(sg.linksToDispatch, l)
				alreadyAdded[l.URL.String()] = true
			}
		}

		for i := 0; i < idealCrawled && crawledPrioritized.Len() > 0 && len(sg.linksToDispatch) < limit; i++ {
			l := heap.Pop(crawledPrioritized).(*LinkInfo)
			if alreadyAdded[l.URL.String()] {
				i--
				continue
			} else {
				sg.linksToDispatch = append(sg.linksToDispatch, l)
				alreadyAdded[l.URL.String()] = true
			}
		}

		for len(sg.uncrawledLinks) > 0 && len(sg.linksToDispatch) < limit {
			l := sg.uncrawledLinks[0]
			sg.uncrawledLinks = sg.uncrawledLinks[1:]
			if alreadyAdded[l.URL.String()] {
				continue
			} else {
				sg.linksToDispatch = append(sg.linksToDispatch, l)
				alreadyAdded[l.URL.String()] = true
			}
		}

		for crawledPrioritized.Len() > 0 && len(sg.linksToDispatch) < limit {
			l := heap.Pop(crawledPrioritized).(*LinkInfo)
			if alreadyAdded[l.URL.String()] {
				continue
			} else {
				sg.linksToDispatch = append(sg.linksToDispatch, l)
				alreadyAdded[l.URL.String()] = true
			}
		}
	}
	log4go.Debug("Build final segment for %v in %v", sg.domain, time.Since(start))
}

// insertSegment inserts the links in sg.linksToDispatch into cassandra and
// updates domain_info accordingly
func (sg *SegmentGenerator) insertSegment() error {
	start := time.Now()

	for _, l := range sg.linksToDispatch {
		log4go.Debug("Inserting link in segment: %s", l.URL)
		dom, subdom, err := l.URL.TLDPlusOneAndSubdomain()
		if err != nil {
			return fmt.Errorf("generateSegment not inserting %v: %v", l.URL, err)
		}
		err = sg.DB.Query(`INSERT INTO segments
			(dom, subdom, path, proto, time)
			VALUES (?, ?, ?, ?, ?)`,
			dom, subdom, l.URL.RequestURI(), l.URL.Scheme, l.URL.LastCrawled).Exec()
		if err != nil {
			log4go.Error("Failed to insert link (%v), error: %v", l.URL, err)
		}
	}

	//
	// Got any links
	//
	dispatched := true
	if len(sg.linksToDispatch) == 0 {
		log4go.Info("No links to dispatch for %v", sg.domain)
		dispatched = false
	}

	dispatchStamp := time.Now()
	dispatchFieldName := "last_dispatch"
	if !dispatched {
		dispatchFieldName = "last_empty_dispatch"
	}

	//
	// Update domain_info
	//
	updateQuery := fmt.Sprintf(`UPDATE domain_info
								   SET 
								   		dispatched = ?,
								   		tot_links = ?,
								   		uncrawled_links = ?,
								   		queued_links = ?,
								   		%s = ?
								   WHERE dom = ?`, dispatchFieldName)

	err := sg.DB.Query(updateQuery, dispatched, sg.totalLinksCount, sg.uncrawledLinksCount, len(sg.linksToDispatch),
		dispatchStamp, sg.domain).Exec()
	if err != nil {
		return fmt.Errorf("error inserting %v to domain_info: %v", sg.domain, err)
	}

	log4go.Debug("Inserted segment for %v in %v", sg.domain, time.Since(start))
	return nil
}

//
// Some mathy type functions used in generateSegment
//
func imin(l int, r int) int {
	if l < r {
		return l
	}

	return r
}

func round(f float64) int {
	abs := math.Abs(f)
	sign := f / abs
	floor := math.Floor(abs)
	if abs-floor >= 0.5 {
		return int(sign * (floor + 1))
	}
	return int(sign * floor)
}

// stringListsEqual simply checks for deep equality between two lists of
// strings (faster than using reflect.DeepEqual). It will return false even if
// only the string order differs.
func stringListsEqual(l1 []string, l2 []string) bool {
	if len(l1) != len(l2) {
		return false
	}
	for i := range l1 {
		if l1[i] != l2[i] {
			return false
		}
	}
	return true
}
