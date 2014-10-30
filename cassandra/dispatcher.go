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

func (d *Dispatcher) domainIterator() {
	for {
		log4go.Debug("Starting new domain iteration")
		domainiter := d.db.Query(`SELECT dom, dispatched FROM domain_info
									WHERE claim_tok = 00000000-0000-0000-0000-000000000000
									AND dispatched = false ALLOW FILTERING`).Iter()

		var domain string
		var dispatched bool
		for domainiter.Scan(&domain, &dispatched) {
			select {
			case <-d.quit:
				log4go.Debug("Domain iterator signaled to stop")
				close(d.domains)
				return
			default:
			}

			if !dispatched {
				d.domains <- domain
			}
		}

		// Check for exit here as well in case domain_info is empty
		select {
		case <-d.quit:
			log4go.Debug("Domain iterator signaled to stop")
			close(d.domains)
			return
		default:
		}

		if err := domainiter.Close(); err != nil {
			log4go.Error("Error iterating domains from domain_info: %v", err)
		}

		//TODO: configure this sleep time
		time.Sleep(time.Second)
		d.generatingWG.Wait()
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
			heap.Push(&crawledLinks, u)
		}
		return
	}

	//
	// Do the scan, and populate the 3 lists
	//
	var start = true
	var finish = true
	var current cell
	var previous cell
	iter := d.db.Query(`SELECT subdom, path, proto, time, getnow
						FROM links WHERE dom = ?`, domain).Iter()
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
	if finish {
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
