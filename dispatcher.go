package walker

import (
	"fmt"
	"sync"
	"time"

	"code.google.com/p/log4go"

	"github.com/gocql/gocql"
)

// Dispatcher analyzes what we've crawled so far (generally on a per-domain
// basis) and updates the database. At minimum this means generating new
// segments to crawl in the `segments` table, but it can also mean updating
// domain_info if we find out new things about a domain.
//
// This dispatcher has been designed to run simultaneously with the
// crawlmanager. Crawlers and dispatchers claim domains in Cassandra, so the
// dispatcher can operate on the domains not currently being crawled (and vice
// versa).
//
// This dispatcher works with the CassandraDatastore; not all datastores that
// could be implemented will need a separate dispatcher piece.
type Dispatcher struct {
	cf *gocql.ClusterConfig
	db *gocql.Session

	domains chan string    // For passing domains to generate to worker goroutines
	quit    chan bool      // Channel to close to stop the dispatcher (used by `Stop()`)
	wg      sync.WaitGroup // WaitGroup for the generator goroutines
}

// Start is a blocking call that starts the dispatching. I will return an error
// if it could not start, and returns nil when it has been signaled to stop.
func (d *Dispatcher) Start() error {
	d.cf = GetCassandraConfig()
	var err error
	d.db, err = d.cf.CreateSession()
	if err != nil {
		return fmt.Errorf("Failed to create cassandra session: %v", err)
	}

	d.quit = make(chan bool)
	d.domains = make(chan string)

	//TODO: add Concurrency to config
	concurrency := 1
	for i := 0; i < concurrency; i++ {
		d.wg.Add(1)
		go func() {
			d.generateRoutine()
			d.wg.Done()
		}()
	}

	d.domainIterator()
	return nil
}

// Stop signals the dispatcher to stop and blocks until all internal goroutines
// have stopped.
func (d *Dispatcher) Stop() {
	close(d.quit)
	d.wg.Wait()
	d.db.Close()
}

func (d *Dispatcher) domainIterator() {
	for {
		log4go.Info("Starting new domain iteration")
		domainiter := d.db.Query(`SELECT domain FROM domain_info`).Iter()

		domain := ""
		for domainiter.Scan(&domain) {
			select {
			case <-d.quit:
				log4go.Info("Domain iterator signaled to stop")
				close(d.domains)
				return
			default:
			}

			d.domains <- domain
		}

		// Check for exit here as well in case domain_info is empty
		select {
		case <-d.quit:
			log4go.Info("Domain iterator signaled to stop")
			close(d.domains)
			return
		default:
		}

		if err := domainiter.Close(); err != nil {
			log4go.Error("Error iterating domains from domain_info: %v", err)
		}

		//TODO: configure this sleep time
		time.Sleep(time.Second)
	}
}

func (d *Dispatcher) generateRoutine() {
	for domain := range d.domains {
		scheduled, err := d.domainAlreadyScheduled(domain)
		if err != nil {
			log4go.Error(err.Error())
			continue
		}
		if scheduled {
			log4go.Debug("%v already scheduled to crawl, not generating a segment", domain)
			continue
		}

		if err := d.generateSegment(domain); err != nil {
			log4go.Error("error generating segment for %v: %v", domain, err)
		}
	}
	log4go.Debug("Finishing generateRoutine")
}

// domainAlreadyScheduled verifies that this domain isn't already in
// domains_to_crawl
func (d *Dispatcher) domainAlreadyScheduled(domain string) (bool, error) {
	var count int
	err := d.db.Query(`SELECT COUNT(*) FROM domains_to_crawl
						WHERE domain = ? ALLOW FILTERING`, domain).Scan(&count)
	if err != nil {
		return true, fmt.Errorf("Failed to query for domain count of %v: %v", domain, err)
	}
	return count > 0, nil
}

// generateSegment reads links in for this domain, generates a segment for it,
// and inserts the domain into domains_to_crawl (assuming a segment is ready to
// go)
//
// This implementation is dumb, we're just scheduling the first 500 links we
// haven't crawled yet. We never recrawl.
func (d *Dispatcher) generateSegment(domain string) error {
	log4go.Info("Generating a crawl segment for %v", domain)
	iter := d.db.Query(`SELECT domain, subdomain, path, protocol, crawl_time
						FROM links WHERE domain = ?
						ORDER BY subdomain, path, protocol, crawl_time`, domain).Iter()
	var linkdomain, subdomain, path, protocol string
	var crawl_time time.Time
	epoch := time.Unix(0, 0)
	links := make(map[CassandraLink]bool)
	for iter.Scan(&linkdomain, &subdomain, &path, &protocol, &crawl_time) {
		link := CassandraLink{
			Domain:    linkdomain,
			Subdomain: subdomain,
			Path:      path,
			Protocol:  protocol,
			CrawlTime: crawl_time,
		}

		if crawl_time.Equal(epoch) {
			if len(links) >= 500 {
				// Stop here because we've moved on to a new link
				log4go.Debug("Hit 500 links, not adding any more to the segment")
				break
			}

			// Set CrawlTime to epoch so this link can be removed below (time
			// given by Cassandra query can be different timezone so it won't
			// match)
			link.CrawlTime = epoch

			log4go.Debug("Adding link to segment list: %v", link)
			links[link] = true
		} else {
			// This means we've already crawled the link, so leave it out
			// Because we order by crawl_time we won't hit the link again
			// later with crawl_time == epoch

			// The link in the map has epoch as CrawlTime, need this to properly delete it
			link.CrawlTime = epoch
			log4go.Debug("Link already crawled, removing from segment list: %v", link)
			delete(links, link)
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("error selecting links for %v: %v", domain, err)
	}

	for link, _ := range links {
		log4go.Debug("Inserting link in segment: %v", link)
		err := d.db.Query(`INSERT INTO segments
			(domain, subdomain, path, protocol, crawl_time)
			VALUES (?, ?, ?, ?, ?)`,
			link.Domain, link.Subdomain, link.Path, link.Protocol, link.CrawlTime).Exec()

		if err != nil {
			log4go.Error("Failed to insert link (%v), error: %v", link, err)
		}
	}
	err := d.db.Query(`INSERT INTO domains_to_crawl (domain, priority, crawler_token)
		VALUES (?, ?, 00000000-0000-0000-0000-000000000000)`, domain, 0).Exec()
	if err != nil {
		return fmt.Errorf("error inserting %v to domains_to_crawl: %v", domain, err)
	}

	// Batch insert -- may be faster but hard to figured out what happened on
	// errors
	//
	//batch := d.db.NewBatch(gocql.UnloggedBatch)
	//batch.Query(`INSERT INTO domains_to_crawl (domain, priority, crawler_token)
	//				VALUES (?, ?, 00000000-0000-0000-0000-000000000000)`, domain, 0)
	//for u, _ := range links {
	//	log4go.Debug("Adding link to segment batch insert: %v", u)
	//	batch.Query(`INSERT INTO segments (domain, subdomain, path, protocol, crawl_time)
	//						VALUES (?, ?, ?, ?, ?)`,
	//		u.Host, "", u.Path, u.Scheme, time.Unix(0, 0))
	//}
	//log4go.Info("Inserting %v links in segment for %v", batch.Size()-1, domain)
	//if err := d.db.ExecuteBatch(batch); err != nil {
	//	return fmt.Errorf("error inserting links for segment %v: %v", domain, err)
	//}
	return nil
}