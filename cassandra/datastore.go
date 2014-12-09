package cassandra

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"code.google.com/p/log4go"
	"github.com/dropbox/godropbox/container/lrucache"
	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

// Datastore is the primary walker Datastore implementation, using Apache
// Cassandra as a highly scalable backend. It provides extra access calls for
// the database for use in the console and other applications.
//
// NewDatastore should be used to create one.
type Datastore struct {
	cf *gocql.ClusterConfig
	db *gocql.Session

	// A group of domains that this datastore has already claimed, ready to
	// pass to a fetcher
	domains []string
	mu      sync.Mutex

	// A cache for domains we've already verified do or do not exist in domain_info
	// Cache key is TopLevelDomain+1, value is a bool (true if the domain exists)
	domainCache *lrucache.LRUCache

	// This is a unique UUID for the entire crawler.
	crawlerUUID gocql.UUID

	// Number of seconds the crawlerUUID lives in active_fetchers before
	// it's flushed (unless KeepAlive is called in the interim).
	activeFetchersTTL int
}

// NewDatastore creates a Cassandra session and initializes a Datastore
func NewDatastore() (*Datastore, error) {
	ds := &Datastore{
		cf: GetConfig(),
	}
	var err error
	ds.db, err = ds.cf.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("Failed to create cassandra datastore: %v", err)
	}
	ds.domainCache = lrucache.New(walker.Config.Cassandra.AddedDomainsCacheSize)

	u, err := gocql.RandomUUID()
	if err != nil {
		return ds, err
	}
	ds.crawlerUUID = u

	durr, err := time.ParseDuration(walker.Config.Fetcher.ActiveFetchersTTL)
	if err != nil {
		panic(err) // This won't happen b/c this duration is checked in Config
	}
	ds.activeFetchersTTL = int(durr / time.Second)

	return ds, nil
}

func (ds *Datastore) Close() {
	ds.db.Close()
}

//
// Implementation of the walker.Datastore interface
//

// limitPerClaimCycle is the target number of domains to put in the
// Datastore.domains per population.
var limitPerClaimCycle int = 50

// The allowed values of the priority in the domain_info table
var AllowedPriorities = []int{5, 4, 3, 2, 1, 0, -1, -2, -3, -4, -5} //order matters here

func (ds *Datastore) ClaimNewHost() string {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if len(ds.domains) == 0 {
		for _, priority := range AllowedPriorities {
			var domainsPerPrio []string
			retryLimit := 5
			for i := 0; i < retryLimit; i++ {
				log4go.Fine("ClaimNewHost pulling priority = %d", priority)
				var retry bool
				domainsPerPrio, retry = ds.tryClaimHosts(priority, limitPerClaimCycle-len(ds.domains))
				if !retry {
					break
				}
			}
			ds.domains = append(ds.domains, domainsPerPrio...)
			if len(ds.domains) >= limitPerClaimCycle {
				break
			}
		}
	}

	if len(ds.domains) == 0 {
		return ""
	}

	domain := ds.domains[0]
	ds.domains = ds.domains[1:]
	return domain
}

// tryClaimHosts trys to read a list of hosts from domain_info. Returns retry
// if the caller should re-call the method.
func (ds *Datastore) tryClaimHosts(priority int, limit int) (domains []string, retry bool) {
	loopQuery := fmt.Sprintf(`SELECT dom 
									FROM domain_info
									WHERE 
										claim_tok = 00000000-0000-0000-0000-000000000000 AND
								 		dispatched = true AND
								 		priority = ?
								 	LIMIT %d 
								 	ALLOW FILTERING`, limit)

	casQuery := `UPDATE domain_info 
						SET 
							claim_tok = ?, 
							claim_time = ?
						WHERE 
							dom = ?
						IF 
							dispatched = true AND
							claim_tok = 00000000-0000-0000-0000-000000000000`

	// The trumpedClaim counter handles the case when the code attempts to
	// grab limit domains, but all limit of those domains are claimed by
	// another datastore before any can be claimed by this datastore.
	// Under current expected use, it seems like we wouldn't need to retry
	// more than 5-ish times (hence the retryLimit setting).

	var domain string
	start := time.Now()
	trumpedClaim := 0
	domain_iter := ds.db.Query(loopQuery, priority).Iter()
	for domain_iter.Scan(&domain) {
		// The query below is a compare-and-set type query. It will only update the claim_tok, claim_time
		// if the claim_tok remains 00000000-0000-0000-0000-000000000000 at the time of update.
		casMap := map[string]interface{}{}
		applied, err := ds.db.Query(casQuery, ds.crawlerUUID, time.Now(), domain).MapScanCAS(casMap)
		if err != nil {
			log4go.Error("Failed to claim segment %v: %v", domain, err)
		} else if !applied {
			trumpedClaim++
			log4go.Fine("Domain %v was claimed by another crawler before resolution", domain)
		} else {
			domains = append(domains, domain)
			log4go.Fine("Claimed segment %v with token %v in %v", domain, ds.crawlerUUID, time.Since(start))
			start = time.Now()
		}
	}

	err := domain_iter.Close()

	if err != nil {
		log4go.Error("Domain iteration query failed: %v", err)
		return
	}

	if trumpedClaim >= limit {
		log4go.Fine("tryClaimHosts requesting retry with trumpedClaim = %d, and limit = %d", trumpedClaim, limit)
		retry = true
	}

	return
}

func (ds *Datastore) UnclaimHost(host string) {
	err := ds.db.Query(`DELETE FROM segments WHERE dom = ?`, host).Exec()
	if err != nil {
		log4go.Error("Failed deleting segment links for %v: %v", host, err)
	}

	err = ds.db.Query(`UPDATE domain_info 
					   SET 
					   		dispatched = false,
							claim_tok = 00000000-0000-0000-0000-000000000000,
							queued_links = 0
						WHERE dom = ?`, host).Exec()
	if err != nil {
		log4go.Error("Failed deleting %v from domains_to_crawl: %v", host, err)
	}
}

func (ds *Datastore) LinksForHost(domain string) <-chan *walker.URL {
	links, err := ds.getSegmentLinks(domain)
	if err != nil {
		log4go.Error("Failed to grab segment for %v: %v", domain, err)
		c := make(chan *walker.URL)
		close(c)
		return c
	}
	log4go.Info("Returning %v links to crawl domain %v", len(links), domain)

	linkchan := make(chan *walker.URL, len(links))
	for _, l := range links {
		linkchan <- l
	}
	close(linkchan)
	return linkchan
}

// getSegmentLinks returns all the URLs in a domain's segment.
// TODO: change our LinksForHost implementation to kick off a goroutine to feed
// 			the channel, instead of keeping all links in memory as we do now.
func (ds *Datastore) getSegmentLinks(domain string) (links []*walker.URL, err error) {
	q := ds.db.Query(`SELECT dom, subdom, path, proto, time
						FROM segments WHERE dom = ?`, domain)
	iter := q.Iter()
	defer func() { err = iter.Close() }()

	var dbdomain, subdomain, path, protocol string
	var crawl_time time.Time
	for iter.Scan(&dbdomain, &subdomain, &path, &protocol, &crawl_time) {
		u, e := walker.CreateURL(dbdomain, subdomain, path, protocol, crawl_time)
		if e != nil {
			log4go.Error("Error adding link (%v) to crawl: %v", u, e)
		} else {
			log4go.Debug("Adding link: %v", u)
			links = append(links, u)
		}
	}
	return
}

// dbfield is a little struct for updating a dynamic list of columns in the
// database
type dbfield struct {
	name  string
	value interface{}
}

func (ds *Datastore) StoreURLFetchResults(fr *walker.FetchResults) {
	url := fr.URL
	if len(fr.RedirectedFrom) > 0 {
		// Remember that the actual response of this FetchResults is from
		// the url at the end of RedirectedFrom
		url = fr.RedirectedFrom[len(fr.RedirectedFrom)-1]
	}

	dom, subdom, err := fr.URL.TLDPlusOneAndSubdomain()
	if err != nil {
		// Consider storing in the link table so we don't keep trying to crawl
		// this link
		log4go.Error("StoreURLFetchResults not storing %v: %v", fr.URL, err)
		return
	}

	inserts := []dbfield{
		dbfield{"dom", dom},
		dbfield{"subdom", subdom},
		dbfield{"path", url.RequestURI()},
		dbfield{"proto", url.Scheme},
		dbfield{"time", fr.FetchTime},
		dbfield{"fnv", fr.FnvFingerprint},
	}

	if fr.FetchError != nil {
		inserts = append(inserts, dbfield{"err", fr.FetchError.Error()})
	}

	if fr.ExcludedByRobots {
		inserts = append(inserts, dbfield{"robot_ex", true})
	}

	if fr.Response != nil {
		inserts = append(inserts, dbfield{"stat", fr.Response.StatusCode})
	}

	if fr.MimeType != "" {
		inserts = append(inserts, dbfield{"mime", fr.MimeType})
	}

	// Put the values together and run the query
	names := []string{}
	values := []interface{}{}
	placeholders := []string{}
	for _, f := range inserts {
		names = append(names, f.name)
		values = append(values, f.value)
		placeholders = append(placeholders, "?")
	}
	err = ds.db.Query(
		fmt.Sprintf(`INSERT INTO links (%s) VALUES (%s)`,
			strings.Join(names, ", "), strings.Join(placeholders, ", ")),
		values...,
	).Exec()
	if err != nil {
		log4go.Error("Failed storing fetch results: %v", err)
		return
	}

	if len(fr.RedirectedFrom) > 0 {
		// Only trick with this is that fr.URL redirected to RedirectedFrom[0], after that
		// RedirectedFrom[n] redirected to RedirectedFrom[n+1]
		rf := fr.RedirectedFrom
		back := fr.URL
		for i := 0; i < len(rf); i++ {
			front := rf[i]
			dom, subdom, err = back.TLDPlusOneAndSubdomain()
			if err != nil {
				log4go.Error("StoreURLFetchResults not storing info for url that redirected (%v): %v", back, err)
				continue
			}
			err := ds.db.Query(`INSERT INTO links (dom, subdom, path, proto, time, redto_url) VALUES (?, ?, ?, ?, ?, ?)`,
				dom, subdom, back.RequestURI(), back.Scheme, fr.FetchTime,
				front.String()).Exec()
			if err != nil {
				log4go.Error("Failed to insert redirected link %s -> %s: %v", back.String(), front.String(), err)
			}
			back = front
		}
	}
}

func (ds *Datastore) StoreParsedURL(u *walker.URL, fr *walker.FetchResults) {
	if !u.IsAbs() {
		log4go.Warn("Link should not have made it to StoreParsedURL: %v", u)
		return
	}
	dom, subdom, err := u.TLDPlusOneAndSubdomain()
	if err != nil {
		log4go.Debug("StoreParsedURL not storing %v: %v", fr.URL, err)
		return
	}

	exists := ds.hasDomain(dom)

	if !exists && walker.Config.Cassandra.AddNewDomains {
		log4go.Debug("Adding new domain to system: %v", dom)
		ds.addDomain(dom)
		exists = true
	}

	if exists {
		log4go.Fine("Inserting parsed URL: %v", u)
		err = ds.db.Query(`INSERT INTO links (dom, subdom, path, proto, time)
							VALUES (?, ?, ?, ?, ?)`,
			dom, subdom, u.RequestURI(), u.Scheme, walker.NotYetCrawled).Exec()
		if err != nil {
			log4go.Error("failed inserting parsed url (%v): %v", u, err)
		}
	}
}

func (ds *Datastore) KeepAlive() error {
	err := ds.db.Query(`INSERT INTO active_fetchers (tok) VALUES (?) USING TTL ?`,
		ds.crawlerUUID, ds.activeFetchersTTL).Exec()
	return err
}

// hasDomain expects a TopLevelDomain+1 (no subdomain) and returns true if the
// domain exists in the domain_info table
func (ds *Datastore) hasDomain(dom string) bool {
	exists, ok := ds.domainCache.Get(dom)
	if ok {
		return exists.(bool)
	}
	var count int
	err := ds.db.Query(`SELECT COUNT(*) FROM domain_info WHERE dom = ?`, dom).Scan(&count)
	if err != nil {
		log4go.Error("Failed to check if %v is in domain_info: %v", dom, err)
		return false // with error, assume we don't have it
	}
	existsDB := (count == 1)
	ds.domainCache.Set(dom, existsDB)
	return existsDB
}

// addDomain adds the domain to the domain_info table if it does not exist. If
// it encounters an error it will log it and move on.
func (ds *Datastore) addDomain(dom string) {
	err := ds.addDomainWithExcludeReason(dom, "")
	if err != nil {
		log4go.Error("Failed to add new dom %v: %v", dom, err)
	}
}

// addDomainWithExcludeReason adds a domain to the domain_info table if it does
// not exist.
func (ds *Datastore) addDomainWithExcludeReason(dom string, reason string) error {

	// Try insert with excluded set to avoid dispatcher picking this domain up before the
	// excluded reason can be set.
	query := `INSERT INTO domain_info (dom, claim_tok, dispatched, priority, excluded) 
					 VALUES (?, ?, false, 0, true) IF NOT EXISTS`
	err := ds.db.Query(query, dom, gocql.UUID{}).Exec()
	if err != nil {
		return err
	}

	// Now set the exclude reason
	excluded := true
	if reason == "" {
		excluded = false
	}
	query = `UPDATE domain_info 
	     	 SET 
	  	    	excluded = ?,
	  	    	exclude_reason = ?
	  		 WHERE 
	  	  		dom = ?`
	err = ds.db.Query(query, excluded, reason, dom).Exec()
	if err != nil {
		return err
	}

	ds.domainCache.Set(dom, true)
	return nil
}

//
// DomainInfo calls
//

// DomainInfo defines a row from the domain_info table
type DomainInfo struct {
	// TLD+1
	Domain string

	// Is this domain excluded from the crawl?
	Excluded bool

	// Why did this domain get excluded, or empty if not excluded
	ExcludeReason string

	// When did this domain last get queued to be crawled. Or TimeQueed.IsZero() if not crawled
	ClaimTime time.Time

	// What was the UUID of the crawler that last crawled the domain
	ClaimToken gocql.UUID

	// Number of (unique) links found in this domain
	NumberLinksTotal int

	// Number of (unique) links queued to be processed for this domain
	NumberLinksQueued int

	// Number of links not yet crawled
	NumberLinksUncrawled int

	// Priority of this domain
	Priority int
}

// DomainInfoUpdateConfig is used to configure the method Datastore.UpdateDomain
type DomainInfoUpdateConfig struct {

	// Setting Exclude to true indicates that the ExcludeReason field of the DomainInfo passed to UpdateDomain should be
	// persisted to the database.
	Exclude bool

	// Setting Priority to true indicates that the Priority field of the DomainInfo passed to UpdateDomain should be
	// persisted to the database.
	Priority bool
}

// DQ is a domain query struct used for getting domains from cassandra.
// Zero-values mean use default behavior.
type DQ struct {
	// When listing domains, the seed should be the domain preceding the
	// queried set. When paginating, use the last domain of the previous set as
	// the seed.
	// Default: select from the beginning
	Seed string

	// Limit the returned results, used for pagination.
	// Default: no limit
	Limit int

	// Set to true to get only dispatched domains
	// default: get all domains
	Working bool
}

// FindDomain returns the DomainInfo for the specified domain
func (ds *Datastore) FindDomain(domain string) (*DomainInfo, error) {
	itr := ds.db.Query(`SELECT claim_tok, claim_time, excluded, exclude_reason, priority, tot_links, uncrawled_links, 
						queued_links FROM domain_info WHERE dom = ?`, domain).Iter()
	var claim_tok gocql.UUID
	var claim_time time.Time
	var excluded bool
	var exclude_reason string
	var priority, linksCount, uncrawledLinksCount, queuedLinksCount int
	if !itr.Scan(&claim_tok, &claim_time, &excluded, &exclude_reason, &priority, &linksCount, &uncrawledLinksCount,
		&queuedLinksCount) {
		err := itr.Close()
		return nil, err
	}

	reason := ""
	if exclude_reason != "" {
		reason = exclude_reason
	} else if excluded {
		// This should just be a backstop in case someone doesn't set exclude_reason.
		reason = "Exclusion marked"
	}
	dinfo := &DomainInfo{
		Domain:               domain,
		ClaimToken:           claim_tok,
		ClaimTime:            claim_time,
		Excluded:             excluded,
		ExcludeReason:        reason,
		Priority:             priority,
		NumberLinksTotal:     linksCount,
		NumberLinksUncrawled: uncrawledLinksCount,
		NumberLinksQueued:    queuedLinksCount,
	}
	err := itr.Close()
	if err != nil {
		return dinfo, err
	}

	return dinfo, err
}

// ListDomains returns a slice of DomainInfo structs populated according to the
// specified DQ (domain query)
func (ds *Datastore) ListDomains(query DQ) ([]*DomainInfo, error) {
	conditions := []string{}
	args := []interface{}{}
	if query.Working {
		conditions = append(conditions, "dispatched = true")
	}

	if query.Seed != "" {
		conditions = append(conditions, "TOKEN(dom) > TOKEN(?)")
		args = append(args, query.Seed)
	}

	cql := `SELECT dom, claim_tok, claim_time, excluded, exclude_reason, priority,
				   tot_links, uncrawled_links, queued_links 
			FROM domain_info`

	if len(conditions) > 0 {
		cql += " WHERE " + strings.Join(conditions, " AND ")
	}

	if query.Limit > 0 {
		cql += " LIMIT ?"
		args = append(args, query.Limit)
	}

	log4go.Debug("Listing domains with query: %v %v", cql, args)
	itr := ds.db.Query(cql, args...).Iter()

	var dinfos []*DomainInfo
	var domain, exclude_reason string
	var claim_tok gocql.UUID
	var claim_time time.Time
	var excluded bool
	var priority, linksCount, uncrawledLinksCount, queuedLinksCount int
	for itr.Scan(&domain, &claim_tok, &claim_time, &excluded, &exclude_reason, &priority, &linksCount,
		&uncrawledLinksCount, &queuedLinksCount) {
		reason := ""
		if exclude_reason != "" {
			reason = exclude_reason
		} else if excluded {
			// This should just be a backstop in case someone doesn't set exclude_reason.
			reason = "Exclusion marked"
		}

		dinfos = append(dinfos, &DomainInfo{
			Domain:               domain,
			ClaimToken:           claim_tok,
			ClaimTime:            claim_time,
			Excluded:             excluded,
			ExcludeReason:        reason,
			Priority:             priority,
			NumberLinksTotal:     linksCount,
			NumberLinksUncrawled: uncrawledLinksCount,
			NumberLinksQueued:    queuedLinksCount,
		})
	}
	err := itr.Close()
	if err != nil {
		return dinfos, err
	}

	return dinfos, err
}

//
// LinkInfo calls
//

// LinkInfo defines a row from the link or segment table
type LinkInfo struct {
	// URL of the link
	URL *walker.URL

	// Status of the fetch
	Status int

	// When did this link get crawled
	CrawlTime time.Time

	// Any error reported when attempting to fetch the URL
	Error string

	// Was this excluded by robots
	RobotsExcluded bool

	// URL this link redirected to if it was a redirect
	RedirectedTo string

	// Whether this link was flagged for immediate fetching
	GetNow bool

	// Mime type (or Content-Type) of the returned data
	Mime string

	// FNV hash of the contents
	FnvFingerprint int64
}

// LQ is a link query struct used for gettings links from cassandra.
// Zero-values mean use default behavior.
type LQ struct {
	// When listing links, the seed should be the URL preceding the queried
	// set. When paginating, use the last URL of the previous set as the seed.
	// Default: select from the beginning
	Seed *walker.URL

	// Limit the returned results, used for pagination.
	// Default: no limit
	Limit int

	FilterRegex string
}

// rememberTimes is a map helper for showing only the latest link results
type rememberTimes struct {
	ctm time.Time
	ind int
}

// FindLink returns a LinkInfo matching the given URL
func (ds *Datastore) FindLink(u *walker.URL) (*LinkInfo, error) {
	tld1, subtld1, err := u.TLDPlusOneAndSubdomain()
	if err != nil {
		return nil, err
	}

	itr := ds.db.Query(`SELECT dom, subdom, path, proto, time, stat, err, robot_ex 
						FROM links 
						WHERE dom = ? AND
							  subdom = ? AND
							  path = ? AND
							  proto = ?`, tld1, subtld1, u.RequestURI(), u.Scheme).Iter()
	rtimes := map[string]rememberTimes{}
	linfos, err := ds.collectLinkInfos(nil, rtimes, itr, 1, nil)
	if err != nil {
		itr.Close()
		return nil, err
	}

	err = itr.Close()
	if err != nil {
		return nil, err
	}

	if len(linfos) == 0 {
		return nil, nil
	} else {
		return linfos[0], nil
	}
}

// Pagination note:
// To paginate a single column you can do
//
//   SELECT a FROM table WHERE a > startingA
//
// If you have two columns though, it requires two queries
//
//   SELECT a,b from table WHERE a == startingA AND b > startingB
//   SELECT a,b from table WHERE a > startingA
//
// With 3 columns it looks like this
//
//   SELECT a,b,c FROM table WHERE a == startingA AND b == startingB AND c > startingC
//   SELECT a,b,c FROM table WHERE a == startingA AND b > startingB
//   SELECT a,b,c FROM table WHERE a > startingA
//
// Particularly for our links table, with primary key domain, subdomain, path, protocol, crawl_time
// For right now, ignore the crawl time we write
//
// SELECT * FROM links WHERE domain = startDomain AND subdomain = startSubDomain AND path = startPath
//                           AND protocol > startProtocol
// SELECT * FROM links WHERE domain = startDomain AND subdomain = startSubDomain AND path > startPath
// SELECT * FROM links WHERE domain = startDomain AND subdomain > startSubDomain
//
// Now the only piece left, is that crawl_time is part of the primary key. Generally we're only going to take the latest
// crawl time. But see Historical query
//

// queryEntry is a helper struct for the layered queries in ListLinks
type queryEntry struct {
	query string
	args  []interface{}
}

// ListLinks fetches links for the given domain according to the given LQ (Link
// Query)
func (ds *Datastore) ListLinks(domain string, query LQ) ([]*LinkInfo, error) {
	if query.Limit <= 0 {
		return nil, fmt.Errorf("Bad value for limit parameter %d", query.Limit)
	}

	var acceptLink func(string) bool = nil
	if query.FilterRegex != "" {
		re, err := regexp.Compile(query.FilterRegex)
		if err != nil {
			return nil, fmt.Errorf("FilterRegex compile error: %v", err)
		}
		acceptLink = func(s string) bool {
			return re.MatchString(s)
		}
	}

	var linfos []*LinkInfo
	rtimes := map[string]rememberTimes{}
	var table []queryEntry

	if query.Seed == nil {
		table = []queryEntry{
			queryEntry{
				query: `SELECT dom, subdom, path, proto, time, stat, err, robot_ex
                      FROM links 
                      WHERE dom = ?`,
				args: []interface{}{domain},
			},
		}
	} else {
		dom, sub, err := query.Seed.TLDPlusOneAndSubdomain()
		if err != nil {
			return linfos, err
		}

		pat := query.Seed.RequestURI()
		pro := query.Seed.Scheme

		table = []queryEntry{
			queryEntry{
				query: `SELECT dom, subdom, path, proto, time, stat, err, robot_ex
                      FROM links 
                      WHERE dom = ? AND 
                            subdom = ? AND 
                            path = ? AND 
                            proto > ?`,
				args: []interface{}{dom, sub, pat, pro},
			},
			queryEntry{
				query: `SELECT dom, subdom, path, proto, time, stat, err, robot_ex 
                      FROM links 
                      WHERE dom = ? AND subdom = ? AND 
                            path > ?`,
				args: []interface{}{dom, sub, pat},
			},
			queryEntry{
				query: `SELECT dom, subdom, path, proto, time, stat, err, robot_ex 
                      FROM links 
                      WHERE dom = ? AND 
                            subdom > ?`,
				args: []interface{}{dom, sub},
			},
		}
	}

	var err error
	for _, qt := range table {
		itr := ds.db.Query(qt.query, qt.args...).Iter()
		linfos, err = ds.collectLinkInfos(linfos, rtimes, itr, query.Limit, acceptLink)
		if err != nil {
			return linfos, err
		}

		err = itr.Close()
		if err != nil {
			return linfos, err
		} else if len(linfos) >= query.Limit {
			return linfos, nil
		}
	}

	return linfos, nil
}

// ListLinkHistorical gets the crawl history of a specific link
func (ds *Datastore) ListLinkHistorical(u *walker.URL) ([]*LinkInfo, error) {
	query := `SELECT dom, subdom, path, proto, time, stat,
						err, robot_ex, redto_url, getnow, mime, fnv
              FROM links
              WHERE dom = ? AND subdom = ? AND path = ? AND proto = ?`
	tld1, subtld1, err := u.TLDPlusOneAndSubdomain()
	if err != nil {
		return nil, err
	}

	itr := ds.db.Query(query, tld1, subtld1, u.RequestURI(), u.Scheme).Iter()

	var linfos []*LinkInfo
	var dom, sub, path, prot, getError, mime, redtoURL string
	var crawlTime time.Time
	var status int
	var fnvFP int64
	var robotsExcluded, getnow bool
	for itr.Scan(&dom, &sub, &path, &prot, &crawlTime, &status,
		&getError, &robotsExcluded, &redtoURL, &getnow, &mime, &fnvFP) {
		// If we need pagination here at some point...
		//if count < seedIndex {
		//	count++
		//	continue
		//}

		u, _ := walker.CreateURL(dom, sub, path, prot, crawlTime)
		linfo := &LinkInfo{
			URL:            u,
			Status:         status,
			Error:          getError,
			CrawlTime:      crawlTime,
			RobotsExcluded: robotsExcluded,
			RedirectedTo:   redtoURL,
			GetNow:         getnow,
			Mime:           mime,
			FnvFingerprint: fnvFP,
		}
		linfos = append(linfos, linfo)

		//if len(linfos) >= limit {
		//	break
		//}
	}
	err = itr.Close()

	return linfos, err
}

// InsertLink inserts the given link into the database, adding it's domain if
// it does not exist. If excludeDomainReason is not empty, this domain will be
// excluded from crawling marked with the given reason.
func (ds *Datastore) InsertLink(link string, excludeDomainReason string) error {
	errors := ds.InsertLinks([]string{link}, excludeDomainReason)
	if len(errors) > 0 {
		return errors[0]
	} else {
		return nil
	}
}

// InsertLinks does the same as InsertLink with many potential errors. It will
// insert as many as it can (it won't stop once it hits a bad link) and only
// return errors for problematic links or domains.
func (ds *Datastore) InsertLinks(links []string, excludeDomainReason string) []error {
	//
	// Collect domains
	//
	var domains []string
	var errList []error
	var urls []*walker.URL
	for i := range links {
		link := links[i]
		url, err := walker.ParseURL(link)
		if err != nil {
			errList = append(errList, fmt.Errorf("%v # ParseURL: %v", link, err))
			domains = append(domains, "")
			urls = append(urls, nil)
			continue
		} else if url.Scheme == "" {
			errList = append(errList, fmt.Errorf("%v # ParseURL: undefined scheme (http:// or https://)", link))
			domains = append(domains, "")
			urls = append(urls, nil)
			continue
		}
		domain, err := url.ToplevelDomainPlusOne()
		if err != nil {
			errList = append(errList, fmt.Errorf("%v # ToplevelDomainPlusOne: bad domain: %v", link, err))
			domains = append(domains, "")
			urls = append(urls, nil)
			continue
		}

		domains = append(domains, domain)
		urls = append(urls, url)
	}

	//
	// Push domain information to table. The only trick to this, is I don't add links unless
	// the domain can be added
	//
	db := ds.db
	var seen = map[string]bool{}
	for i := range links {
		link := links[i]
		d := domains[i]
		u := urls[i]

		// if you already had an error, keep going
		if u == nil {
			continue
		}

		if !seen[d] {
			err := ds.addDomainWithExcludeReason(d, excludeDomainReason)
			if err != nil {
				errList = append(errList, fmt.Errorf("%v # add domain: %v", link, err))
				continue
			}
		}
		seen[d] = true

		subdom, err := u.Subdomain()
		if err != nil {
			errList = append(errList, fmt.Errorf("%v # Subdomain(): %v", link, err))
			continue
		}

		err = db.Query(`INSERT INTO links (dom, subdom, path, proto, time)
                                     VALUES (?, ?, ?, ?, ?)`, d, subdom,
			u.RequestURI(), u.Scheme, walker.NotYetCrawled).Exec()
		if err != nil {
			errList = append(errList, fmt.Errorf("%v # `insert query`: %v", link, err))
			continue
		}
	}

	return errList
}

//collectLinkInfos populates a []LinkInfo list given a cassandra iterator. Arguments are described as:
// (a) linfos is the list of LinkInfo's to build on
// (b) rtimes is scratch space used to filter most recent link
// (c) itr is a gocql.Iter instance to be read
// (d) limit is the max length of linfos
// (e) linkAccept is a func(string)bool. If linkAccept(linkText) returns false, the link IS NOT retained in linfos [
//  This is used to implement filterRegex on ListLinks]
func (ds *Datastore) collectLinkInfos(linfos []*LinkInfo, rtimes map[string]rememberTimes, itr *gocql.Iter, limit int,
	linkAccept func(string) bool) ([]*LinkInfo, error) {
	var domain, subdomain, path, protocol, anerror string
	var crawlTime time.Time
	var robotsExcluded bool
	var status int
	for itr.Scan(&domain, &subdomain, &path, &protocol, &crawlTime, &status, &anerror, &robotsExcluded) {
		u, err := walker.CreateURL(domain, subdomain, path, protocol, crawlTime)
		if err != nil {
			return linfos, err
		}
		urlString := u.String()

		if linkAccept != nil && !linkAccept(urlString) {
			continue
		}

		qq, yes := rtimes[urlString]

		if yes && qq.ctm.After(crawlTime) {
			continue
		}

		linfo := &LinkInfo{
			URL:            u,
			Status:         status,
			Error:          anerror,
			RobotsExcluded: robotsExcluded,
			CrawlTime:      crawlTime,
		}

		nindex := -1
		if yes {
			nindex = qq.ind
			linfos[qq.ind] = linfo
		} else {
			// If you've reached the limit, then we're all done
			if len(linfos) >= limit {
				break
			}
			linfos = append(linfos, linfo)
			nindex = len(linfos) - 1
		}
		rtimes[urlString] = rememberTimes{ctm: crawlTime, ind: nindex}
	}

	return linfos, nil
}

// UpdateDomain updates the given domain with fields from `info`. Which fields will be persisted to the store from
// the argument DomainInfo is configured from the DomainInfoUpdateConfig argument. For example, to persist
// the Priority field in the info strut, one would pass DomainInfoUpdateConfig{Priority: true} as the cfg
// argument to UpdateDomain.
func (ds *Datastore) UpdateDomain(domain string, info *DomainInfo, cfg DomainInfoUpdateConfig) error {

	vars := []string{}
	args := []interface{}{}

	if cfg.Exclude {
		reason := info.ExcludeReason
		if !info.Excluded {
			reason = ""
		}
		vars = append(vars, "excluded", "exclude_reason")
		args = append(args, info.Excluded, reason)
	}

	if cfg.Priority {
		vars = append(vars, "priority")
		args = append(args, info.Priority)
	}

	if len(vars) < 1 {
		return fmt.Errorf("Expected at least one variable set in cfg (of type DomainInfoUpdateConfig)")
	}

	var buffer bytes.Buffer
	buffer.WriteString("UPDATE domain_info\n")
	buffer.WriteString("SET\n")
	for i, v := range vars {
		buffer.WriteString(v)
		if i != len(vars)-1 {
			buffer.WriteString(" = ?,\n")
		} else {
			buffer.WriteString(" = ?\n")
		}
	}
	buffer.WriteString("WHERE dom = ?\n")
	args = append(args, domain)
	query := buffer.String()

	err := ds.db.Query(query, args...).Exec()
	return err
}

//
// Extra helper methods
//

// UnclaimAll iterates domains to unclaim them. Crawlers will unclaim domains
// by themselves, but this is used in case crawlers crash or are killed and
// have left domains claimed.
func (ds *Datastore) UnclaimAll() error {
	iter := ds.db.Query(`SELECT dom FROM domain_info WHERE dispatched = true`).Iter()
	var dom string
	for iter.Scan(&dom) {
		ds.UnclaimHost(dom)
	}
	return iter.Close()
}
