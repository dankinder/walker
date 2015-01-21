package cassandra

import (
	"bytes"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"code.google.com/p/log4go"
	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"

	lru "github.com/hashicorp/golang-lru"
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
	domainCache *lru.Cache

	// This is a unique UUID for the entire crawler.
	crawlerUUID gocql.UUID

	// Number of seconds the crawlerUUID lives in active_fetchers before
	// it's flushed (unless KeepAlive is called in the interim).
	activeFetchersTTL int

	// This field stores the seed domain for the next ClaimNewHost call
	claimCursor string

	// restartCursor is used to indicate the claimCursor should be restarted.
	// Note: we used to use claimCursor == "" to indicate that the cursor should
	// be restarted, but that left us vulnerable to the (unlikely) event that
	// the empty string was stored in domain_infos.
	restartCursor bool

	// The time stamp, after which, max_priority should be re-read
	maxPrioNeedFetch time.Time

	// The value in this variable is the last recorded value of max_priority, if a
	// value was recorded. Otherwise, if max_priority hasn't been read successfully
	// it equals Config.Cassandra.DefaultDomainPriority. In either case maxPrio is the
	// best max_priority value available.
	maxPrio int
}

var MaxPriorityPeriod time.Duration

func init() {
	var err error
	MaxPriorityPeriod, err = time.ParseDuration("60s")
	if err != nil {
		panic(err)
	}
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
	ds.domainCache, err = lru.New(walker.Config.Cassandra.AddedDomainsCacheSize)
	if err != nil {
		return nil, err
	}

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

	ds.restartCursor = true
	ds.maxPrioNeedFetch = time.Now().AddDate(-1, 0, 0)
	ds.maxPrio = walker.Config.Cassandra.DefaultDomainPriority

	return ds, nil
}

// Close will close the Datastore
func (ds *Datastore) Close() {
	ds.db.Close()
}

//
// Implementation of the walker.Datastore interface
//

// limitPerClaimCycle is the target number of domains to put in the
// Datastore.domains per population.
var limitPerClaimCycle = 50

// ClaimNewHost is documented on the walker.Datastore interface.
func (ds *Datastore) ClaimNewHost() string {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if len(ds.domains) == 0 {
		retryLimit := 5
		for i := 0; i < retryLimit; i++ {
			domainsPerPrio, retry := ds.tryClaimHosts(limitPerClaimCycle - len(ds.domains))
			ds.domains = append(ds.domains, domainsPerPrio...)
			if !retry {
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

// domainPriorityTry will return true if the domain, dom, is eligible to be claimed. The second argument, domPriority,
// is the domain priority of dom. This method updates the domain_counters table. NOTE: next_crawl uses cassandra
// counters which can increment/decrement in a concurrent-consistent manner. Plus, the compare-and-set operation in
// tryClaimHosts guarantees that only one thread can claim a domain, even if several workers, on several different
// machines, are simultaneously trying to claim the domain.
func (ds *Datastore) domainPriorityTry(dom string, domPriority int) bool {
	err := ds.db.Query("UPDATE domain_counters SET next_crawl = next_crawl+? WHERE dom = ?", domPriority, dom).Exec()
	if err != nil {
		log4go.Error("domainPriorityQuery failed to increment/establish counter: %v", err)
		return false
	}

	itr := ds.db.Query(`SELECT next_crawl FROM domain_counters WHERE dom = ?`, dom).Iter()
	cnt := 0
	scaned := itr.Scan(&cnt)
	err = itr.Close()
	if !scaned || err != nil {
		log4go.Error("domainPriorityQuery failed to scan cnt: %v", err)
		return false
	}

	if cnt >= ds.MaxPriority() {
		return true
	}

	return false
}

// This method sets the domain_counters table correctly after a domain has been claimed.
func (ds *Datastore) domainPriorityClaim(dom string) bool {
	err := ds.db.Query("UPDATE domain_counters SET next_crawl = next_crawl-? WHERE dom = ?", ds.MaxPriority(), dom).Exec()
	if err != nil {
		log4go.Error("domainPrioritySet failed to clear domain_counters: %v", err)
		return false
	}

	return true
}

// tryClaimHosts trys to read a list of hosts from domain_info. Returns retry
// if the caller should re-call the method.
func (ds *Datastore) tryClaimHosts(limit int) (domains []string, retry bool) {
	var domainIter *gocql.Iter
	if ds.restartCursor {
		loopQuery := fmt.Sprintf(`SELECT dom, priority 
									FROM domain_info
									WHERE 
										claim_tok = 00000000-0000-0000-0000-000000000000 AND
								 		dispatched = true
								 	LIMIT %d 
								 	ALLOW FILTERING`, limit)
		domainIter = ds.db.Query(loopQuery).Iter()
		ds.restartCursor = false
	} else {
		loopQuery := fmt.Sprintf(`SELECT dom, priority 
									FROM domain_info
									WHERE 
										claim_tok = 00000000-0000-0000-0000-000000000000 AND
								 		dispatched = true AND
								 		TOKEN(dom) > TOKEN(?)
								 	LIMIT %d 
								 	ALLOW FILTERING`, limit)
		domainIter = ds.db.Query(loopQuery, ds.claimCursor).Iter()
	}

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
	var domPriority int
	start := time.Now()
	trumpedClaim := 0
	scanComplete := false
	for domainIter.Scan(&domain, &domPriority) {
		scanComplete = true
		if !ds.domainPriorityTry(domain, domPriority) {
			continue
		}

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
			if ds.domainPriorityClaim(domain) {
				log4go.Fine("Claimed segment %v with token %v in %v", domain, ds.crawlerUUID, time.Since(start))
			}
			start = time.Now()
		}
	}

	err := domainIter.Close()

	if err != nil {
		log4go.Error("Domain iteration query failed: %v", err)
		return
	}

	ds.claimCursor = domain

	if !scanComplete {
		// Restart claimCursor.
		ds.restartCursor = true
		retry = true
	} else if trumpedClaim >= limit {
		log4go.Fine("tryClaimHosts requesting retry with trumpedClaim = %d, and limit = %d", trumpedClaim, limit)
		retry = true
	}

	return
}

// UnclaimHost is documented on the walker.Datastore interface.
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

// LinksForHost is documented on the walker.Datastore interface.
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
	var crawlTime time.Time
	for iter.Scan(&dbdomain, &subdomain, &path, &protocol, &crawlTime) {
		u, e := walker.CreateURL(dbdomain, subdomain, path, protocol, crawlTime)
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

// StoreURLFetchResults is documented on the walker.Datastore interface.
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
		dbfield{"fnv_txt", fr.FnvTextFingerprint},
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

	if fr.Body != "" {
		inserts = append(inserts, dbfield{"body", fr.Body})
	}

	if walker.Config.Cassandra.StoreResponseHeaders && fr.Response != nil && fr.Response.Header != nil {
		h := map[string]string{}
		for k, v := range fr.Response.Header {
			h[k] = strings.Join(v, "\000")
		}
		inserts = append(inserts, dbfield{"headers", h})
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

// StoreParsedURL is documented on the walker.Datastore interface.
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

// KeepAlive is documented on the walker.Datastore interface.
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
	ds.domainCache.Add(dom, existsDB)
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
					 VALUES (?, ?, false, ?, true) IF NOT EXISTS`
	err := ds.db.Query(query, dom, gocql.UUID{}, walker.Config.Cassandra.DefaultDomainPriority).Exec()
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

	ds.domainCache.Add(dom, true)
	return nil
}

func (ds *Datastore) MaxPriority() int {
	if time.Now().After(ds.maxPrioNeedFetch) {
		var prio int
		err := ds.db.Query("SELECT val FROM walker_globals WHERE key = ?", "max_priority").Scan(&prio)
		if err != nil {
			log4go.Error("MaxPriority failed to read max_priority: %v", err)
		} else {
			ds.maxPrio = prio
		}
		ds.maxPrioNeedFetch = time.Now().Add(MaxPriorityPeriod)
	}
	return ds.maxPrio
}

//
// DomainInfo calls
//

func (ds *Datastore) FindDomain(domain string) (*DomainInfo, error) {
	itr := ds.db.Query(`SELECT claim_tok, claim_time, excluded, exclude_reason, priority, tot_links, uncrawled_links, 
						queued_links FROM domain_info WHERE dom = ?`, domain).Iter()
	var claimTok gocql.UUID
	var claimTime time.Time
	var excluded bool
	var excludeReason string
	var priority, linksCount, uncrawledLinksCount, queuedLinksCount int
	if !itr.Scan(&claimTok, &claimTime, &excluded, &excludeReason, &priority, &linksCount, &uncrawledLinksCount,
		&queuedLinksCount) {
		err := itr.Close()
		return nil, err
	}

	reason := ""
	if excludeReason != "" {
		reason = excludeReason
	} else if excluded {
		// This should just be a backstop in case someone doesn't set exclude_reason.
		reason = "Exclusion marked"
	}
	dinfo := &DomainInfo{
		Domain:               domain,
		ClaimToken:           claimTok,
		ClaimTime:            claimTime,
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
	var domain, excludeReason string
	var claimTok gocql.UUID
	var claimTime time.Time
	var excluded bool
	var priority, linksCount, uncrawledLinksCount, queuedLinksCount int
	for itr.Scan(&domain, &claimTok, &claimTime, &excluded, &excludeReason, &priority, &linksCount,
		&uncrawledLinksCount, &queuedLinksCount) {
		reason := ""
		if excludeReason != "" {
			reason = excludeReason
		} else if excluded {
			// This should just be a backstop in case someone doesn't set exclude_reason.
			reason = "Exclusion marked"
		}

		dinfos = append(dinfos, &DomainInfo{
			Domain:               domain,
			ClaimToken:           claimTok,
			ClaimTime:            claimTime,
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
// LinkInfo calls
//

// rememberTimes is a map helper for showing only the latest link results
type rememberTimes struct {
	ctm time.Time
	ind int
}

func (ds *Datastore) FindLink(u *walker.URL, collectContent bool) (*LinkInfo, error) {
	tld1, subtld1, err := u.TLDPlusOneAndSubdomain()
	if err != nil {
		return nil, err
	}

	extraSelect := ""
	if collectContent {
		extraSelect = ", body, headers "
	}

	itr := ds.db.Query(
		`SELECT dom, subdom, path, proto, time, stat, err, robot_ex `+
			extraSelect+
			"FROM links "+
			"WHERE dom = ? AND"+
			"	  subdom = ? AND"+
			"     path = ? AND"+
			"     proto = ?", tld1, subtld1, u.RequestURI(), u.Scheme).Iter()
	rtimes := map[string]rememberTimes{}
	linfos, err := ds.collectLinkInfos(nil, rtimes, itr, 1, nil, collectContent)
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
	}

	return linfos[0], nil
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

func (ds *Datastore) ListLinks(domain string, query LQ) ([]*LinkInfo, error) {
	if query.Limit <= 0 {
		return nil, fmt.Errorf("Bad value for limit parameter %d", query.Limit)
	}

	var acceptLink func(string) bool
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
		linfos, err = ds.collectLinkInfos(linfos, rtimes, itr, query.Limit, acceptLink, false)
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
			URL:                u,
			Status:             status,
			Error:              getError,
			CrawlTime:          crawlTime,
			RobotsExcluded:     robotsExcluded,
			RedirectedTo:       redtoURL,
			GetNow:             getnow,
			Mime:               mime,
			FnvFingerprint:     fnvFP,
			FnvTextFingerprint: fnvFP,
		}
		linfos = append(linfos, linfo)

		//if len(linfos) >= limit {
		//	break
		//}
	}
	err = itr.Close()

	return linfos, err
}

func (ds *Datastore) InsertLink(link string, excludeDomainReason string) error {
	errors := ds.InsertLinks([]string{link}, excludeDomainReason)
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

func (ds *Datastore) InsertLinks(links []string, excludeDomainReason string) []error {
	//
	// Collect domains
	//
	var domains []string
	var errList []error
	var urls []*walker.URL
	for i := range links {
		link := links[i]
		url, err := walker.ParseAndNormalizeURL(link)
		if err != nil {
			errList = append(errList, fmt.Errorf("%v # ParseAndNormalizeURL: %v", link, err))
			domains = append(domains, "")
			urls = append(urls, nil)
			continue
		} else if url.Scheme == "" {
			errList = append(errList, fmt.Errorf("%v # ParseAndNormalizeURL: undefined scheme (http:// or https://)", link))
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
	linkAccept func(string) bool, collectContent bool) ([]*LinkInfo, error) {
	var domain, subdomain, path, protocol, anerror string
	var crawlTime time.Time
	var robotsExcluded bool
	var status int
	var body string
	var headers map[string]string
	var httpHeaders http.Header

	args := []interface{}{&domain, &subdomain, &path, &protocol, &crawlTime, &status, &anerror, &robotsExcluded}
	if collectContent {
		args = append(args, &body, &headers)
	}

	for itr.Scan(args...) {
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

		if collectContent {
			httpHeaders = nil
			if headers != nil {
				httpHeaders = http.Header{}
				for k, v := range headers {
					vs := strings.Split(v, "\000")
					httpHeaders[k] = vs
				}
			}
			headers = nil
		}

		linfo := &LinkInfo{
			URL:            u,
			Status:         status,
			Error:          anerror,
			RobotsExcluded: robotsExcluded,
			CrawlTime:      crawlTime,
			Body:           body,
			Headers:        httpHeaders,
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
