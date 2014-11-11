/*
	Model contains data source related code.
*/

package console

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

type DomainInfo struct {
	// TLD+1
	Domain string

	// Why did this domain get excluded, or empty if not excluded
	ExcludeReason string

	// When did this domain last get queued to be crawled. Or TimeQueed.IsZero() if not crawled
	TimeQueued time.Time

	// What was the UUID of the crawler that last crawled the domain
	UuidOfQueued gocql.UUID

	// Number of (unique) links found in this domain
	NumberLinksTotal int

	// Number of (unique) links queued to be processed for this domain
	NumberLinksQueued int

	// Number of links not yet crawled
	NumberLinksUncrawled int
}

type LinkInfo struct {
	// URL of the link
	Url string

	// Status of the GET
	Status int

	// Any error reported during the get
	Error string

	// Was this excluded by robots
	RobotsExcluded bool

	// When did this link get crawled
	CrawlTime time.Time
}

//
//DataStore represents all the interaction the application has with the datastore.
//
const DontSeedDomain = ""
const DontSeedUrl = ""
const DontSeedIndex = 0

type Model interface {
	//INTERFACE NOTE: any place you see a seed variable that is a string/timestamp
	// that represents the last value of the previous call. limit is the max number
	// of results returned. seed and limit are used to implement pagination.

	// Close the data store after you're done with it
	Close()

	// InsertLinks queues a set of URLS to be crawled. If excludeDomainReason
	// is a non-empty string, then all the domains (TLD+1) of the links list
	// will be added/set as excluded with the exclude reason set to
	// excludeDomainReason.
	InsertLinks(links []string, excludeDomainReason string) []error

	// Find a specific domain
	FindDomain(domain string) (*DomainInfo, error)

	// List domains
	ListDomains(seedDomain string, limit int) ([]DomainInfo, error)

	// Same as ListDomains, but only lists the domains that are currently queued
	ListWorkingDomains(seedDomain string, limit int) ([]DomainInfo, error)

	// List links from the given domain
	ListLinks(domain string, seedUrl string, limit int) ([]LinkInfo, error)

	// For a given linkUrl, return the entire crawl history
	ListLinkHistorical(linkUrl string, seedIndex int, limit int) ([]LinkInfo, int, error)

	// Find a link
	FindLink(link string) (*LinkInfo, error)

	// Change the exclusion on a domain
	UpdateDomainExclude(domain string, exclude bool, reason string) error
}

var DS Model

//
// Cassandra DataSTore
//
type CqlModel struct {
	Cluster *gocql.ClusterConfig
	Db      *gocql.Session
}

func NewCqlModel() (*CqlModel, error) {
	ds := new(CqlModel)
	ds.Cluster = gocql.NewCluster(walker.Config.Cassandra.Hosts...)
	ds.Cluster.Keyspace = walker.Config.Cassandra.Keyspace
	var err error
	ds.Db, err = ds.Cluster.CreateSession()
	return ds, err
}

func (ds *CqlModel) Close() {
	ds.Db.Close()
}

//NOTE: part of this is cribbed from walker.datastore.go. Code share?
func (ds *CqlModel) addDomainIfNew(domain string, excludeDomainReason string) error {
	var count int
	err := ds.Db.Query(`SELECT COUNT(*) FROM domain_info WHERE dom = ?`, domain).Scan(&count)
	if err != nil {
		return fmt.Errorf("seek; %v", err)
	}

	if count == 0 {
		if excludeDomainReason == "" {
			err = ds.Db.Query(`INSERT INTO domain_info (dom, priority) VALUES (?, 0)`, domain).Exec()
		} else {
			err = ds.Db.Query(`INSERT INTO domain_info (dom, priority, excluded, exclude_reason) VALUES (?, 0, true, ?)`, domain, excludeDomainReason).Exec()
		}
	} else if excludeDomainReason != "" {
		err = ds.Db.Query(`UPDATE domain_info SET excluded = ?, exclude_reason = ? WHERE dom = ?`, true, excludeDomainReason, domain).Exec()
	}

	if err != nil {
		return fmt.Errorf("insert; %v", err)
	}

	return nil
}

//NOTE: InsertLinks should try to insert as much information as possible
//return errors for things it can't handle
func (ds *CqlModel) InsertLinks(links []string, excludeDomainReason string) []error {
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
	db := ds.Db
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
			err := ds.addDomainIfNew(d, excludeDomainReason)
			if err != nil {
				errList = append(errList, fmt.Errorf("%v # addDomainIfNew: %v", link, err))
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

// countUniqueLinks counts
//   (a) Total links for this domain
//   (b) Total links for this domain, not-yet-crawled
func (ds *CqlModel) countUniqueLinks(domain string, table string) (linksTotal int, linksUncrawled int, err error) {
	db := ds.Db
	q := fmt.Sprintf("SELECT subdom, path, proto, time FROM %s WHERE dom = ?", table)
	itr := db.Query(q, domain).Iter()

	var subdomain, path, protocol string
	var crawlTime time.Time
	found := map[string]time.Time{}
	for itr.Scan(&subdomain, &path, &protocol, &crawlTime) {
		key := fmt.Sprintf("%s:%s:%s", path, subdomain, protocol)
		t, foundT := found[key]
		if !foundT {
			found[key] = crawlTime
			if crawlTime.Equal(walker.NotYetCrawled) {
				linksUncrawled++
			}
		} else if t.Before(crawlTime) {
			found[key] = crawlTime
			if t.Equal(walker.NotYetCrawled) {
				linksUncrawled--
			}
		}
	}
	err = itr.Close()
	linksTotal = len(found)

	return
}

func (ds *CqlModel) annotateDomainInfo(dinfos []DomainInfo) error {
	//
	// Count Links
	//
	for i := range dinfos {
		d := &dinfos[i]

		linkCount, linkUncrawled, err := ds.countUniqueLinks(d.Domain, "links")
		if err != nil {
			return err
		}
		d.NumberLinksTotal = linkCount
		d.NumberLinksUncrawled = linkUncrawled

		d.NumberLinksQueued = 0
		if d.TimeQueued != zeroTime {
			segmentCount, _, err := ds.countUniqueLinks(d.Domain, "segments")
			if err != nil {
				return err
			}
			d.NumberLinksQueued = segmentCount
		}
	}

	return nil
}

func (ds *CqlModel) listDomainsImpl(seed string, limit int, working bool) ([]DomainInfo, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("Bad value for limit parameter %d", limit)
	}
	db := ds.Db

	var itr *gocql.Iter
	if seed == "" && !working {
		itr = db.Query("SELECT dom, claim_tok, claim_time, excluded, exclude_reason FROM domain_info LIMIT ?", limit).Iter()
	} else if seed == "" {
		itr = db.Query("SELECT dom, claim_tok, claim_time, excluded, exclude_reason FROM domain_info WHERE dispatched = true LIMIT ?", limit).Iter()
	} else if !working {
		itr = db.Query("SELECT dom, claim_tok, claim_time, excluded, exclude_reason FROM domain_info WHERE TOKEN(dom) > TOKEN(?) LIMIT ?", seed, limit).Iter()
	} else { //working==true AND seed != ""
		itr = db.Query("SELECT dom, claim_tok, claim_time, excluded, exclude_reason FROM domain_info WHERE dispatched = true AND TOKEN(dom) > TOKEN(?) LIMIT ?", seed, limit).Iter()
	}

	var dinfos []DomainInfo
	var domain, exclude_reason string
	var claim_tok gocql.UUID
	var claim_time time.Time
	var excluded bool
	for itr.Scan(&domain, &claim_tok, &claim_time, &excluded, &exclude_reason) {
		reason := ""
		if exclude_reason != "" {
			reason = exclude_reason
		} else if excluded {
			// This should just be a backstop in case someone doesn't set exclude_reason.
			reason = "Exclusion marked"
		}

		dinfos = append(dinfos, DomainInfo{Domain: domain, UuidOfQueued: claim_tok, TimeQueued: claim_time, ExcludeReason: reason})
	}
	err := itr.Close()
	if err != nil {
		return dinfos, err
	}
	err = ds.annotateDomainInfo(dinfos)

	return dinfos, err
}

func (ds *CqlModel) ListDomains(seed string, limit int) ([]DomainInfo, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("Bad value for limit parameter %d", limit)
	}
	return ds.listDomainsImpl(seed, limit, false)
}

func (ds *CqlModel) ListWorkingDomains(seedDomain string, limit int) ([]DomainInfo, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("Bad value for limit parameter %d", limit)
	}
	return ds.listDomainsImpl(seedDomain, limit, true)
}

//		itr = db.Query("SELECT domain, claim_tok, claim_time FROM domain_info WHERE dispatched = true AND TOKEN(domain) > TOKEN(?) LIMIT ?", seed, limit).Iter()
func (ds *CqlModel) FindDomain(domain string) (*DomainInfo, error) {
	db := ds.Db
	itr := db.Query("SELECT claim_tok, claim_time, excluded, exclude_reason FROM domain_info WHERE dom = ?", domain).Iter()
	var claim_tok gocql.UUID
	var claim_time time.Time
	var excluded bool
	var exclude_reason string
	if !itr.Scan(&claim_tok, &claim_time, &excluded, &exclude_reason) {
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

	dinfo := &DomainInfo{Domain: domain, UuidOfQueued: claim_tok, TimeQueued: claim_time, ExcludeReason: reason}
	err := itr.Close()
	if err != nil {
		return dinfo, err
	}

	dinfos := []DomainInfo{*dinfo}
	err = ds.annotateDomainInfo(dinfos)
	*dinfo = dinfos[0]
	return dinfo, err
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
// Now the only piece left, is that crawl_time is part of the primary key. Generally we're only going to take the latest crawl time. But see
// Historical query
//

type rememberTimes struct {
	ctm time.Time
	ind int
}

//collectLinkInfos populates a []LinkInfo list given a cassandra iterator
func (ds *CqlModel) collectLinkInfos(linfos []LinkInfo, rtimes map[string]rememberTimes, itr *gocql.Iter, limit int) ([]LinkInfo, error) {
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

		qq, yes := rtimes[urlString]

		if yes && qq.ctm.After(crawlTime) {
			continue
		}

		linfo := LinkInfo{
			Url:            urlString,
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

type queryEntry struct {
	query string
	args  []interface{}
}

func (ds *CqlModel) ListLinks(domain string, seedUrl string, limit int) ([]LinkInfo, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("Bad value for limit parameter %d", limit)
	}
	db := ds.Db
	var linfos []LinkInfo
	rtimes := map[string]rememberTimes{}
	var table []queryEntry

	if seedUrl == "" {
		table = []queryEntry{
			queryEntry{
				query: `SELECT dom, subdom, path, proto, time, stat, err, robot_ex
                      FROM links 
                      WHERE dom = ?`,
				args: []interface{}{domain},
			},
		}
	} else {
		u, err := walker.ParseURL(seedUrl)
		if err != nil {
			return linfos, err
		}

		dom, err := u.ToplevelDomainPlusOne()
		if err != nil {
			return linfos, err
		}

		sub, err := u.Subdomain()
		if err != nil {
			return linfos, err
		}

		pat := u.RequestURI()
		pro := u.Scheme

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
                      WHERE dom = ? AND 
                            subdom = ? AND 
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
		itr := db.Query(qt.query, qt.args...).Iter()
		linfos, err = ds.collectLinkInfos(linfos, rtimes, itr, limit)
		if err != nil {
			return linfos, err
		}

		err = itr.Close()
		if err != nil {
			return linfos, err
		} else if len(linfos) >= limit {
			return linfos, nil
		}
	}

	return linfos, nil
}

func (ds *CqlModel) ListLinkHistorical(linkUrl string, seedIndex int, limit int) ([]LinkInfo, int, error) {
	if limit <= 0 {
		return nil, seedIndex, fmt.Errorf("Bad value for limit parameter %d", limit)
	}
	db := ds.Db
	u, err := walker.ParseURL(linkUrl)
	if err != nil {
		return nil, seedIndex, err
	}

	query := `SELECT dom, subdom, path, proto, time, stat, err, robot_ex 
              FROM links
              WHERE dom = ? AND subdom = ? AND path = ? AND proto = ?`
	tld1, err := u.ToplevelDomainPlusOne()
	if err != nil {
		return nil, seedIndex, err
	}
	subtld1, err := u.Subdomain()
	if err != nil {
		return nil, seedIndex, err
	}

	itr := db.Query(query, tld1, subtld1, u.RequestURI(), u.Scheme).Iter()

	var linfos []LinkInfo
	var dom, sub, path, prot, getError string
	var crawlTime time.Time
	var status int
	var robotsExcluded bool
	count := 0
	for itr.Scan(&dom, &sub, &path, &prot, &crawlTime, &status, &getError, &robotsExcluded) {
		if count < seedIndex {
			count++
			continue
		}

		url, _ := walker.CreateURL(dom, sub, path, prot, crawlTime)
		linfo := LinkInfo{
			Url:            url.String(),
			Status:         status,
			Error:          getError,
			RobotsExcluded: robotsExcluded,
			CrawlTime:      crawlTime,
		}
		linfos = append(linfos, linfo)
		if len(linfos) >= limit {
			break
		}
	}
	err = itr.Close()

	return linfos, seedIndex + len(linfos), err
}

func (ds *CqlModel) FindLink(link string) (*LinkInfo, error) {
	db := ds.Db
	u, err := walker.ParseURL(link)
	if err != nil {
		return nil, err
	}
	query := `SELECT dom, subdom, path, proto, time, stat, err, robot_ex 
                      FROM links 
                      WHERE dom = ? AND 
                            subdom = ? AND 
                            path = ? AND 
                            proto = ?`

	tld1, err := u.ToplevelDomainPlusOne()
	if err != nil {
		return nil, err
	}

	subtld1, err := u.Subdomain()
	if err != nil {
		return nil, err
	}

	itr := db.Query(query, tld1, subtld1, u.RequestURI(), u.Scheme).Iter()
	rtimes := map[string]rememberTimes{}
	linfos, err := ds.collectLinkInfos(nil, rtimes, itr, 1)
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
		return &linfos[0], nil
	}
}

func (ds *CqlModel) UpdateDomainExclude(domain string, exclude bool, reason string) error {
	db := ds.Db
	query := `UPDATE domain_info 
			  SET 
				  excluded = ?,
				  exclude_reason = ?
			  WHERE 
				  dom = ?`
	if !exclude {
		reason = ""
	}
	err := db.Query(query, exclude, reason, domain).Exec()
	return err
}

// comment
