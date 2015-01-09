package cassandra

import (
	"net/http"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

// ModelDatastore defines additional methods for querying and modifying domains
// and links in walker, and includes the walker.Datastore interface (which is
// intended only for the bare minimum that fetchers in the walker package
// need). This interface is good for use by the console and other tools that
// need CRUD-like capabilities.
type ModelDatastore interface {
	walker.Datastore

	// FindDomain returns the DomainInfo for the specified domain
	FindDomain(domain string) (*DomainInfo, error)

	// ListDomains returns a slice of DomainInfo structs populated according to
	// the specified DQ (domain query)
	ListDomains(query DQ) ([]*DomainInfo, error)

	// UpdateDomain updates the given domain with fields from `info`. Which
	// fields will be persisted to the store from the argument DomainInfo is
	// configured from the DomainInfoUpdateConfig argument. For example, to
	// persist the Priority field in the info strut, one would pass
	// DomainInfoUpdateConfig{Priority: true} as the cfg argument to
	// UpdateDomain.
	UpdateDomain(domain string, info *DomainInfo, cfg DomainInfoUpdateConfig) error

	// FindLink returns a LinkInfo matching the given URL. Arguments to this
	// function are: (a) u is the url to find (b) collectContent, if true,
	// indicates that Body and Headers field of LinkInfo will be populated.
	FindLink(u *walker.URL, collectContent bool) (*LinkInfo, error)

	// ListLinks fetches links for the given domain according to the given LQ
	// (Link Query)
	ListLinks(domain string, query LQ) ([]*LinkInfo, error)

	// ListLinkHistorical gets the crawl history of a specific link
	ListLinkHistorical(u *walker.URL) ([]*LinkInfo, error)

	// InsertLink inserts the given link into the database, adding it's domain
	// if it does not exist. If excludeDomainReason is not empty, this domain
	// will be excluded from crawling marked with the given reason.
	InsertLink(link string, excludeDomainReason string) error

	// InsertLinks does the same as InsertLink with many potential errors. It
	// will insert as many as it can (it won't stop once it hits a bad link)
	// and only return errors for problematic links or domains.
	InsertLinks(links []string, excludeDomainReason string) []error
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

	// Body of request (if configured to be stored)
	Body string

	// Header of request (if configured to be stored)
	Headers http.Header
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

	// Setting Exclude to true indicates that the ExcludeReason field of the
	// DomainInfo passed to UpdateDomain should be persisted to the database.
	Exclude bool

	// Setting Priority to true indicates that the Priority field of the
	// DomainInfo passed to UpdateDomain should be persisted to the database.
	Priority bool
}
