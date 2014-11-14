package console

//TODO: comment these

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

	// List links from the given domain. If seedUrl is non-empty, it indicates
	// where to start the cursor for the list. limit denotes the maximum
	// number of results returned. If filterRegex is non-empty, the links
	// returned will be pre-filtered by this regular expression.
	ListLinks(domain string, seedUrl string, limit int, filterRegex string) ([]LinkInfo, error)

	// For a given linkUrl, return the entire crawl history
	ListLinkHistorical(linkUrl string, seedIndex int, limit int) ([]LinkInfo, int, error)

	// Find a link
	FindLink(link string) (*LinkInfo, error)

	// Change the exclusion on a domain
	UpdateDomainExclude(domain string, exclude bool, reason string) error
}

var DS Model
