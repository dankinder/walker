package walker

// Handler defines the interface for objects that will be set as handlers on a
// FetchManager.
type Handler interface {
	// HandleResponse will be called by fetchers as they make requests.
	// Handlers can do whatever they want with responses. HandleResponse will
	// be called as long as the request successfully reached the remote server
	// and got an HTTP code. This means there should never be a FetchError set
	// on the FetchResults.
	HandleResponse(res *FetchResults)
}

// Datastore defines the interface for an object to be used as walker's datastore.
//
// Note that this is for link and metadata storage required to make walker
// function properly. It has nothing to do with storing fetched content (see
// `Handler` for that).
type Datastore interface {
	// ClaimNewHost returns a hostname that is now claimed for this crawler to
	// crawl. A segment of links for this host is assumed to be available.
	// Returns the domain of the segment it claimed, or "" if there are none
	// available.
	ClaimNewHost() string

	// UnclaimHost indicates that all links from `LinksForHost` have been
	// processed, so other work may be done with this host. For example the
	// dispatcher will be free analyze the links and generate a new segment.
	UnclaimHost(host string)

	// LinksForHost returns a channel that will feed URLs for a given host.
	LinksForHost(host string) <-chan *URL

	// StoreURLFetchResults takes the return data/metadata from a fetch and
	// stores the visit. Fetchers will call this once for each link in the
	// segment being crawled.
	StoreURLFetchResults(fr *FetchResults)

	// StoreParsedURL stores a URL parsed out of a page (i.e. a URL we may not
	// have crawled yet). `u` is the URL to store. `fr` is the FetchResults
	// object for the fetch from which we got the URL, for any context the
	// datastore may want. A datastore implementation should handle `fr` being
	// nil, so links can be seeded without a fetch having occurred.
	//
	// URLs passed to StoreParsedURL should be absolute.
	//
	// This layer should handle efficiently deduplicating
	// links (i.e. a fetcher should be safe feeding the same URL many times.
	StoreParsedURL(u *URL, fr *FetchResults)

	// This method will be called periodically in fetcher (see XXX). This
	// method should notify the datastore that this fetcher is still alive.
	KeepAlive() error
}

// Dispatcher defines the calls a dispatcher should respond to. A dispatcher
// would typically be paired with a particular Datastore, and not all Datastore
// implementations may need a Dispatcher.
//
// A basic crawl will likely run the dispatcher in the same process as the
// fetchers, but higher-scale crawl setups may run dispatchers separately.
type Dispatcher interface {
	// StartDispatcher should be a blocking call that starts the dispatcher. It
	// should return an error if it could not start or stop properly and nil
	// when it has safely shut down and stopped all internal processing.
	StartDispatcher() error

	// Stop signals the dispatcher to stop. It should block until all internal
	// goroutines have stopped.
	StopDispatcher() error
}
