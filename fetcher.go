package walker

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"code.google.com/p/log4go"
	"github.com/iParadigms/walker/dnscache"
	"github.com/iParadigms/walker/mimetools"
	"github.com/temoto/robotstxt.go"
)

// NotYetCrawled is a convenience for time.Unix(0, 0), used as a crawl time in
// Walker for links that have not yet been fetched.
var NotYetCrawled time.Time

func init() {
	NotYetCrawled = time.Unix(0, 0)
}

// FetchResults contains all relevant context and return data from an
// individual fetch. Handlers receive this to process results.
type FetchResults struct {

	// URL that was requested; will always be populated. If this URL redirects,
	// RedirectedFrom will contain a list of all requested URLS.
	URL *URL

	// A list of redirects. During this request cycle, the first request URL is stored
	// in URL. The second request (first redirect) is stored in RedirectedFrom[0]. And
	// the Nth request (N-1 th redirect) will be stored in RedirectedFrom[N-2],
	// and this is the URL that furnished the http.Response.
	RedirectedFrom []*URL

	// Response object; nil if there was a FetchError or ExcludedByRobots is
	// true. Response.Body may not be the same object the HTTP request actually
	// returns; the fetcher may have read in the response to parse out links,
	// replacing Response.Body with an alternate reader.
	Response *http.Response

	// If the user has set cassandra.store_response_body to true in the config file,
	// then the content of the link will be stored in Body (and consequently stored in the
	// body column of the links table). Otherwise Body is the empty string.
	Body string

	// FetchError if the net/http request had an error (non-2XX HTTP response
	// codes are not considered errors)
	FetchError error

	// Time at the beginning of the request (if a request was made)
	FetchTime time.Time

	// True if we did not request this link because it is excluded by
	// robots.txt rules
	ExcludedByRobots bool

	// True if the page was marked as 'noindex' via a <meta> tag. Whether it
	// was crawled depends on the honor_meta_noindex configuration parameter
	MetaNoIndex bool

	// True if the page was marked as 'nofollow' via a <meta> tag. Whether it
	// was crawled depends on the honor_meta_nofollow configuration parameter
	MetaNoFollow bool

	// The Content-Type of the fetched page.
	MimeType string

	// Fingerprint computed with fnv algorithm (see hash/fnv in standard library)
	FnvFingerprint int64
}

// FetchManager configures and runs the crawl.
//
// The calling code must create a FetchManager, set a Datastore and handlers,
// then call `Start()`
type FetchManager struct {
	// Handler must be set to handle fetch responses.
	Handler Handler

	// Datastore must be set to drive the fetching.
	Datastore Datastore

	// Transport can be set to override the default network transport the
	// FetchManager is going to use. Good for faking remote servers for
	// testing.
	Transport http.RoundTripper

	// TransNoKeepAlive stores a RoundTripper with Keep-Alive set to 0 IF
	// http_keep_alive == "threashold". Otherwise it's nil.
	TransNoKeepAlive http.RoundTripper

	// Parsed duration of the string Config.Fetcher.HttpKeepAliveThreshold
	KeepAliveThreshold time.Duration

	fetchers  []*fetcher
	fetchWait sync.WaitGroup
	started   bool

	// used to match Content-Type headers
	acceptFormats *mimetools.Matcher

	defCrawlDelay time.Duration
	maxCrawlDelay time.Duration

	// how long to wait between Datastore.KeepAlive() calls.
	activeFetcherHeartbeat time.Duration

	// close this channel to kill the keep-alive thread
	keepAliveQuit chan struct{}
}

// Start begins processing assuming that the datastore and any handlers have
// been set. This is a blocking call (run in a goroutine if you want to do
// other things)
//
// You cannot change the datastore or handlers after starting.
func (fm *FetchManager) Start() {
	log4go.Info("Starting FetchManager")
	if fm.Datastore == nil {
		panic("Cannot start a FetchManager without a datastore")
	}
	if fm.Handler == nil {
		panic("Cannot start a FetchManager without a handler")
	}
	if fm.started {
		panic("Cannot start a FetchManager multiple times")
	}

	var err error
	fm.defCrawlDelay, err = time.ParseDuration(Config.Fetcher.DefaultCrawlDelay)
	if err != nil {
		// This won't happen b/c this duration is checked in Config
		panic(err)
	}

	fm.maxCrawlDelay, err = time.ParseDuration(Config.Fetcher.MaxCrawlDelay)
	if err != nil {
		// This won't happen b/c this duration is checked in Config
		panic(err)
	}

	ttl, err := time.ParseDuration(Config.Fetcher.ActiveFetchersTTL)
	if err != nil {
		panic(err) // This won't happen b/c this duration is checked in Config
	}
	fm.activeFetcherHeartbeat = time.Duration(float32(ttl) * Config.Fetcher.ActiveFetchersKeepratio)

	fm.acceptFormats, err = mimetools.NewMatcher(Config.Fetcher.AcceptFormats)
	if err != nil {
		panic(fmt.Errorf("mimetools.NewMatcher failed to initialize: %v", err))
	}

	// Make sure that the initial KeepAlive work is done
	err = fm.Datastore.KeepAlive()
	if err != nil {
		err = fmt.Errorf("Initial KeepAlive call fatally failed: %v", err)
		log4go.Error(err.Error())
		panic(err)
	}

	// Create keep-alive thread
	fm.keepAliveQuit = make(chan struct{})
	fm.fetchWait.Add(1)
	go func() {
		for {
			select {
			case <-fm.keepAliveQuit:
				fm.fetchWait.Done()
				return
			case <-time.After(fm.activeFetcherHeartbeat):
			}

			err := fm.Datastore.KeepAlive()
			if err != nil {
				log4go.Error("KeepAlive Failed: %v", err)
			}
		}
	}()

	fm.started = true

	timeout, err := time.ParseDuration(Config.Fetcher.HttpTimeout)
	if err != nil {
		// This shouldn't happen because HttpTimeout is tested in assertConfigInvariants
		panic(err)
	}

	fm.KeepAliveThreshold, err = time.ParseDuration(Config.Fetcher.HttpKeepAliveThreshold)
	if err != nil {
		// Shouldn't happen since this variable is parsed in assertConfigInvariants
		panic(err)
	}

	if fm.Transport == nil {
		keepAlive := 30 * time.Second
		if strings.ToLower(Config.Fetcher.HttpKeepAlive) == "never" {
			keepAlive = 0 * time.Second
		}

		// Set fm.Transport == http.DefaultTransport, but create a new one; we
		// want to override Dial but don't want to globally override it in
		// http.DefaultTransport.
		fm.Transport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: keepAlive,
			}).Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		}
	}
	if fm.TransNoKeepAlive == nil && strings.ToLower(Config.Fetcher.HttpKeepAlive) == "threshold" {
		fm.TransNoKeepAlive = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: 0 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		}
	}

	t, ok := fm.Transport.(*http.Transport)
	if ok {
		var err error
		t.Dial, err = dnscache.Dial(t.Dial, Config.Fetcher.MaxDNSCacheEntries)
		if err != nil {
			// This should be a very rare panic
			log4go.Error("Failed to construct dnscacheing Dialer: %v", err)
			panic(err)
		}
	} else {
		log4go.Info("Given an non-http Transport, not using dns caching")
	}

	if fm.TransNoKeepAlive != nil {
		t, ok = fm.TransNoKeepAlive.(*http.Transport)
		if ok {
			t.Dial, err = dnscache.Dial(t.Dial, Config.Fetcher.MaxDNSCacheEntries)
			if err != nil {
				// This should be a very rare panic
				log4go.Error("Failed to construct dnscacheing Dialer for TransNoKeepAlive: %v", err)
				panic(err)
			}
		} else {
			log4go.Info("Given an non-http TransNoKeepAlive, not using dns caching")
		}
	}

	numFetchers := Config.Fetcher.NumSimultaneousFetchers
	fm.fetchers = make([]*fetcher, numFetchers)
	for i := 0; i < numFetchers; i++ {
		f := newFetcher(fm)
		fm.fetchers[i] = f
		fm.fetchWait.Add(1)
		go func() {
			f.start()
			fm.fetchWait.Done()
		}()
	}
	fm.fetchWait.Wait()
}

// Stop notifies the fetchers to finish their current requests. It blocks until
// all fetchers have finished.
func (fm *FetchManager) Stop() {
	log4go.Info("Stopping FetchManager")
	if !fm.started {
		panic("Cannot stop a FetchManager that has not been started")
	}
	for _, f := range fm.fetchers {
		go f.stop()
	}
	close(fm.keepAliveQuit)
	fm.fetchWait.Wait()
}

// fetcher encompasses one of potentially many fetchers the FetchManager may
// start up. It will effectively manage one goroutine, crawling one host at a
// time, claiming a new host when it has exhausted the previous one.
type fetcher struct {
	fm         *FetchManager
	host       string
	httpclient *http.Client
	crawldelay time.Duration

	// quit signals the fetcher to stop
	quit chan struct{}

	// done receives when the fetcher has finished; this is necessary because
	// the fetcher may need to clean up (ex. unclaim the current host) after
	// reading from quit
	done chan struct{}

	excludeLink *regexp.Regexp
	includeLink *regexp.Regexp

	// defRobots holds the robots.txt definition used if a host doesn't
	// publish a robots.txt file on it's own.
	defRobots *robotstxt.Group

	// robotsMap maps host -> robots.txt definition to use
	robotsMap map[string]*robotstxt.Group

	// Where to read content pages into
	readBuffer bytes.Buffer
}

func aggregateRegex(list []string, sourceName string) (*regexp.Regexp, error) {
	if len(list) == 0 {
		return nil, nil
	}

	fullPat := strings.Join(list, "|")
	re, err := regexp.Compile(fullPat)
	if err != nil {
		message := fmt.Sprintf("Bad regex in %s:", sourceName)
		found := false
		for _, p := range list {
			_, e := regexp.Compile(p)
			if e != nil {
				found = true
				message += "\n\t'"
				message += p
				message += "'"
			}
		}
		if !found {
			message += "\n\t--UNKNOWN PATTERN--"
		}

		return nil, fmt.Errorf("%v", message)
	}

	return re, nil
}

func newFetcher(fm *FetchManager) *fetcher {
	timeout, err := time.ParseDuration(Config.Fetcher.HttpTimeout)
	if err != nil {
		// This shouldn't happen because HttpTimeout is tested in assertConfigInvariants
		panic(err)
	}

	f := new(fetcher)
	f.fm = fm
	f.httpclient = &http.Client{
		Transport: fm.Transport,
		Timeout:   timeout,
	}
	f.quit = make(chan struct{})
	f.done = make(chan struct{})

	if len(Config.Fetcher.ExcludeLinkPatterns) > 0 {
		f.excludeLink, err = aggregateRegex(Config.Fetcher.ExcludeLinkPatterns, "exclude_link_patterns")
		if err != nil {
			// This shouldn't happen b/c it's already been checked when loading config
			panic(err)
		}
	}

	if len(Config.Fetcher.IncludeLinkPatterns) > 0 {
		f.includeLink, err = aggregateRegex(Config.Fetcher.IncludeLinkPatterns, "include_link_patterns")
		if err != nil {
			// This shouldn't happen b/c it's already been checked when loading config
			panic(err)
		}
	}

	return f
}

// start blocks until the fetcher has completed by being told to quit.
func (f *fetcher) start() {
	log4go.Debug("Starting new fetcher")
	for f.crawlNewHost() {
		// Crawl until told to stop...
	}
	log4go.Debug("Stopping fetcher")
	f.done <- struct{}{}
}

// crawlNewHost host crawls a single host, or delays and returns if there was
// nothing to crawl.
// Returns false if it was signaled to quit and the routine should finish
func (f *fetcher) crawlNewHost() bool {
	select {
	case <-f.quit:
		return false
	default:
	}

	f.host = f.fm.Datastore.ClaimNewHost()
	if f.host == "" {
		time.Sleep(time.Second)
		return true
	}
	defer func() {
		log4go.Info("Finished crawling %v, unclaiming", f.host)
		f.fm.Datastore.UnclaimHost(f.host)
	}()

	if f.checkForBlacklisting(f.host) {
		return true
	}

	// Set up robots map
	log4go.Info("Crawling host: %v with crawl delay %v", f.host, f.crawldelay)
	f.initializeRobotsMap(f.host)

	// Loop through the links
	for link := range f.fm.Datastore.LinksForHost(f.host) {
		select {
		case <-f.quit:
			// Let the defer unclaim the host and the caller indicate that this
			// goroutine is done
			return false
		default:
		}

		log4go.Error("PETE Pre fetch %v", f.httpclient.Transport)
		robots := f.fetchRobots(link.Host)
		log4go.Error("PETE POST fetch %v", f.httpclient.Transport)
		log4go.Error("PETE setTrans for (host=%v) %v is %v", link.Host, link, f.httpclient.Transport)

		// Set which dialer to use here based on robots.CrawlDelay
		shouldDelay, crawlDelayClockStart := f.fetchAndHandle(link, robots)
		if shouldDelay {
			// fetchTime is the last server GET (not counting robots.txt GET's). So
			// delta represents the amount of the CrawlDelay that still needs to be
			// waited
			delta := robots.CrawlDelay - time.Now().Sub(crawlDelayClockStart)
			if delta > 0 {
				time.Sleep(delta)
			}
		}
	}
	return true
}

// fetchAndHandle takes care of fetching and processing a URL beginning to end.
// Returns true if it did actually perform a fetch (even if it wasn't
// successful), indicating that crawl-delay should be observed. Returns, also,
// the time we start the clock for a return visit to the server.
func (f *fetcher) fetchAndHandle(link *URL, robots *robotstxt.Group) (bool, time.Time) {
	fr := &FetchResults{URL: link}

	if !robots.Test(link.RequestURI()) {
		log4go.Debug("Not fetching due to robots rules: %v", link)
		fr.ExcludedByRobots = true
		f.fm.Datastore.StoreURLFetchResults(fr)
		return false, time.Now()
	}

	fr.FetchTime = time.Now()
	fr.Response, fr.RedirectedFrom, fr.FetchError = f.fetch(link)
	if fr.FetchError != nil {
		log4go.Debug("Error fetching %v: %v", link, fr.FetchError)
		f.fm.Datastore.StoreURLFetchResults(fr)
		return true, time.Now()
	}
	log4go.Debug("Fetched %v -- %v", link, fr.Response.Status)

	if fr.Response.StatusCode == http.StatusNotModified {
		log4go.Fine("Received 304 when fetching %v", link)
		f.fm.Datastore.StoreURLFetchResults(fr)

		// There are some logical problems with this handler call.  For
		// example, the page we're fetching could have been rejected by the
		// handler (see below) by either fr.MetaNoIndex or
		// !f.isHandleable(fr.Response). BUT, then stored when we go back with
		// a 304. By definition a 304 is never MetaNoIndex, and f.isHandleable
		// always returns false. May need to address in the future.
		f.fm.Handler.HandleResponse(fr)

		return true, time.Now()
	}

	//
	// Nab the body of the request, and compute fingerprint
	//
	fr.FetchError = f.fillReadBuffer(fr.Response.Body, fr.Response.Header)
	if fr.FetchError != nil {
		log4go.Debug("Error reading body of %v: %v", link, fr.FetchError)
		f.fm.Datastore.StoreURLFetchResults(fr)
		return true, time.Now()
	}

	// At this point, we are certain the complete response has been read from
	// the remote server. Start the Crawl-Delay clock
	crawlDelayClockStart := time.Now()

	fr.MimeType = getMimeType(fr.Response)

	// Replace the response body so the handler can read it.
	fr.Response.Body = ioutil.NopCloser(bytes.NewReader(f.readBuffer.Bytes()))
	if Config.Cassandra.StoreResponseBody {
		fr.Body = string(f.readBuffer.Bytes())
	}

	//
	// Get the fingerprint
	//
	fnv := fnv.New64()
	fnv.Write(f.readBuffer.Bytes())
	fr.FnvFingerprint = int64(fnv.Sum64())

	//
	// Handle html and generic handlers
	//
	if isHTML(fr.Response) {
		log4go.Fine("Reading and parsing as HTML (%v)", link)
		f.parseLinks(f.readBuffer.Bytes(), fr)
	}

	if !(Config.Fetcher.HonorMetaNoindex && fr.MetaNoIndex) && f.isHandleable(fr.Response) {
		f.fm.Handler.HandleResponse(fr)
	}

	//TODO: Wrap the reader and check for read error here
	log4go.Fine("Storing fetch results for %v", link)
	f.fm.Datastore.StoreURLFetchResults(fr)
	return true, crawlDelayClockStart
}

//
// fillReadBuffer will fill up readBuffer with the contents of reader. Any
// problems with the read will be returned in an error; including (and
// importantly) if the content size would exceed MaxHTTPContentSizeBytes.
//
func (f *fetcher) fillReadBuffer(reader io.Reader, headers http.Header) error {
	f.readBuffer.Reset()
	lenArr, lenOk := headers["Content-Length"]
	if lenOk && len(lenArr) > 0 {
		var size int64
		n, err := fmt.Sscanf(lenArr[0], "%d", &size)
		if n != 1 || err != nil || size < 0 {
			log4go.Error("Failed to process Content-Length: %v", err)
		} else if size > Config.Fetcher.MaxHTTPContentSizeBytes {
			return fmt.Errorf("Content size exceeded MaxHTTPContentSizeBytes")
		} else {
			f.readBuffer.Grow(int(size))
		}
	}

	limitReader := io.LimitReader(reader, Config.Fetcher.MaxHTTPContentSizeBytes+1)
	n, err := f.readBuffer.ReadFrom(limitReader)
	if err != nil {
		return err
	} else if n > Config.Fetcher.MaxHTTPContentSizeBytes {
		return fmt.Errorf("Content size exceeded MaxHTTPContentSizeBytes")
	}

	return nil
}

// stop signals a fetcher to stop and waits until completion.
func (f *fetcher) stop() {
	f.quit <- struct{}{}
	<-f.done
}

func (f *fetcher) resetTransport() {
	if f.fm.TransNoKeepAlive != nil {
		f.httpclient.Transport = f.fm.TransNoKeepAlive
	}
}

func (f *fetcher) setTransportFromCrawlDelay(crawlDelay time.Duration) {
	if f.fm.TransNoKeepAlive != nil {
		if crawlDelay > f.fm.KeepAliveThreshold {
			f.httpclient.Transport = f.fm.TransNoKeepAlive
		} else {
			f.httpclient.Transport = f.fm.Transport
		}
	}
}

// initializeRobotsMap inits the robotsMap system
func (f *fetcher) initializeRobotsMap(host string) {

	// Set default robots
	rdata, _ := robotstxt.FromBytes([]byte("User-agent: *\n"))
	f.defRobots = rdata.FindGroup(Config.Fetcher.UserAgent)
	f.defRobots.CrawlDelay = f.fm.defCrawlDelay

	// try read $host/robots.txt. Failure to GET, will just returns
	// f.defRobots before call
	f.resetTransport()
	f.robotsMap = map[string]*robotstxt.Group{}
	f.defRobots = f.getRobots(host)
	f.robotsMap[host] = f.defRobots
	f.setTransportFromCrawlDelay(f.defRobots.CrawlDelay)
}

// fetchRobots is a caching version of getRobots
func (f *fetcher) fetchRobots(host string) *robotstxt.Group {
	rob, robOk := f.robotsMap[host]
	if !robOk {
		f.resetTransport()
		rob = f.getRobots(host)
		f.robotsMap[host] = rob
	}
	f.setTransportFromCrawlDelay(rob.CrawlDelay)
	return rob
}

// getRobots will return the robotstxt.Group for the given host, or the
// default robotstxt.Group if the host doesn't support robots.txt
func (f *fetcher) getRobots(host string) *robotstxt.Group {

	u := &URL{
		URL: &url.URL{
			Scheme: "http",
			Host:   host,
			Path:   "robots.txt",
		},
		LastCrawled: NotYetCrawled, //explicitly set this so that fetcher.fetch won't send If-Modified-Since
	}

	res, _, err := f.fetch(u)
	gotRobots := err == nil && res.StatusCode >= 200 && res.StatusCode < 300
	if !gotRobots {
		if err != nil {
			log4go.Debug("Could not fetch %v, assuming there is no robots.txt (error: %v)", u, err)
		}
		return f.defRobots
	}

	robots, err := robotstxt.FromResponse(res)
	res.Body.Close()
	if err != nil {
		log4go.Debug("Error parsing robots.txt (%v) assuming there is no robots.txt: %v", u, err)
		return f.defRobots
	}

	grp := robots.FindGroup(Config.Fetcher.UserAgent)
	max := f.fm.maxCrawlDelay
	if grp.CrawlDelay > max {
		grp.CrawlDelay = max
	}

	return grp
}

func (f *fetcher) fetch(u *URL) (*http.Response, []*URL, error) {
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to create new request object for %v): %v", u, err)
	}

	req.Header.Set("User-Agent", Config.Fetcher.UserAgent)
	req.Header.Set("Accept", strings.Join(Config.Fetcher.AcceptFormats, ","))
	if !u.LastCrawled.Equal(NotYetCrawled) {
		// Date format used is RFC1123 as specified by
		// http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3.1
		req.Header.Set("If-Modified-Since", u.LastCrawled.Format(time.RFC1123))
	}
	log4go.Debug("Sending request: %+v", req)

	var redirectedFrom []*URL
	f.httpclient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		redirectedFrom = append(redirectedFrom, &URL{URL: req.URL})
		return nil
	}

	res, err := f.httpclient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	return res, redirectedFrom, nil
}

// shouldStoreParsedLink returns true if the argument URL should
// be stored in datastore. The link can (currently) be rejected
// because it's not in the AcceptProtocols, or if the path matches
// exclude_link_patterns and doesn't match include_link_patterns
func (f *fetcher) shouldStoreParsedLink(u *URL) bool {
	path := u.RequestURI()
	include := !(f.excludeLink != nil && f.excludeLink.MatchString(path)) ||
		(f.includeLink != nil && f.includeLink.MatchString(path))
	if !include {
		return false
	}

	for _, f := range Config.Fetcher.AcceptProtocols {
		if u.Scheme == f {
			return true
		}
	}

	return false
}

// checkForBlacklisting returns true if this site is blacklisted or should be
// blacklisted. If we detect that this site should be blacklisted, this
// function will call the datastore appropriately.
//
// One example of blacklisting is detection of IP addresses that resolve to
// localhost or other bad IP ranges.
//
// TODO: since different subdomains my resolve to different IPs, find a way to
// check this for every HTTP fetch without extraneous connections or fetching
// data we aren't going to care about
// TODO: write back to the database that this domain has been blacklisted so we
// don't just keep re-dispatching it
func (f *fetcher) checkForBlacklisting(host string) bool {
	t, ok := f.fm.Transport.(*http.Transport)
	if !ok {
		// We need to get the transport's Dial function in order to check the
		// IP address
		return false
	}

	conn, err := t.Dial("tcp", net.JoinHostPort(host, "80"))
	if err != nil {
		// Don't simply blacklist because we couldn't connect; the TLD+1 may
		// not work but subdomains may work
		log4go.Debug("Could not connect to host (%v, %v) to check blacklisting", host, err)
		return false
	}
	defer conn.Close()

	if Config.Fetcher.BlacklistPrivateIPs && isPrivateAddr(conn.RemoteAddr().String()) {
		log4go.Debug("Host (%v) resolved to private IP address, blacklisting", host)
		return true
	}
	return false
}

func (f *fetcher) isHandleable(r *http.Response) bool {
	for _, ct := range r.Header["Content-Type"] {
		matched, err := f.fm.acceptFormats.Match(ct)
		if err == nil && matched {
			return true
		}
	}
	ctype := strings.Join(r.Header["Content-Type"], ",")
	log4go.Fine("URL (%v) did not match accepted content types, had: %v", r.Request.URL, ctype)
	return false
}
