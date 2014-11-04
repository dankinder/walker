package walker

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"mime"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go.net/html"
	"code.google.com/p/go.net/html/charset"
	"code.google.com/p/go.net/publicsuffix"
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
}

// URL is the walker URL object, which embeds *url.URL but has extra data and
// capabilities used by walker. Note that LastCrawled should not be set to its
// zero value, it should be set to NotYetCrawled.
type URL struct {
	*url.URL

	// LastCrawled is the last time we crawled this URL, for example to use a
	// Last-Modified header.
	LastCrawled time.Time
}

// CreateURL creates a walker URL from values usually pulled out of the
// datastore. subdomain may optionally include a trailing '.', and path may
// optionally include a prefixed '/'.
func CreateURL(domain, subdomain, path, protocol string, lastcrawled time.Time) (*URL, error) {
	if subdomain != "" && !strings.HasSuffix(subdomain, ".") {
		subdomain = subdomain + "."
	}
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	ref := fmt.Sprintf("%s://%s%s%s", protocol, subdomain, domain, path)
	u, err := ParseURL(ref)
	if err != nil {
		return nil, err
	}
	u.LastCrawled = lastcrawled
	return u, nil
}

// ParseURL is the walker.URL equivalent of url.Parse
func ParseURL(ref string) (*URL, error) {
	u, err := url.Parse(ref)
	return &URL{URL: u, LastCrawled: NotYetCrawled}, err
}

// ToplevelDomainPlusOne returns the Effective Toplevel Domain of this host as
// defined by https://publicsuffix.org/, plus one extra domain component.
//
// For example the TLD of http://www.bbc.co.uk/ is 'co.uk', plus one is
// 'bbc.co.uk'. Walker uses these TLD+1 domains as the primary unit of
// grouping.
func (u *URL) ToplevelDomainPlusOne() (string, error) {
	return publicsuffix.EffectiveTLDPlusOne(u.Host)
}

// Subdomain provides the remaining subdomain after removing the
// ToplevelDomainPlusOne. For example http://www.bbc.co.uk/ will return 'www'
// as the subdomain (note that there is no trailing period). If there is no
// subdomain it will return "".
func (u *URL) Subdomain() (string, error) {
	dom, err := u.ToplevelDomainPlusOne()
	if err != nil {
		return "", err
	}
	if len(u.Host) == len(dom) {
		return "", nil
	}
	return strings.TrimSuffix(u.Host, "."+dom), nil
}

// TLDPlusOneAndSubdomain is a convenience function that calls
// ToplevelDomainPlusOne and Subdomain, returning an error if we could not get
// either one.
// The first return is the TLD+1 and second is the subdomain
func (u *URL) TLDPlusOneAndSubdomain() (string, string, error) {
	dom, err := u.ToplevelDomainPlusOne()
	if err != nil {
		return "", "", err
	}
	subdom, err := u.Subdomain()
	if err != nil {
		return "", "", err
	}
	return dom, subdom, nil
}

// MakeAbsolute uses URL.ResolveReference to make this URL object an absolute
// reference (having Schema and Host), if it is not one already. It is
// resolved using `base` as the base URL.
func (u *URL) MakeAbsolute(base *URL) {
	if u.IsAbs() {
		return
	}
	u.URL = base.URL.ResolveReference(u.URL)
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

	fetchers  []*fetcher
	fetchWait sync.WaitGroup
	started   bool

	// used to match Content-Type headers
	acceptFormats *mimetools.Matcher
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

	mm, err := mimetools.NewMatcher(Config.AcceptFormats)
	fm.acceptFormats = mm
	if err != nil {
		panic(fmt.Errorf("mimetools.NewMatcher failed to initialize: %v", err))
	}

	fm.started = true

	if fm.Transport == nil {
		timeout, err := time.ParseDuration(Config.HttpTimeout)
		if err != nil {
			// This shouldn't happen because HttpTimeout is tested in assertConfigInvariants
			panic(err)
		}

		// Set fm.Transport == http.DefaultTransport, but create a new one; we
		// want to override Dial but don't want to globally override it in
		// http.DefaultTransport.
		fm.Transport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		}
	}
	t, ok := fm.Transport.(*http.Transport)
	if ok {
		t.Dial = dnscache.Dial(t.Dial, Config.MaxDNSCacheEntries)
	} else {
		log4go.Info("Given an non-http transport, not using dns caching")
	}

	numFetchers := Config.NumSimultaneousFetchers
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
	fm.fetchWait.Wait()
}

// fetcher encompasses one of potentially many fetchers the FetchManager may
// start up. It will effectively manage one goroutine, crawling one host at a
// time, claiming a new host when it has exhausted the previous one.
type fetcher struct {
	fm         *FetchManager
	host       string
	httpclient *http.Client
	robots     *robotstxt.Group
	crawldelay time.Duration

	// quit signals the fetcher to stop
	quit chan struct{}

	// done receives when the fetcher has finished; this is necessary because
	// the fetcher may need to clean up (ex. unclaim the current host) after
	// reading from quit
	done chan struct{}

	excludeLink *regexp.Regexp
	includeLink *regexp.Regexp
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
	timeout, err := time.ParseDuration(Config.HttpTimeout)
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

	if len(Config.ExcludeLinkPatterns) > 0 {
		f.excludeLink, err = aggregateRegex(Config.ExcludeLinkPatterns, "exclude_link_patterns")
		if err != nil {
			// This shouldn't happen b/c it's already been checked when loading config
			panic(err)
		}
	}

	if len(Config.IncludeLinkPatterns) > 0 {
		f.includeLink, err = aggregateRegex(Config.IncludeLinkPatterns, "include_link_patterns")
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

	f.fetchRobots(f.host)
	f.crawldelay = time.Duration(Config.DefaultCrawlDelay) * time.Second
	if f.robots != nil && int(f.robots.CrawlDelay) > Config.DefaultCrawlDelay {
		f.crawldelay = f.robots.CrawlDelay
	}
	log4go.Info("Crawling host: %v with crawl delay %v", f.host, f.crawldelay)

	for link := range f.fm.Datastore.LinksForHost(f.host) {
		select {
		case <-f.quit:
			// Let the defer unclaim the host and the caller indicate that this
			// goroutine is done
			return false
		default:
		}

		shouldDelay := f.fetchAndHandle(link)
		if shouldDelay {
			time.Sleep(f.crawldelay)
		}
	}
	return true
}

// fetchAndHandle takes care of fetching and processing a URL beginning to end.
// Returns true if it did actually perform a fetch (even if it wasn't
// successful), indicating that crawl-delay should be observed.
func (f *fetcher) fetchAndHandle(link *URL) bool {
	fr := &FetchResults{URL: link}

	if f.robots != nil && !f.robots.Test(link.String()) {
		log4go.Debug("Not fetching due to robots rules: %v", link)
		fr.ExcludedByRobots = true
		f.fm.Datastore.StoreURLFetchResults(fr)
		return false
	}

	fr.FetchTime = time.Now()
	fr.Response, fr.RedirectedFrom, fr.FetchError = f.fetch(link)
	if fr.FetchError != nil {
		log4go.Debug("Error fetching %v: %v", link, fr.FetchError)
		f.fm.Datastore.StoreURLFetchResults(fr)
		return true
	}
	log4go.Debug("Fetched %v -- %v", link, fr.Response.Status)

	fr.MimeType = getMimeType(fr.Response)

	if isHTML(fr.Response) {
		log4go.Fine("Reading and parsing as HTML (%v)", link)

		//TODO: ReadAll is inefficient. We should use a properly sized
		//		buffer here (determined by
		//		Config.MaxHTTPContentSizeBytes or possibly
		//		Content-Length of the response)
		var body []byte
		body, fr.FetchError = ioutil.ReadAll(fr.Response.Body)
		if fr.FetchError != nil {
			log4go.Debug("Error reading body of %v: %v", link, fr.FetchError)
			f.fm.Datastore.StoreURLFetchResults(fr)
			return true
		}

		f.parseLinks(body, fr)

		// Replace the response body so the handler can read it
		fr.Response.Body = ioutil.NopCloser(bytes.NewReader(body))
	}

	if !(Config.HonorMetaNoindex && fr.MetaNoIndex) && f.isHandleable(fr.Response) {
		f.fm.Handler.HandleResponse(fr)
	}

	//TODO: Wrap the reader and check for read error here
	log4go.Fine("Storing fetch results for %v", link)
	f.fm.Datastore.StoreURLFetchResults(fr)
	return true
}

// stop signals a fetcher to stop and waits until completion.
func (f *fetcher) stop() {
	f.quit <- struct{}{}
	<-f.done
}

func (f *fetcher) fetchRobots(host string) {
	u := &URL{
		URL: &url.URL{
			Scheme: "http",
			Host:   host,
			Path:   "robots.txt",
		},
	}
	res, _, err := f.fetch(u)
	if err != nil {
		log4go.Debug("Could not fetch %v, assuming there is no robots.txt (error: %v)", u, err)
		f.robots = nil
		return
	}
	robots, err := robotstxt.FromResponse(res)
	res.Body.Close()
	if err != nil {
		log4go.Debug("Error parsing robots.txt (%v) assuming there is no robots.txt: %v", u, err)
		f.robots = nil
		return
	}
	f.robots = robots.FindGroup(Config.UserAgent)
}

func (f *fetcher) fetch(u *URL) (*http.Response, []*URL, error) {
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to create new request object for %v): %v", u, err)
	}

	req.Header.Set("User-Agent", Config.UserAgent)
	req.Header.Set("Accept", strings.Join(Config.AcceptFormats, ","))

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

// rejectLink checks all the fetcher filters and returns true
// if the link in question should NOT be retained in the datastore
func (f *fetcher) rejectLink(u *URL) bool {
	path := u.RequestURI()
	if f.excludeLink != nil && f.excludeLink.MatchString(path) {
		if f.includeLink != nil && f.includeLink.MatchString(path) {
			return false
		}

		return true
	}
	return false
}

// parseLinks tries to parse the http response in the given FetchResults for
// links and stores them in the datastore.
func (f *fetcher) parseLinks(body []byte, fr *FetchResults) {
	outlinks, noindex, nofollow, err := parseHtml(body)
	if err != nil {
		log4go.Debug("error parsing HTML for page %v: %v", fr.URL, err)
		return
	}

	if noindex {
		fr.MetaNoIndex = true
		log4go.Fine("Page has noindex meta tag: %v", fr.URL)
	}
	if nofollow {
		fr.MetaNoFollow = true
		log4go.Fine("Page has nofollow meta tag: %v", fr.URL)
	}

	for _, outlink := range outlinks {
		if f.rejectLink(outlink) {
			continue
		}
		outlink.MakeAbsolute(fr.URL)
		log4go.Fine("Parsed link: %v", outlink)
		if shouldStoreParsedLink(outlink) {
			f.fm.Datastore.StoreParsedURL(outlink, fr)
		}
	}
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

	if Config.BlacklistPrivateIPs && isPrivateAddr(conn.RemoteAddr().String()) {
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

// getIncludedTags gets a map of tags we should check for outlinks. It uses
// ignored_tags in the config to exclude ones we don't want. Tags are []byte
// types (not strings) because []byte is what the parser uses.
func getIncludedTags() map[string]bool {
	tags := map[string]bool{
		"a":      true,
		"area":   true,
		"form":   true,
		"frame":  true,
		"iframe": true,
		"script": true,
		"link":   true,
		"img":    true,
		"object": true,
		"embed":  true,
	}
	for _, t := range Config.IgnoreTags {
		delete(tags, t)
	}

	tags["meta"] = true
	return tags
}

// parseHtml processes the html stored in content.
// It returns:
//     (a) a list of `links` on the page
//     (b) a boolean metaNoindex to note if <meta name="ROBOTS" content="noindex"> was found
//     (c) a boolean metaNofollow indicating if <meta name="ROBOTS" content="nofollow"> was found
func parseHtml(body []byte) (links []*URL, metaNoindex bool, metaNofollow bool, err error) {
	utf8Reader, err := charset.NewReader(bytes.NewReader(body), "text/html")
	if err != nil {
		return
	}
	tokenizer := html.NewTokenizer(utf8Reader)

	tags := getIncludedTags()

	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			//TODO: should use tokenizer.Err() to see if this is io.EOF
			//		(meaning success) or an actual error
			return
		case html.StartTagToken, html.SelfClosingTagToken:
			tagNameB, hasAttrs := tokenizer.TagName()
			tagName := string(tagNameB)
			if hasAttrs && tags[tagName] {
				switch tagName {
				case "a":
					if !metaNofollow {
						links = parseAnchorAttrs(tokenizer, links)
					}

				case "embed":
					if !metaNofollow {
						links = parseObjectOrEmbed(tokenizer, links, true)
					}

				case "iframe":
					links = parseIframe(tokenizer, links, metaNofollow)

				case "meta":
					isRobots, index, follow := parseMetaAttrs(tokenizer)
					if isRobots {
						metaNoindex = metaNoindex || index
						metaNofollow = metaNofollow || follow
					}

				case "object":
					if !metaNofollow {
						links = parseObjectOrEmbed(tokenizer, links, false)
					}

				}
			}
		}
	}

	return
}

func parseObjectOrEmbed(tokenizer *html.Tokenizer, links []*URL, isEmbed bool) []*URL {
	var ln *URL
	var err error
	if isEmbed {
		ln, err = parseEmbedAttrs(tokenizer)
	} else {
		ln, err = parseObjectAttrs(tokenizer)
	}

	if err != nil {
		label := "parseEmbedAttrs"
		if !isEmbed {
			label = "parseObjectAttrs"
		}
		log4go.Error("%s encountered an error: %v", label, err)
	} else {
		links = append(links, ln)
	}

	return links
}

// parseIframe takes 3 arguments
// (a) tokenizer
// (b) list of links already collected
// (c) a flag indicating if the parser is currently in a nofollow state
// and returns a possibly extended list of links.
func parseIframe(tokenizer *html.Tokenizer, in_links []*URL, metaNofollow bool) (links []*URL) {
	links = in_links
	docsrc, body, err := parseIframeAttrs(tokenizer)
	if err != nil {
		return
	} else if docsrc {
		var nlinks []*URL
		var nNofollow bool
		nlinks, _, nNofollow, err = parseHtml([]byte(body))
		if err != nil {
			log4go.Error("parseEmbed failed to parse docsrc: %v", err)
			return
		}
		if !Config.HonorMetaNofollow || !(nNofollow || metaNofollow) {
			links = append(links, nlinks...)
		}
	} else { //!docsrc
		if !metaNofollow {
			var u *URL
			u, err = ParseURL(body)
			if err != nil {
				log4go.Error("parseEmbed failed to parse src: %v", err)
				return
			}
			links = append(links, u)
		}
	}

	return
}

// A set of words used by the parse* routines below
var contentWordBytes = []byte("content")
var dataWordBytes = []byte("data")
var nameWordBytes = []byte("name")
var noindexWordBytes = []byte("noindex")
var nofollowWordBytes = []byte("nofollow")
var robotsWordBytes = []byte("robots")
var srcWordBytes = []byte("src")
var srcdocWordBytes = []byte("srcdoc")

func parseMetaAttrs(tokenizer *html.Tokenizer) (isRobots bool, noIndex bool, noFollow bool) {
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, nameWordBytes) == 0 {
			name := bytes.ToLower(val)
			isRobots = bytes.Compare(name, robotsWordBytes) == 0
		} else if bytes.Compare(key, contentWordBytes) == 0 {
			content := bytes.ToLower(val)
			// This will match ill-formatted contents like "noindexnofollow",
			// but I don't expect that to be a big deal.
			noIndex = bytes.Contains(content, noindexWordBytes)
			noFollow = bytes.Contains(content, nofollowWordBytes)
		}
		if !moreAttr {
			break
		}
	}
	return
}

// parse object tag attributes
func parseObjectAttrs(tokenizer *html.Tokenizer) (*URL, error) {
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, dataWordBytes) == 0 {
			return ParseURL(string(val))
		}

		if !moreAttr {
			break
		}
	}
	return nil, fmt.Errorf("Failed to find data attribute in object tag")
}

// parse embed tag attributes
func parseEmbedAttrs(tokenizer *html.Tokenizer) (*URL, error) {
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, srcWordBytes) == 0 {
			return ParseURL(string(val))
		}

		if !moreAttr {
			break
		}
	}
	return nil, fmt.Errorf("Failed to find src attribute in embed tag")
}

// parseIframeAttrs parses iframe attributes. An iframe can have a src attribute, which
// holds a url to an second document. An iframe can also have a srcdoc attribute which
// include html inline in a string. The method below returns 3 results
// (a) a boolean indicating if the iframe had a srcdoc attribute (true means srcdoc, false
//     means src)
// (b) the body of whichever src or srcdoc attribute was read
// (c) any errors that arise during processing.
func parseIframeAttrs(tokenizer *html.Tokenizer) (srcdoc bool, body string, err error) {
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, srcWordBytes) == 0 {
			srcdoc = false
			body = string(val)
			return
		} else if bytes.Compare(key, srcdocWordBytes) == 0 {
			srcdoc = true
			body = string(val)
			return
		}

		if !moreAttr {
			break
		}
	}
	err = fmt.Errorf("Failed to find src or srcdoc attribute in iframe tag")
	return
}

// parseAnchorAttrs iterates over all of the attributes in the current anchor token.
// If a href is found, it adds the link value to the links slice.
// Returns the new link slice.
func parseAnchorAttrs(tokenizer *html.Tokenizer, links []*URL) []*URL {
	//TODO: rework this to be cleaner, passing in `links` to be appended to
	//isn't great
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, []byte("href")) == 0 {
			u, err := ParseURL(strings.TrimSpace(string(val)))
			if err == nil {
				links = append(links, u)
			}
		}
		if !moreAttr {
			return links
		}
	}
}

// getMimeType attempts to get the mime type (i.e. "Content-Type") from the
// response. Returns an empty string if unable to.
func getMimeType(r *http.Response) string {
	ctype, ctypeOk := r.Header["Content-Type"]
	if ctypeOk && len(ctype) > 0 {
		mediaType, _, err := mime.ParseMediaType(ctype[0])
		if err != nil {
			log4go.Debug("Failed to parse mime header %q: %v", ctype[0], err)
		} else {
			return mediaType
		}
	}
	return ""
}

func isHTML(r *http.Response) bool {
	if r == nil {
		return false
	}
	for _, ct := range r.Header["Content-Type"] {
		if strings.HasPrefix(ct, "text/html") {
			return true
		}
	}
	return false
}

// shouldStoreParsedLink returns true if the argument URL is an Accepted
// Protocol
func shouldStoreParsedLink(u *URL) bool {
	// Could also check extension here, possibly
	for _, f := range Config.AcceptProtocols {
		if u.Scheme == f {
			return true
		}
	}
	return false
}

var privateNetworks = []*net.IPNet{
	parseCIDR("10.0.0.0/8"),
	parseCIDR("192.168.0.0/16"),
	parseCIDR("172.16.0.0/12"),
	parseCIDR("127.0.0.0/8"),
}

// parseCIDR is a convenience for creating our static private IPNet ranges
func parseCIDR(netstring string) *net.IPNet {
	_, network, err := net.ParseCIDR(netstring)
	if err != nil {
		panic(err.Error())
	}
	return network
}

// isPrivateAddr determines whether the input address belongs to any of the
// private networks specified in privateNetworkStrings. It returns an error
// if the input string does not represent an IP address.
func isPrivateAddr(addr string) bool {
	// Remove the port number if there is one
	if index := strings.LastIndex(addr, ":"); index != -1 {
		addr = addr[:index]
	}

	thisIP := net.ParseIP(addr)
	if thisIP == nil {
		log4go.Error("Failed to parse as IP address: %v", addr)
		return false
	}
	for _, network := range privateNetworks {
		if network.Contains(thisIP) {
			return true
		}
	}
	return false
}
