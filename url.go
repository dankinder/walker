package walker

import (
	"bytes"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"code.google.com/p/go.net/publicsuffix"
	"github.com/PuerkitoBio/purell"
)

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

var parseURLPathStrip *regexp.Regexp
var parseURLPurgeMap map[string]bool

func setupNormalizeURL() error {
	if len(Config.Fetcher.PurgeSidList) == 0 {
		parseURLPathStrip = nil
	} else {
		// Here we want to write a regexp that looks like
		// \;jsessionid=.*$|\;other=.*$
		var buffer bytes.Buffer
		buffer.WriteString("(?i)") // case-insensitive
		startedLoop := false
		for _, sid := range Config.Fetcher.PurgeSidList {
			if startedLoop {
				buffer.WriteRune('|')
			}
			startedLoop = true
			buffer.WriteString(`\;`)
			buffer.WriteString(sid)
			buffer.WriteString(`\=.*$`)
		}
		var err error
		parseURLPathStrip, err = regexp.Compile(buffer.String())
		if err != nil {
			return fmt.Errorf("Failed setupParseURL: %v", err)
		}
	}

	parseURLPurgeMap = map[string]bool{}
	for _, p := range Config.Fetcher.PurgeSidList {
		parseURLPurgeMap[strings.ToLower(p)] = true
	}
	return nil
}

// ParseURL is the walker.URL equivalent of url.Parse. Note, all URL's should
// be passed through this function so that we get consistency.
func ParseURL(ref string) (*URL, error) {
	u, err := url.Parse(ref)
	if err != nil {
		return nil, err
	}
	wurl := &URL{URL: u, LastCrawled: NotYetCrawled}
	return wurl, nil
}

func ParseAndNormalizeURL(ref string) (*URL, error) {
	u, err := ParseURL(ref)
	if err != nil {
		return u, err
	}
	u.Normalize()
	return u, nil
}

// This method will normalize the URL according to the current set of normalizing rules.
func (u *URL) Normalize() {
	rawURL := u.URL

	// Apply standard normalization filters to url. This call will
	// modify the url in place.
	purell.NormalizeURL(rawURL, purell.FlagsSafe|purell.FlagRemoveFragment)

	// Filter the path to catch embedded session ids
	if parseURLPathStrip != nil {
		// Remove SID from path
		u.Path = parseURLPathStrip.ReplaceAllString(rawURL.Path, "")
	}

	//Rewrite the query string to canonical order, removing SID's as needed.
	if rawURL.RawQuery != "" {
		purge := parseURLPurgeMap
		params := rawURL.Query()
		for k := range params {
			if purge[strings.ToLower(k)] {
				delete(params, k)
			}
		}
		rawURL.RawQuery = params.Encode()
	}
}

func (u *URL) Clone() *URL {
	nurl := *u.URL

	if nurl.User != nil {
		userInfo := *nurl.User
		nurl.User = &userInfo
	}

	return &URL{
		URL:         &nurl,
		LastCrawled: u.LastCrawled,
	}
}

// Return nil if u is normalized. Otherwise, return the normalized version of u.
func (u *URL) NormalizedForm() *URL {
	// We compare the fields of url.URL below. A few notes:
	//   (a) We do not compare the Opaque field, as it doesn't appear links we'll be looking at will use that field.
	//   (b) We do not consider the User field (of type Userinfo). You can see where the User field comes into play by
	//       looking at this (from url.URL)
	//           scheme://[userinfo@]host/path[?query][#fragment]
	//    the userinfo information should never be changed by normalization, so it appears there is no need to compare
	//    it.
	c := u.Clone()
	c.Normalize()
	normal := c.URL.Scheme == u.URL.Scheme &&
		c.URL.Host == u.URL.Host &&
		c.URL.Path == u.URL.Path &&
		c.URL.RawQuery == u.URL.RawQuery &&
		c.URL.Fragment == u.URL.Fragment

	if normal {
		return nil
	} else {
		return c
	}
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

// Return the 5 tuple that is the primary key for this url in the links table. The return values
// are (with cassandra keys in parens)
// (a) Domain (dom)
// (b) Subdomain (subdom)
// (c) Path part of url (path)
// (d) Schema of url (proto)
// (e) last update time of link (time)
// (f) any errors that occurred
func (u *URL) PrimaryKey() (dom string, subdom string, path string, proto string, time time.Time, err error) {
	// Grab new and old variables
	dom, subdom, err = u.TLDPlusOneAndSubdomain()
	if err != nil {
		return
	}
	path = u.RequestURI()
	proto = u.Scheme
	time = u.LastCrawled
	return
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
