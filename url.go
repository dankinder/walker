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

func setupParseURL() error {
	if len(Config.PurgeSidList) == 0 {
		parseURLPathStrip = nil
	} else {
		// Here we want to write a regexp that looks like
		// \;jsessionid=.*$|\;other=.*$
		var buffer bytes.Buffer
		buffer.WriteString("(?i)") // case-insensitive
		startedLoop := false
		for _, sid := range Config.PurgeSidList {
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
	for _, p := range Config.PurgeSidList {
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

	// Apply standard normalization filters to u. This call will
	// modify u in place.
	purell.NormalizeURL(u, purell.FlagsSafe|purell.FlagRemoveFragment)

	// Filter the path to catch embedded session ids
	if parseURLPathStrip != nil {
		// Remove SID from path
		u.Path = parseURLPathStrip.ReplaceAllString(u.Path, "")
	}

	//Rewrite the query string to canonical order, removing SID's as needed.
	if u.RawQuery != "" {
		purge := parseURLPurgeMap
		params := u.Query()
		for k := range params {
			if purge[strings.ToLower(k)] {
				delete(params, k)
			}
		}
		u.RawQuery = params.Encode()
	}

	wurl := &URL{URL: u, LastCrawled: NotYetCrawled}
	return wurl, nil
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
