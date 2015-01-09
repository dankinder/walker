package walker

import (
	"bytes"
	"fmt"
	"mime"
	"net"
	"net/http"
	"regexp"
	"strings"

	"code.google.com/p/go.net/html"
	"code.google.com/p/go.net/html/charset"
	"code.google.com/p/log4go"
)

// parseLinks tries to parse the http response in the given FetchResults for
// links and stores them in the datastore.
func (f *fetcher) parseLinks(body []byte, fr *FetchResults) {
	outlinks, noindex, nofollow, err := parseHTML(body)
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
		outlink.MakeAbsolute(fr.URL)
		if f.shouldStoreParsedLink(outlink) {
			log4go.Fine("Storing parsed link: %v", outlink)
			f.fm.Datastore.StoreParsedURL(outlink, fr)
		}
	}
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
	for _, t := range Config.Fetcher.IgnoreTags {
		delete(tags, t)
	}

	tags["meta"] = true
	return tags
}

// parseHTML processes the html stored in content.
// It returns:
//     (a) a list of `links` on the page
//     (b) a boolean metaNoindex to note if <meta name="ROBOTS" content="noindex"> was found
//     (c) a boolean metaNofollow indicating if <meta name="ROBOTS" content="nofollow"> was found
func parseHTML(body []byte) (links []*URL, metaNoindex bool, metaNofollow bool, err error) {
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
			//      (meaning success) or an actual error
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
					var isRobots, index, follow bool
					links, isRobots, index, follow = parseMetaAttrs(tokenizer, links)
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
		log4go.Debug("%s encountered an error: %v", label, err)
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
func parseIframe(tokenizer *html.Tokenizer, inLinks []*URL, metaNofollow bool) (links []*URL) {
	links = inLinks
	docsrc, body, err := parseIframeAttrs(tokenizer)
	if err != nil {
		return
	} else if docsrc {
		var nlinks []*URL
		var nNofollow bool
		nlinks, _, nNofollow, err = parseHTML([]byte(body))
		if err != nil {
			log4go.Error("parseEmbed failed to parse docsrc: %v", err)
			return
		}
		if !Config.Fetcher.HonorMetaNofollow || !(nNofollow || metaNofollow) {
			links = append(links, nlinks...)
		}
	} else { //!docsrc
		if !metaNofollow {
			var u *URL
			u, err = ParseAndNormalizeURL(body)
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
var httpEquivWordBytes = []byte("http-equiv")
var refreshWordBytes = []byte("refresh")
var metaRefreshPattern = regexp.MustCompile(`^\s*\d+;\s*url=(.*)`)

func parseMetaAttrs(tokenizer *html.Tokenizer, in_links []*URL) (links []*URL, isRobots bool, noIndex bool, noFollow bool) {
	links = in_links
	var content, httpEquiv []byte
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, nameWordBytes) == 0 {
			name := bytes.ToLower(val)
			isRobots = bytes.Compare(name, robotsWordBytes) == 0
		} else if bytes.Compare(key, contentWordBytes) == 0 {
			content = bytes.ToLower(val)
			// This will match ill-formatted contents like "noindexnofollow",
			// but I don't expect that to be a big deal.
			noIndex = bytes.Contains(content, noindexWordBytes)
			noFollow = bytes.Contains(content, nofollowWordBytes)
		} else if bytes.Compare(key, httpEquivWordBytes) == 0 {
			httpEquiv = bytes.ToLower(val)
		}
		if !moreAttr {
			break
		}
	}

	if bytes.Compare(httpEquiv, refreshWordBytes) == 0 && content != nil {
		results := metaRefreshPattern.FindSubmatch(content)
		if results != nil {
			link := strings.TrimSpace(string(results[1]))
			u, err := ParseAndNormalizeURL(link)
			if err != nil {
				log4go.Error("parseMetaAttrs failed to parse url for %q: %v", link, err)

			} else {
				links = append(links, u)
			}
		}
	}

	return
}

// parse object tag attributes
func parseObjectAttrs(tokenizer *html.Tokenizer) (*URL, error) {
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, dataWordBytes) == 0 {
			return ParseAndNormalizeURL(string(val))
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
			return ParseAndNormalizeURL(string(val))
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
			u, err := ParseAndNormalizeURL(strings.TrimSpace(string(val)))
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
