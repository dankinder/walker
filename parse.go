package walker

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"code.google.com/p/go.net/html"
	"code.google.com/p/go.net/html/charset"
	"code.google.com/p/log4go"
)

//TODO(dk): it would be great to move the parser out to it's own package, make
//it easier to test independently and plug in non-HTML parsers. If we do make
//this more generic (with an interface) we may want Parse to take a io.Reader
//instead of []byte.

// HTMLParser simply parses html passed from the fetcher. A new struct is
// intended to have Parse() called on it, which will populate it's member
// variables for reading.
type HTMLParser struct {
	// A concatenation of all text, excluding content from script/style tags
	Text []byte
	// A list of links found on the parsed page
	Links []*URL
	// true if <meta name="ROBOTS" content="noindex"> was found
	HasMetaNoIndex bool
	// true if <meta name="ROBOTS" content="nofollow"> was found
	HasMetaNoFollow bool
}

// Parse parses the given content body as HTML and populates instance variables
// as it is able. Parse errors will cause the parser to finish with whatever it
// has found so far. This method will reset it's instance variables if run
// repeatedly
func (p *HTMLParser) Parse(body []byte) {
	// Clear
	p.Links = []*URL{}
	p.HasMetaNoIndex = false
	p.HasMetaNoFollow = false

	utf8Reader, err := charset.NewReader(bytes.NewReader(body), "text/html")
	if err != nil {
		return
	}
	tokenizer := html.NewTokenizer(utf8Reader)

	// Maintains the tag names as we hit open tags. Ex. so we can check "are we
	// currently inside a <script> tag block"
	parentTags := map[string]int{}
	tags := getIncludedTags()

	for {
		tokenType := tokenizer.Next()
		switch tokenType {
		case html.ErrorToken:
			//TODO: should use tokenizer.Err() to see if this is io.EOF
			//      (meaning success) or an actual error
			return

		case html.TextToken:
			// Do not store text from inside script/style tags
			_, inScriptTag := parentTags["script"]
			_, inStyleTag := parentTags["style"]
			if inScriptTag || inStyleTag {
				continue
			}

			txt := bytes.TrimSpace(tokenizer.Text())
			if len(txt) > 0 {
				if len(p.Text) > 0 {
					p.Text = append(p.Text, []byte("\n\n")...)
				}
				p.Text = append(p.Text, txt...)
			}

		case html.StartTagToken, html.SelfClosingTagToken:
			tagNameB, hasAttrs := tokenizer.TagName()
			tagName := string(tagNameB)
			if tokenType == html.StartTagToken {
				num, ok := parentTags[tagName]
				if ok {
					parentTags[tagName] = num + 1
				} else {
					parentTags[tagName] = 1
				}
			}
			if hasAttrs && tags[tagName] {
				switch tagName {
				case "a":
					if !p.HasMetaNoFollow {
						p.parseAnchorAttrs(tokenizer)
					}

				case "embed":
					if !p.HasMetaNoFollow {
						p.parseEmbedAttrs(tokenizer)
					}

				case "iframe":
					p.parseIframe(tokenizer)

				case "meta":
					p.parseMetaAttrs(tokenizer)

				case "object":
					if !p.HasMetaNoFollow {
						p.parseObjectAttrs(tokenizer)
					}

				}
			}

		case html.EndTagToken:
			tagNameB, _ := tokenizer.TagName()
			tagName := string(tagNameB)
			num, ok := parentTags[tagName]

			if !ok {
				log4go.Fine("Page seems to have more end tags than start tags, hit extra %s tag",
					tokenizer.Raw())
			} else if num > 1 {
				parentTags[tagName] = num - 1
			} else {
				delete(parentTags, tagName)
			}
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

// parseIframe grabs links either from the iframe's src attribute or by parsing
// the embedded srcdoc
func (p *HTMLParser) parseIframe(tokenizer *html.Tokenizer) {
	docsrc, body, err := p.parseIframeAttrs(tokenizer)
	if err != nil {
		return
	} else if docsrc {
		subParser := &HTMLParser{}
		subParser.Parse([]byte(body))
		if !Config.Fetcher.HonorMetaNofollow || !(subParser.HasMetaNoFollow || p.HasMetaNoFollow) {
			p.Links = append(p.Links, subParser.Links...)
		}
	} else { //!docsrc
		if !p.HasMetaNoFollow {
			var u *URL
			u, err = ParseAndNormalizeURL(body)
			if err != nil {
				log4go.Fine("parseEmbed failed to parse src: %v", err)
				return
			}
			p.Links = append(p.Links, u)
		}
	}
}

// parseIframeAttrs parses iframe attributes. An iframe can have a src attribute, which
// holds a url to an second document. An iframe can also have a srcdoc attribute which
// include html inline in a string. The method below returns 3 results
// (a) a boolean indicating if the iframe had a srcdoc attribute (true means srcdoc, false
//     means src)
// (b) the body of whichever src or srcdoc attribute was read
// (c) any errors that arise during processing.
func (p *HTMLParser) parseIframeAttrs(tokenizer *html.Tokenizer) (bool, string, error) {
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, srcWordBytes) == 0 {
			return false, string(val), nil
		} else if bytes.Compare(key, srcdocWordBytes) == 0 {
			return true, string(val), nil
		}

		if !moreAttr {
			break
		}
	}
	return false, "", fmt.Errorf("Failed to find src or srcdoc attribute in iframe tag")
}

func (p *HTMLParser) parseMetaAttrs(tokenizer *html.Tokenizer) {
	var content, httpEquiv []byte
	var isRobots, noIndex, noFollow bool
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
				log4go.Fine("parseMetaAttrs failed to parse url for %q: %v", link, err)

			} else {
				p.Links = append(p.Links, u)
			}
		}
	}

	if isRobots {
		p.HasMetaNoIndex = p.HasMetaNoIndex || noIndex
		p.HasMetaNoFollow = p.HasMetaNoFollow || noFollow
	}

	return
}

// parse object tag attributes
func (p *HTMLParser) parseObjectAttrs(tokenizer *html.Tokenizer) {
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, dataWordBytes) == 0 {
			u, err := ParseAndNormalizeURL(strings.TrimSpace(string(val)))
			if err == nil {
				p.Links = append(p.Links, u)
			}
			return
		}

		if !moreAttr {
			break
		}
	}
}

// parse embed tag attributes
func (p *HTMLParser) parseEmbedAttrs(tokenizer *html.Tokenizer) {
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, srcWordBytes) == 0 {
			u, err := ParseAndNormalizeURL(strings.TrimSpace(string(val)))
			if err == nil {
				p.Links = append(p.Links, u)
			}
			return
		}

		if !moreAttr {
			break
		}
	}
}

// parseAnchorAttrs iterates over all of the attributes in the current anchor
// token. It adds links when found in the href attribute.
func (p *HTMLParser) parseAnchorAttrs(tokenizer *html.Tokenizer) {
	for {
		key, val, moreAttr := tokenizer.TagAttr()
		if bytes.Compare(key, []byte("href")) == 0 {
			u, err := ParseAndNormalizeURL(strings.TrimSpace(string(val)))
			if err == nil {
				p.Links = append(p.Links, u)
			}
		}
		if !moreAttr {
			return
		}
	}
}
