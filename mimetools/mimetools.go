/*
Package mimetools provides functions for matching against media types

Also referred to ad MIME types, see http://en.wikipedia.org/wiki/Internet_media_type

It primarily exports Matcher:

	mm := mimetools.NewMatcher([]string{"text/*", "application/json"})
	mm.Match("text/html") // returns true, nil
	mm.Match("text/plain") //returns true, nil
	mm.Match("application/json") // returns true, nil
	mm.Match("application/vnd.ms-excel") // returns false, nil
*/
package mimetools

import (
	"mime"
	"strings"
)

// Matcher will match Accept or Content-Type mime types that include *
type Matcher struct {
	// allOk is true, means any mime type is accepted
	allOk bool

	// exact mime type matches
	exact map[string]bool

	// mime type 'text/*' will cause prefix to hold the string 'text/',
	// and will match any string that starts with 'text/'
	prefix []string

	// for completeness '*/html' will cause '/html' in suffix, and match
	// any string that ends with '/html'
	suffix []string
}

//IMPL NOTE: This implementation could be made into a single regex call. But
//that would require very carefully auditing the input mediaTypes, protecting
//regex characters and such. Seemed safer and easier to build this.

// NewMatcher returns a new matcher instance and any errors that occurred.
// Note that if an error is returned, a Matcher is returned that matches
// any mime type.
func NewMatcher(mediaTypes []string) (*Matcher, error) {
	mm := &Matcher{
		allOk:  false,
		exact:  make(map[string]bool),
		prefix: []string{},
		suffix: []string{},
	}
	for _, x := range mediaTypes {
		err := mm.addMediaType(x)
		if err != nil {
			mm.allOk = true
			return mm, err
		}
	}

	return mm, nil
}

func (mm *Matcher) addMediaType(mimeString string) error {
	mediaName, _, err := mime.ParseMediaType(mimeString)
	if err != nil {
		return err
	}
	if mediaName == "*/*" {
		mm.allOk = true
	} else if strings.HasPrefix(mediaName, "*/") {
		mm.suffix = append(mm.suffix, strings.Replace(mediaName, "*", "", 1))

	} else if strings.HasSuffix(mediaName, "/*") {
		mm.prefix = append(mm.prefix, strings.Replace(mediaName, "*", "", 1))
	} else {
		mm.exact[mediaName] = true
	}
	return nil
}

// Match returns true if mimeString matches, false otherwise.
func (mm *Matcher) Match(mimeString string) (bool, error) {
	mediaName, _, err := mime.ParseMediaType(mimeString)
	if err != nil {
		return false, err
	}

	if mm.allOk {
		return true, nil
	}
	if mm.exact[mediaName] {
		return true, nil
	}
	for _, p := range mm.prefix {
		if strings.HasPrefix(mediaName, p) {
			return true, nil
		}
	}
	for _, s := range mm.suffix {
		if strings.HasSuffix(mediaName, s) {
			return true, nil
		}
	}

	return false, nil
}
