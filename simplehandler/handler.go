/*
Package simplehandler provides a basic walker handler implementation
*/
package simplehandler

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/iParadigms/walker"

	"code.google.com/p/log4go"
)

// Handler implements an object that conforms to walker.Handler interface.
type Handler struct{}

// HandleResponse just writes returned pages as files locally, naming the file
// after the URL of the request made.
//
// For example, when handling the response for
// `http://test.com/amazing/stuff.html`, it will create the directory
// `$PWD/test.com/amazing` and write the page contents (no headers or HTTP
// data) to `$PWD/test.com/amazing/stuff.html`
//
// It skips pages that do not have a 2XX HTTP code.
func (h *Handler) HandleResponse(fr *walker.FetchResults) {
	if fr.ExcludedByRobots {
		log4go.Debug("Excluded by robots.txt, ignoring url: %v", fr.URL)
		return
	}
	if fr.Response.StatusCode < 200 || fr.Response.StatusCode >= 300 {
		log4go.Debug("Returned %v ignoring url: %v", fr.Response.StatusCode, fr.URL)
		return
	}

	path := filepath.Join(fr.URL.Host, fr.URL.RequestURI())
	dir, _ := filepath.Split(path)
	if dir == "" {
		dir = fr.URL.Host
	}
	log4go.Debug("Creating dir %v", dir)
	if err := os.MkdirAll(dir, 0777); err != nil {
		log4go.Error(err.Error())
		return
	}

	if strings.HasSuffix(path, "/") || path == dir {
		// Don't store directory pages; no sensible name to use for them
		return
	}

	out, err := os.Create(path)
	log4go.Debug("Creating file %v", path)
	if err != nil {
		log4go.Error(err.Error())
		return
	}
	defer func() {
		log4go.Debug("Closing file %v", path)
		err := out.Close()
		if err != nil {
			log4go.Error(err.Error())
		}
	}()
	log4go.Debug("Copying contents to %v", path)
	_, err = io.Copy(out, fr.Response.Body)
	if err != nil {
		log4go.Error(err.Error())
		return
	}
}
