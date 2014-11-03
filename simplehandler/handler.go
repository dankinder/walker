package simplehandler

import (
	"io"
	"os"
	"path/filepath"

	"github.com/iParadigms/walker"

	"code.google.com/p/log4go"
)

// Handler just writes returned pages as files locally, naming the
// file after the URL of the request.
type Handler struct{}

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
	log4go.Debug("Creating dir %v", dir)
	if err := os.MkdirAll(dir, 0777); err != nil {
		log4go.Error(err.Error())
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
