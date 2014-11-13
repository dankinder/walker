package simplehandler

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/helpers"
)

func TestSimpleWriterHandler(t *testing.T) {
	handlertests := []struct {
		URL          *walker.URL
		PageContents []byte
		ExpectedDir  string // "" if we expect no directory
		ExpectedFile string // "" if we expect no file matching PageContents
	}{
		{
			helpers.Parse("http://test.com/page1.html"),
			[]byte("<html>stuff</html>"),
			"test.com",
			"test.com/page1.html",
		},
		{
			helpers.Parse("http://test.com/"),
			[]byte("<html>stuff</html>"),
			"test.com",
			"",
		},
		{
			helpers.Parse("http://test.com/a-dir/"),
			[]byte("<html>blah</html>"),
			"test.com/a-dir",
			"",
		},
		{
			helpers.Parse("http://test.com/a-dir/myfile"),
			[]byte(""),
			"test.com/a-dir",
			"test.com/a-dir/myfile",
		},
	}

	h := &Handler{}
	for _, ht := range handlertests {

		fr := &walker.FetchResults{
			URL: ht.URL,
			Response: &http.Response{
				Status:        "200 OK",
				StatusCode:    200,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				ContentLength: int64(len(ht.PageContents)),
				Body:          ioutil.NopCloser(bytes.NewReader(ht.PageContents)),
				Request: &http.Request{
					Method:        "GET",
					URL:           ht.URL.URL,
					Proto:         "HTTP/1.1",
					ProtoMajor:    1,
					ProtoMinor:    1,
					ContentLength: int64(len(ht.PageContents)),
					Host:          ht.URL.Host,
				},
			},
		}
		h.HandleResponse(fr)

		if ht.ExpectedDir != "" {
			_, err := os.Stat(ht.ExpectedDir)
			if err != nil {
				t.Fatalf("Could not stat expected dir(%v): %v", ht.ExpectedDir, err)
			}
		}

		if ht.ExpectedFile != "" {
			contents, err := ioutil.ReadFile(ht.ExpectedFile)
			if err != nil {
				t.Fatalf("Could not read expected file(%v): %v", ht.ExpectedFile, err)
			}
			if string(contents) != string(ht.PageContents) {
				t.Errorf("Page contents not correctly written to file, expected %v\nBut got: %v",
					string(ht.PageContents), string(contents))
			}
		}

		os.RemoveAll(ht.URL.Host)
	}
}

func TestSimpleWriterHandlerIgnoresOnRobots(t *testing.T) {
	h := &Handler{}

	page2URL := helpers.Parse("http://test.com/page2.html")
	page2Contents := []byte("<html>stuff</html>")
	page2Fetch := &walker.FetchResults{
		URL:              page2URL,
		ExcludedByRobots: true,
		Response: &http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: 18,
			Body:          ioutil.NopCloser(bytes.NewReader(page2Contents)),
			Request: &http.Request{
				Method:        "GET",
				URL:           page2URL.URL,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				ContentLength: 18,
				Host:          "test.com",
			},
		},
	}

	h.HandleResponse(page2Fetch)
	file := "test.com-page2.html"
	_, err := ioutil.ReadFile(file)
	if err == nil {
		t.Errorf("File should not have been created due to robots.txt: %v", file)
	}
}

func TestSimpleWriterHandlerIgnoresBadHTTPCode(t *testing.T) {
	h := &Handler{}

	page3URL := helpers.Parse("http://test.com/page3.html")
	page3Contents := []byte("<html>stuff</html>")
	page3Fetch := &walker.FetchResults{
		URL:              page3URL,
		ExcludedByRobots: false,
		Response: &http.Response{
			Status:        "404 NOT FOUND",
			StatusCode:    404,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: 18,
			Body:          ioutil.NopCloser(bytes.NewReader(page3Contents)),
			Request: &http.Request{
				Method:        "GET",
				URL:           page3URL.URL,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				ContentLength: 18,
				Host:          "test.com",
			},
		},
	}

	h.HandleResponse(page3Fetch)
	file := "test.com-page3.html"
	_, err := ioutil.ReadFile(file)
	if err == nil {
		t.Errorf("File should not have been created due http error code: %v", file)
	}
}
