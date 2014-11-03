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
	h := &Handler{}

	page1URL := helpers.Parse("http://test.com/page1.html")
	page1Contents := []byte("<html>stuff</html>")
	page1Fetch := &walker.FetchResults{
		URL:              page1URL,
		ExcludedByRobots: false,
		Response: &http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: 18,
			Body:          ioutil.NopCloser(bytes.NewReader(page1Contents)),
			Request: &http.Request{
				Method:        "GET",
				URL:           page1URL.URL,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				ContentLength: 18,
				Host:          "test.com",
			},
		},
	}

	h.HandleResponse(page1Fetch)
	file := "test.com/page1.html"
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		t.Fatalf("Could not read expected file(%v): %v", file, err)
	}
	if string(contents) != string(page1Contents) {
		t.Errorf("Page contents not correctly written to file, expected %v\nBut got: %v",
			string(page1Contents), string(contents))
	}
	os.Remove(file)
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
