// +build sudo

package test

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/helpers"
	"github.com/stretchr/testify/mock"
)

const html_body string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>Test norobots site</title>
</head>

<div id="menu">
	<a href="/dir1/">Dir1</a>
	<a href="/dir2/">Dir2</a>
	<a id="other" href="http://other.com/" title="stuff">Other</a>
</div>
</html>`

const html_body_nolinks string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
<div id="menu">
</div>
</html>`

const html_test_links string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>Test links page</title>
</head>

<div id="menu">
	<a href="relative-dir/">link</a>
	<a href="relative-page/page.html">link</a>
	<a href="/abs-relative-dir/">link</a>
	<a href="/abs-relative-page/page.html">link</a>
	<a href="https://other.org/abs-dir/">link</a>
	<a href="https://other.org/abs-page/page.html">link</a>
	<a href="javascript:doStuff();">link</a>
	<a href="ftp:ignoreme.zip;">link</a>
	<a href="ftP:ignoreme.zip;">link</a>
	<a href="hTTP:donot/ignore.html">link</a>
</div>
</html>`

func init() {
	helpers.LoadTestConfig("test-walker.yaml")
}

func TestBasicFetchManagerRun(t *testing.T) {
	ds := &helpers.MockDatastore{}
	ds.On("ClaimNewHost").Return("norobots.com").Once()
	ds.On("LinksForHost", "norobots.com").Return([]*walker.URL{
		helpers.Parse("http://norobots.com/page1.html"),
		helpers.Parse("http://norobots.com/page2.html"),
		helpers.Parse("http://norobots.com/page3.html"),
	})
	ds.On("UnclaimHost", "norobots.com").Return()

	ds.On("ClaimNewHost").Return("robotsdelay1.com").Once()
	ds.On("LinksForHost", "robotsdelay1.com").Return([]*walker.URL{
		helpers.Parse("http://robotsdelay1.com/page4.html"),
		helpers.Parse("http://robotsdelay1.com/page5.html"),
	})
	ds.On("UnclaimHost", "robotsdelay1.com").Return()

	ds.On("ClaimNewHost").Return("accept.com").Once()
	ds.On("LinksForHost", "accept.com").Return([]*walker.URL{
		helpers.Parse("http://accept.com/accept_html.html"),
		helpers.Parse("http://accept.com/accept_text.txt"),
		helpers.Parse("http://accept.com/donthandle"),
	})
	ds.On("UnclaimHost", "accept.com").Return()

	ds.On("ClaimNewHost").Return("linktests.com").Once()
	ds.On("LinksForHost", "linktests.com").Return([]*walker.URL{
		helpers.Parse("http://linktests.com/links/test.html"),
	})
	ds.On("UnclaimHost", "linktests.com").Return()

	// This last call will make ClaimNewHost return "" on each subsequent call,
	// which will put the fetcher to sleep.
	ds.On("ClaimNewHost").Return("")

	ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()
	ds.On("StoreParsedURL",
		mock.AnythingOfType("*walker.URL"),
		mock.AnythingOfType("*walker.FetchResults")).Return()

	h := &helpers.MockHandler{}
	h.On("HandleResponse", mock.Anything).Return()

	rs, err := helpers.NewMockRemoteServer()
	if err != nil {
		t.Fatal(err)
	}
	rs.SetResponse("http://norobots.com/robots.txt", &helpers.MockResponse{Status: 404})
	rs.SetResponse("http://norobots.com/page1.html", &helpers.MockResponse{
		Body: html_body,
	})
	rs.SetResponse("http://robotsdelay1.com/robots.txt", &helpers.MockResponse{
		Body: "User-agent: *\nCrawl-delay: 1\n",
	})

	walker.Config.AcceptFormats = []string{"text/html", "text/plain"}
	rs.SetResponse("http://accept.com/robots.txt", &helpers.MockResponse{Status: 404})
	rs.SetResponse("http://accept.com/accept_html.html", &helpers.MockResponse{
		ContentType: "text/html; charset=ISO-8859-4",
		Body:        html_body_nolinks,
	})
	rs.SetResponse("http://accept.com/accept_text.txt", &helpers.MockResponse{
		ContentType: "text/plain",
	})
	rs.SetResponse("http://accept.com/donthandle", &helpers.MockResponse{
		ContentType: "foo/bar",
	})
	rs.SetResponse("http://linktests.com/links/test.html", &helpers.MockResponse{
		Body: html_test_links,
	})
	manager := &walker.FetchManager{
		Datastore: ds,
		Handler:   h,
		Transport: helpers.GetFakeTransport(),
	}

	go manager.Start()
	time.Sleep(time.Second * 3)
	manager.Stop()

	rs.Stop()
	recvTextHtml := false
	recvTextPlain := false
	for _, call := range h.Calls {
		fr := call.Arguments.Get(0).(*walker.FetchResults)
		switch fr.URL.String() {
		case "http://norobots.com/page1.html":
			contents, _ := ioutil.ReadAll(fr.Response.Body)
			if string(contents) != html_body {
				t.Errorf("For %v, expected:\n%v\n\nBut got:\n%v\n",
					fr.URL, html_body, string(contents))
			}
		case "http://norobots.com/page2.html":
		case "http://norobots.com/page3.html":
		case "http://robotsdelay1.com/page4.html":
		case "http://robotsdelay1.com/page5.html":
		case "http://accept.com/accept_html.html":
			recvTextHtml = true
		case "http://accept.com/accept_text.txt":
			recvTextPlain = true
		case "http://linktests.com/links/test.html":
		default:
			t.Errorf("Got a Handler.HandleResponse call we didn't expect: %v", fr)
		}
	}
	if !recvTextHtml {
		t.Errorf("Failed to handle explicit Content-Type: text/html")
	}
	if !recvTextPlain {
		t.Errorf("Failed to handle Content-Type: text/plain")
	}

	// Link tests to ensure we resolve URLs to proper absolute forms
	expectedMimesFound := map[string]string{
		"http://accept.com/donthandle":       "foo/bar",
		"http://accept.com/accept_text.txt":  "text/plain",
		"http://accept.com/accept_html.html": "text/html",
	}

	for _, call := range ds.Calls {
		switch call.Method {
		case "StoreParsedURL":
			u := call.Arguments.Get(0).(*walker.URL)
			fr := call.Arguments.Get(1).(*walker.FetchResults)
			if fr.URL.String() != "http://linktests.com/links/test.html" {
				continue
			}
			switch u.String() {
			case "http://linktests.com/links/relative-dir/":
			case "http://linktests.com/links/relative-page/page.html":
			case "http://linktests.com/abs-relative-dir/":
			case "http://linktests.com/abs-relative-page/page.html":
			case "https://other.org/abs-dir/":
			case "https://other.org/abs-page/page.html":
			case "http:donot/ignore.html":
			default:
				t.Errorf("StoreParsedURL call we didn't expect: %v", u)
			}

		case "StoreURLFetchResults":
			fr := call.Arguments.Get(0).(*walker.FetchResults)
			link := fr.URL.String()
			mime, mimeOk := expectedMimesFound[link]
			if mimeOk {
				delete(expectedMimesFound, link)
				if fr.MimeType != mime {
					t.Errorf("StoreURLFetchResults for link %v, got mime type %q, expected %q",
						link, fr.MimeType, mime)
				}
			}
		}
	}
	for link := range expectedMimesFound {
		t.Errorf("StoreURLFetchResults expected to find mime type for link %v, but didn't", link)
	}

	ds.AssertExpectations(t)
	h.AssertExpectations(t)
}

func TestFetcherBlacklistsPrivateIPs(t *testing.T) {
	orig := walker.Config.BlacklistPrivateIPs
	defer func() { walker.Config.BlacklistPrivateIPs = orig }()
	walker.Config.BlacklistPrivateIPs = true

	ds := &helpers.MockDatastore{}
	ds.On("ClaimNewHost").Return("private.com").Once()
	ds.On("UnclaimHost", "private.com").Return()
	ds.On("ClaimNewHost").Return("")

	h := &helpers.MockHandler{}

	rs, err := helpers.NewMockRemoteServer()
	if err != nil {
		t.Fatal(err)
	}

	manager := &walker.FetchManager{
		Datastore: ds,
		Handler:   h,
		Transport: helpers.GetFakeTransport(),
	}

	go manager.Start()
	time.Sleep(time.Second * 1)
	manager.Stop()
	rs.Stop()

	if len(h.Calls) != 0 {
		t.Error("Did not expect any handler calls due to host resolving to private IP")
	}

	ds.AssertExpectations(t)
	h.AssertExpectations(t)
	ds.AssertNotCalled(t, "LinksForHost", "private.com")
}

func TestStillCrawlWhenDomainUnreachable(t *testing.T) {
	orig := walker.Config.BlacklistPrivateIPs
	defer func() { walker.Config.BlacklistPrivateIPs = orig }()
	walker.Config.BlacklistPrivateIPs = true

	ds := &helpers.MockDatastore{}
	ds.On("ClaimNewHost").Return("a1234567890bcde.com").Once()
	ds.On("LinksForHost", "a1234567890bcde.com").Return([]*walker.URL{
		helpers.Parse("http://a1234567890bcde.com/"),
	})
	ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()
	ds.On("UnclaimHost", "a1234567890bcde.com").Return()
	ds.On("ClaimNewHost").Return("")

	h := &helpers.MockHandler{}

	manager := &walker.FetchManager{
		Datastore: ds,
		Handler:   h,
		Transport: helpers.GetFakeTransport(),
	}

	go manager.Start()
	time.Sleep(time.Millisecond * 10)
	manager.Stop()

	ds.AssertExpectations(t)
	h.AssertExpectations(t)
}

func TestFetcherCreatesTransport(t *testing.T) {
	orig := walker.Config.BlacklistPrivateIPs
	defer func() { walker.Config.BlacklistPrivateIPs = orig }()
	walker.Config.BlacklistPrivateIPs = false

	ds := &helpers.MockDatastore{}
	ds.On("ClaimNewHost").Return("localhost.localdomain").Once()
	ds.On("LinksForHost", "localhost.localdomain").Return([]*walker.URL{
		helpers.Parse("http://localhost.localdomain/"),
	})
	ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()
	ds.On("UnclaimHost", "localhost.localdomain").Return()
	ds.On("ClaimNewHost").Return("")

	h := &helpers.MockHandler{}
	h.On("HandleResponse", mock.Anything).Return()

	rs, err := helpers.NewMockRemoteServer()
	if err != nil {
		t.Fatal(err)
	}

	manager := &walker.FetchManager{
		Datastore: ds,
		Handler:   h,
	}

	go manager.Start()
	time.Sleep(time.Second * 1)
	manager.Stop()
	rs.Stop()

	if manager.Transport == nil {
		t.Fatalf("Expected Transport to get set")
	}
	_, ok := manager.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Expected Transport to get set to a *http.Transport")
	}

	// It would be great to check that the DNS cache actually got used here,
	// but with the current design there seems to be no way to check it

	ds.AssertExpectations(t)
	h.AssertExpectations(t)
}

func TestRedirects(t *testing.T) {
	link := func(index int) string {
		return fmt.Sprintf("http://sub.dom.com/page%d.html", index)
	}

	roundTriper := helpers.MapRoundTrip{
		Responses: map[string]*http.Response{
			link(1): helpers.Response307(link(2)),
			link(2): helpers.Response307(link(3)),
			link(3): helpers.Response200(),
		},
	}

	ds := &helpers.MockDatastore{}
	ds.On("ClaimNewHost").Return("dom.com").Once()
	ds.On("LinksForHost", "dom.com").Return([]*walker.URL{
		helpers.Parse(link(1)),
	})
	ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()
	ds.On("UnclaimHost", "dom.com").Return()
	ds.On("ClaimNewHost").Return("")

	h := &helpers.MockHandler{}
	h.On("HandleResponse", mock.Anything).Return()

	manager := &walker.FetchManager{
		Datastore: ds,
		Handler:   h,
		Transport: &roundTriper,
	}

	go manager.Start()
	time.Sleep(time.Second * 2)
	manager.Stop()
	if len(h.Calls) < 1 {
		t.Fatalf("Expected to find calls made to handler, but didn't")
	}
	fr := h.Calls[0].Arguments.Get(0).(*walker.FetchResults)

	if fr.URL.String() != link(1) {
		t.Errorf("URL mismatch, got %q, expected %q", fr.URL.String(), link(1))
	}
	if len(fr.RedirectedFrom) != 2 {
		t.Errorf("RedirectedFrom length mismatch, got %d, expected %d", len(fr.RedirectedFrom), 2)
	}
	if fr.RedirectedFrom[0].String() != link(2) {
		t.Errorf("RedirectedFrom[0] mismatch, got %q, expected %q", fr.RedirectedFrom[0].String(), link(2))
	}
	if fr.RedirectedFrom[1].String() != link(3) {
		t.Errorf("RedirectedFrom[0] mismatch, got %q, expected %q", fr.RedirectedFrom[1].String(), link(3))
	}

	ds.AssertExpectations(t)
	h.AssertExpectations(t)
}

func TestHrefWithSpace(t *testing.T) {
	testPage := "http://t.com/page1.html"
	const html_with_href_space = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>Test links page</title>
</head>

<div id="menu">
	<a href=" relative-dir/">link</a>
	<a href=" relative-page/page.html">link</a>
	<a href=" /abs-relative-dir/">link</a>
	<a href=" /abs-relative-page/page.html">link</a>
	<a href=" https://other.org/abs-dir/">link</a>
	<a href=" https://other.org/abs-page/page.html">link</a>
</div>
</html>`

	ds := &helpers.MockDatastore{}
	ds.On("ClaimNewHost").Return("t.com").Once()
	ds.On("LinksForHost", "t.com").Return([]*walker.URL{
		helpers.Parse(testPage),
	})
	ds.On("UnclaimHost", "t.com").Return()
	ds.On("ClaimNewHost").Return("")

	ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()
	ds.On("StoreParsedURL",
		mock.AnythingOfType("*walker.URL"),
		mock.AnythingOfType("*walker.FetchResults")).Return()

	h := &helpers.MockHandler{}
	h.On("HandleResponse", mock.Anything).Return()

	rs, err := helpers.NewMockRemoteServer()
	if err != nil {
		t.Fatal(err)
	}
	rs.SetResponse(testPage, &helpers.MockResponse{
		ContentType: "text/html",
		Body:        html_with_href_space,
	})

	manager := &walker.FetchManager{
		Datastore: ds,
		Handler:   h,
		Transport: helpers.GetFakeTransport(),
	}

	go manager.Start()
	time.Sleep(time.Second * 2)
	manager.Stop()

	rs.Stop()

	foundTCom := false
	for _, call := range h.Calls {
		fr := call.Arguments.Get(0).(*walker.FetchResults)
		if fr.URL.String() == testPage {
			foundTCom = true
			break
		}
	}
	if !foundTCom {
		t.Fatalf("Failed to find pushed link 'http://t.com/page1.html'")
	}

	expected := map[string]bool{
		"http://t.com/relative-dir/":               true,
		"http://t.com/relative-page/page.html":     true,
		"http://t.com/abs-relative-dir/":           true,
		"http://t.com/abs-relative-page/page.html": true,
		"https://other.org/abs-dir/":               true,
		"https://other.org/abs-page/page.html":     true,
	}

	for _, call := range ds.Calls {
		if call.Method == "StoreParsedURL" {
			u := call.Arguments.Get(0).(*walker.URL)
			fr := call.Arguments.Get(1).(*walker.FetchResults)
			if fr.URL.String() == testPage {
				if expected[u.String()] {
					delete(expected, u.String())
				} else {
					t.Errorf("StoreParsedURL mismatch found unexpected link %q", u.String())
				}
			}
		}
	}

	for link, _ := range expected {
		t.Errorf("StoreParsedURL didn't find link %q", link)
	}

	ds.AssertExpectations(t)
	h.AssertExpectations(t)
}

func TestHttpTimeout(t *testing.T) {
	origTimeout := walker.Config.HttpTimeout
	walker.Config.HttpTimeout = "200ms"
	defer func() {
		walker.Config.HttpTimeout = origTimeout
	}()

	for _, timeoutType := range []string{"wontConnect", "stalledRead"} {

		ds := &helpers.MockDatastore{}
		ds.On("ClaimNewHost").Return("t1.com").Once()
		ds.On("LinksForHost", "t1.com").Return([]*walker.URL{
			helpers.Parse("http://t1.com/page1.html"),
		})
		ds.On("UnclaimHost", "t1.com").Return()

		ds.On("ClaimNewHost").Return("t2.com").Once()
		ds.On("LinksForHost", "t2.com").Return([]*walker.URL{
			helpers.Parse("http://t2.com/page1.html"),
		})
		ds.On("UnclaimHost", "t2.com").Return()

		ds.On("ClaimNewHost").Return("t3.com").Once()
		ds.On("LinksForHost", "t3.com").Return([]*walker.URL{
			helpers.Parse("http://t3.com/page1.html"),
		})
		ds.On("UnclaimHost", "t3.com").Return()

		ds.On("ClaimNewHost").Return("")

		ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()
		ds.On("StoreParsedURL",
			mock.AnythingOfType("*walker.URL"),
			mock.AnythingOfType("*walker.FetchResults")).Return()

		h := &helpers.MockHandler{}
		h.On("HandleResponse", mock.Anything).Return()

		var transport *helpers.CancelTrackingTransport
		var closer io.Closer
		if timeoutType == "wontConnect" {
			transport, closer = helpers.GetWontConnectTransport()
		} else {
			transport, closer = helpers.GetStallingReadTransport()
		}

		manager := &walker.FetchManager{
			Datastore: ds,
			Handler:   h,
			Transport: transport,
		}

		go manager.Start()
		time.Sleep(time.Second * 2)
		manager.Stop()
		closer.Close()

		canceled := map[string]bool{}
		for k := range transport.Canceled {
			canceled[k] = true
		}

		expected := map[string]bool{
			"http://t1.com/page1.html": true,
			"http://t2.com/page1.html": true,
			"http://t3.com/page1.html": true,
		}

		for k := range expected {
			if !canceled[k] {
				t.Errorf("For timeoutType %q Expected to find canceled http get for %q, but didn't", timeoutType, k)
			}
		}

		if len(h.Calls) > 0 {
			t.Fatalf("For timeoutType %q Fetcher shouldn't have been able to connect, but did", timeoutType)
		}
	}
}

func TestMetaNos(t *testing.T) {
	origHonorNoindex := walker.Config.HonorMetaNoindex
	origHonorNofollow := walker.Config.HonorMetaNofollow
	defer func() {
		walker.Config.HonorMetaNoindex = origHonorNoindex
		walker.Config.HonorMetaNofollow = origHonorNofollow
	}()
	walker.Config.HonorMetaNoindex = true
	walker.Config.HonorMetaNofollow = true

	const nofollowHtml string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<meta name="ROBOTS" content="NoFollow">
<title>No Links</title>
</head>
<div id="menu">
	<a href="relative-dir/">link</a>
	<a href="relative-page/page.html">link</a>
	<a href="/abs-relative-dir/">link</a>
	<a href="/abs-relative-page/page.html">link</a>
	<a href="https://other.org/abs-dir/">link</a>
	<a href="https://other.org/abs-page/page.html">link</a>
</div>
</html>`

	const noindexHtml string = `<!DOCTYPE html>
<html>
<head>
<meta name="ROBOTS" content="noindex">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
</html>`

	const bothHtml string = `<!DOCTYPE html>
<html>
<head>
<meta name="ROBOTS" content="noindeX, nofoLLow">
<title>No Links</title>
</head>
div id="menu">
	<a href="relative-dirX/">link</a>
	<a href="relative-pageX/page.html">link</a>
	<a href="/abs-relative-dirX/">link</a>
	<a href="/abs-relative-pageX/page.html">link</a>
	<a href="https://other.org/abs-dirX/">link</a>
	<a href="https://other.org/abs-pageX/page.html">link</a>
</div>
</html>`

	ds := &helpers.MockDatastore{}
	ds.On("ClaimNewHost").Return("t1.com").Once()
	ds.On("LinksForHost", "t1.com").Return([]*walker.URL{
		helpers.Parse("http://t1.com/nofollow.html"),
		helpers.Parse("http://t1.com/noindex.html"),
		helpers.Parse("http://t1.com/both.html"),
	})
	ds.On("UnclaimHost", "t1.com").Return()
	ds.On("ClaimNewHost").Return("")

	ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()
	ds.On("StoreParsedURL",
		mock.AnythingOfType("*walker.URL"),
		mock.AnythingOfType("*walker.FetchResults")).Return()

	h := &helpers.MockHandler{}
	h.On("HandleResponse", mock.Anything).Return()

	rs, err := helpers.NewMockRemoteServer()
	if err != nil {
		t.Fatal(err)
	}
	rs.SetResponse("http://t1.com/nofollow.html", &helpers.MockResponse{
		Body: nofollowHtml,
	})
	rs.SetResponse("http://t1.com/noindex.html", &helpers.MockResponse{
		Body: noindexHtml,
	})
	rs.SetResponse("http://t1.com/both.html", &helpers.MockResponse{
		Body: bothHtml,
	})

	manager := &walker.FetchManager{
		Datastore: ds,
		Handler:   h,
		Transport: helpers.GetFakeTransport(),
	}

	go manager.Start()
	time.Sleep(time.Second * 2)
	manager.Stop()

	rs.Stop()

	// Did the fetcher honor noindex (if noindex is set
	// the handler shouldn't be called)
	callCount := 0
	for _, call := range h.Calls {
		fr := call.Arguments.Get(0).(*walker.FetchResults)
		link := fr.URL.String()
		switch link {
		case "http://t1.com/nofollow.html":
			callCount++
		default:
			t.Errorf("Fetcher did not honor noindex in meta link = %s", link)
		}
	}
	if callCount != 1 {
		t.Errorf("Expected call to handler for nofollow.html, but didn't get it")
	}

	// Did the fetcher honor nofollow (if nofollow is set fetcher
	// shouldn't follow any links)
	callCount = 0
	for _, call := range ds.Calls {
		if call.Method == "StoreParsedURL" {
			callCount++
		}
	}
	if callCount != 0 {
		t.Errorf("Fetcher did not honor nofollow in meta: expected 0 callCount, found %d", callCount)
	}
}

func TestFetchManagerFastShutdown(t *testing.T) {
	origDefaultCrawlDelay := walker.Config.DefaultCrawlDelay
	defer func() {
		walker.Config.DefaultCrawlDelay = origDefaultCrawlDelay
	}()
	walker.Config.DefaultCrawlDelay = 1

	ds := &helpers.MockDatastore{}
	ds.On("ClaimNewHost").Return("test.com").Once()
	ds.On("LinksForHost", "test.com").Return([]*walker.URL{
		helpers.Parse("http://test.com/page1.html"),
		helpers.Parse("http://test.com/page2.html"),
	})
	ds.On("UnclaimHost", "test.com").Return()
	ds.On("ClaimNewHost").Return("")

	ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()

	manager := &walker.FetchManager{
		Datastore: ds,
		Handler:   &helpers.MockHandler{},
		Transport: helpers.GetFakeTransport(),
	}

	go manager.Start()
	time.Sleep(time.Millisecond * 200)
	manager.Stop()

	expectedCall := false
	for _, call := range ds.Calls {
		switch call.Method {
		case "StoreURLFetchResults":
			fr := call.Arguments.Get(0).(*walker.FetchResults)
			link := fr.URL.String()
			switch link {
			case "http://test.com/page1.html":
				expectedCall = true
			default:
				t.Errorf("Got unexpected StoreURLFetchResults call for %v", link)
			}
		}
	}
	if !expectedCall {
		t.Errorf("Did not get expected StoreURLFetchResults call for http://test.com/page1.html")
	}

	ds.AssertExpectations(t)
}
