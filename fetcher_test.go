// +build sudo

package walker

import (
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
)

const defaultSleep time.Duration = time.Millisecond * 40

func init() {
	LoadTestConfig("test-walker.yaml")
}

//
// Test table structs
//

// LinkSpec describes a mocked link
type LinkSpec struct {
	// The url of the link
	url string

	// The time last crawled for this link
	lastCrawled time.Time

	// The response the mock server should deliver for this url
	response *MockResponse

	// This should be true if this link is a robots.txt path
	robots bool
}

// DomainSpec describes a mocked domain
type DomainSpec struct {
	// Name of the domain
	domain string

	// Links mocked for this host
	links []LinkSpec
}

// TestSpec describes an entire mocked fetcher urn
type TestSpec struct {
	// List of mocked domains to include in test
	hosts []DomainSpec

	// This should be set true if any of the links stored in hosts
	// has links embedded in html
	hasParsedLinks bool

	// This should be true if the mocked data store does/should not
	// return ANY links
	hasNoLinks bool

	// This should be true, if an explicit transport should NOT be
	// provided to the FetchManager during initialization
	suppressTransport bool

	// An alternate transport to provide to FetchManager. If suppressTransport
	// is false, and transport is nil, the FetchManger uses getFakeTransport()
	transport http.RoundTripper

	// Allows user to set the TransNoKeepAlive on fetch manager
	transNoKeepAlive http.RoundTripper

	// true means do not mock a remote server during this particular test
	suppressMockServer bool
}

//
// Test result class
//
// TestResults represents the results of a runFetcher invocation
type TestResults struct {
	// mock server used for test
	server *MockRemoteServer

	// mock datastore used for test
	datastore *MockDatastore

	// mock handler used for test
	handler *MockHandler

	// FetchManager used for test
	manager *FetchManager
}

// handlerCalls will return a list of all FetchResults passed to
// TestResults.handler during the test.
func (self *TestResults) handlerCalls() []*FetchResults {
	var ret []*FetchResults
	for _, call := range self.handler.Calls {
		fr := call.Arguments.Get(0).(*FetchResults)
		ret = append(ret, fr)
	}
	return ret
}

// dsStoreParsedURLCalls will return a list of URLs and their associated
// FetchResults that are passed to TestResults.datastore.StoreParsedUrl
// during the test.
func (self *TestResults) dsStoreParsedURLCalls() ([]*URL, []*FetchResults) {
	var r1 []*URL
	var r2 []*FetchResults
	for _, call := range self.datastore.Calls {
		if call.Method == "StoreParsedURL" {
			u := call.Arguments.Get(0).(*URL)
			fr := call.Arguments.Get(1).(*FetchResults)
			r1 = append(r1, u)
			r2 = append(r2, fr)
		}
	}
	return r1, r2
}

// dsStoreURLFetchResultsCalls will return a list of FetchResults passed to
// TestResults.datastore.StoreUrlFetchResults during test.
func (self *TestResults) dsStoreURLFetchResultsCalls() []*FetchResults {
	var r1 []*FetchResults
	for _, call := range self.datastore.Calls {
		if call.Method == "StoreURLFetchResults" {
			fr := call.Arguments.Get(0).(*FetchResults)
			r1 = append(r1, fr)
		}
	}
	return r1
}

func (self *TestResults) dsCountKeepAliveCalls() int {
	r1 := 0
	for _, call := range self.datastore.Calls {
		if call.Method == "KeepAlive" {
			r1++
		}
	}
	return r1
}

// assertExpectations verifies the expectations set up for the mocked
// datastore and handler.
func (self *TestResults) assertExpectations(t *testing.T) {
	self.datastore.AssertExpectations(t)
	self.handler.AssertExpectations(t)
}

//
// Couple convenience functions to generate DomainSpecs in one line.
//

// singleLinkDomainSpec can be used to provide a DomainSpec for a single link.
// This is just a convenience function.
func singleLinkDomainSpec(link string, response *MockResponse) DomainSpec {
	u := MustParse(link)
	domain, err := u.ToplevelDomainPlusOne()
	if err != nil {
		panic(err)
	}

	return DomainSpec{
		domain: domain,
		links: []LinkSpec{
			LinkSpec{
				url:      link,
				response: response,
			},
		},
	}

}

// singleLinkDomainSpecArr is a convenience function that returns []DomainSpec{singlLinkDomainSpec(link, response)}
func singleLinkDomainSpecArr(link string, response *MockResponse) []DomainSpec {
	return []DomainSpec{
		singleLinkDomainSpec(link, response),
	}
}

//
// runFetcher/runFetcherTimed interprets a TestSpec and runs a FetchManager in accordance with
// that specification.
//
func runFetcher(test TestSpec, t *testing.T) TestResults {
	return runFetcherTimed(test, 0*time.Second, t)
}

// If you run runFetcherTimed with a zero duration, it will call FetchManger.oneShot rather than
// having a timed-out FetchManger.Start()/FetchManager.Stop() pair.
func runFetcherTimed(test TestSpec, duration time.Duration, t *testing.T) TestResults {

	//
	// Build mocks
	//
	h := &MockHandler{}

	var rs *MockRemoteServer
	var err error
	if !test.suppressMockServer {
		rs, err = NewMockRemoteServer()
		if err != nil {
			t.Fatal(err)
		}
	}
	ds := &MockDatastore{}

	//
	// Configure mocks
	//
	ds.On("KeepAlive").Return(nil)

	if !test.hasNoLinks {
		ds.On("StoreURLFetchResults", mock.AnythingOfType("*walker.FetchResults")).Return()
	}
	if test.hasParsedLinks {
		ds.On("StoreParsedURL",
			mock.AnythingOfType("*walker.URL"),
			mock.AnythingOfType("*walker.FetchResults")).Return()

	}

	if !test.hasNoLinks {
		h.On("HandleResponse", mock.Anything).Return()
	}
	for _, host := range test.hosts {
		ds.On("ClaimNewHost").Return(host.domain).Once()
		var urls []*URL
		for _, link := range host.links {
			if !link.robots {
				u := MustParse(link.url)
				zero := time.Time{}
				if link.lastCrawled != zero {
					u.LastCrawled = link.lastCrawled
				}
				urls = append(urls, u)
			}

			if link.response != nil && !test.suppressMockServer {
				rs.SetResponse(link.url, link.response)
			}
		}
		if !test.hasNoLinks {
			ds.On("LinksForHost", host.domain).Return(urls)
		}
		ds.On("UnclaimHost", host.domain).Return()

	}
	// This last call will make ClaimNewHost return "" on each subsequent call,
	// which will put the fetcher to sleep.
	ds.On("ClaimNewHost").Return("")

	//
	// Run the manager
	//
	var transport http.RoundTripper
	if !test.suppressTransport {
		if test.transport != nil {
			transport = test.transport
		} else {
			transport = getFakeTransport()
		}
	}

	manager := &FetchManager{
		Datastore: ds,
		Handler:   h,
		Transport: transport,
	}

	if test.transNoKeepAlive != nil {
		manager.TransNoKeepAlive = test.transNoKeepAlive
	}

	zeroDur := 0 * time.Second
	if duration == zeroDur {
		manager.oneShot()
	} else {
		go manager.Start()
		time.Sleep(duration)
		manager.Stop()
	}

	if !test.suppressMockServer {
		rs.Stop()
	}

	//
	// Return the mocks
	//
	return TestResults{
		handler:   h,
		datastore: ds,
		manager:   manager,
		server:    rs,
	}
}

func TestUrlParsing(t *testing.T) {
	orig := Config.Fetcher.PurgeSidList
	defer func() {
		Config.Fetcher.PurgeSidList = orig
		PostConfigHooks()
	}()
	Config.Fetcher.PurgeSidList = []string{"jsessionid", "phpsessid"}
	PostConfigHooks()

	tests := []struct {
		tag    string
		input  string
		expect string
	}{
		{
			tag:    "UpCase",
			input:  "HTTP://A.com/page1.com",
			expect: "http://a.com/page1.com",
		},
		{
			tag:    "Fragment",
			input:  "http://a.com/page1.com#Fragment",
			expect: "http://a.com/page1.com",
		},
		{
			tag:    "PathSID",
			input:  "http://a.com/page1.com;jsEssIoniD=436100313FAFBBB9B4DC8BA3C2EC267B",
			expect: "http://a.com/page1.com",
		},
		{
			tag:    "PathSID2",
			input:  "http://a.com/page1.com;phPseSsId=436100313FAFBBB9B4DC8BA3C2EC267B",
			expect: "http://a.com/page1.com",
		},
		{
			tag:    "QuerySID",
			input:  "http://a.com/page1.com?foo=bar&jsessionID=436100313FAFBBB9B4DC8BA3C2EC267B&baz=niffler",
			expect: "http://a.com/page1.com?baz=niffler&foo=bar",
		},
		{
			tag:    "QuerySID2",
			input:  "http://a.com/page1.com?PHPSESSID=436100313FAFBBB9B4DC8BA3C2EC267B",
			expect: "http://a.com/page1.com",
		},
		{
			tag:    "EmbeddedPort",
			input:  "http://a.com:8080/page1.com",
			expect: "http://a.com:8080/page1.com",
		},
	}

	for _, tst := range tests {
		u, err := ParseAndNormalizeURL(tst.input)
		if err != nil {
			t.Fatalf("For tag %q ParseURL failed %v", tst.tag, err)
		}
		got := u.String()
		if got != tst.expect {
			t.Errorf("For tag %q link mismatch got %q, expected %q", tst.tag, got, tst.expect)
		}
	}
}
func TestBasicNoRobots(t *testing.T) {
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

	tests := TestSpec{
		hasParsedLinks: true,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "norobots.com",
				links: []LinkSpec{
					LinkSpec{
						url:      "http://norobots.com/robots.txt",
						response: &MockResponse{Status: 404},
						robots:   true,
					},
					LinkSpec{
						url:      "http://norobots.com/page1.html",
						response: &MockResponse{Body: html_body},
					},
					LinkSpec{
						url: "http://norobots.com/page2.html",
					},
					LinkSpec{
						url: "http://norobots.com/page3.html",
					},
				},
			},
		},
	}

	//
	// Run the fetcher
	//
	results := runFetcher(tests, t)

	//
	// Make sure KeepAlive was called
	//
	kacount := results.dsCountKeepAliveCalls()
	if kacount < 1 {
		t.Errorf("Expected KeepAlive to be called, but it wasn't")
	}

	//
	// Make sure expected results are there
	//
	expected := map[string]bool{
		"http://norobots.com/page1.html": true,
		"http://norobots.com/page2.html": true,
		"http://norobots.com/page3.html": true,
	}

	for _, fr := range results.handlerCalls() {
		link := fr.URL.String()
		if expected[link] {
			delete(expected, link)
		}
		switch link {
		case "http://norobots.com/page1.html":
			contents, _ := ioutil.ReadAll(fr.Response.Body)
			if string(contents) != html_body {
				t.Errorf("For %v, expected:\n%v\n\nBut got:\n%v\n",
					fr.URL, html_body, string(contents))
			}
		case "http://norobots.com/page2.html":
		case "http://norobots.com/page3.html":
		default:
			t.Errorf("Got a Handler.HandleResponse call we didn't expect: %v", fr)
		}
	}

	for link := range expected {
		t.Errorf("Expected to find %q in handlerCalls, but didn't", link)
	}

	results.assertExpectations(t)
}

func TestBasicRobots(t *testing.T) {
	tests := TestSpec{
		hasParsedLinks: false,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "robotsdelay1.com",
				links: []LinkSpec{

					LinkSpec{
						url: "http://robotsdelay1.com/robots.txt",
						response: &MockResponse{
							Body: "User-agent: *\nCrawl-delay: 1\n",
						},
						robots: true,
					},

					LinkSpec{
						url: "http://robotsdelay1.com/page4.html",
					},
					LinkSpec{
						url: "http://robotsdelay1.com/page5.html",
					},
				},
			},
		},
	}

	//
	// Run the fetcher
	//
	results := runFetcher(tests, t)

	//
	// Make sure expected results are there
	//
	expected := map[string]bool{
		"http://robotsdelay1.com/page4.html": true,
		"http://robotsdelay1.com/page5.html": true,
	}

	for _, fr := range results.handlerCalls() {
		link := fr.URL.String()
		if expected[link] {
			delete(expected, link)
		}
		switch fr.URL.String() {
		case "http://robotsdelay1.com/page4.html":
		case "http://robotsdelay1.com/page5.html":
		default:
			t.Errorf("Got a Handler.HandleResponse call we didn't expect: %v", fr)
		}
	}

	for link := range expected {
		t.Errorf("Didn't find %q in handlerCalls, but should have", link)
	}

	results.assertExpectations(t)
}

func TestBasicRobotsDisallow(t *testing.T) {
	tests := TestSpec{
		hasParsedLinks: false,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "robots.com",
				links: []LinkSpec{

					LinkSpec{
						url: "http://robots.com/robots.txt",
						response: &MockResponse{
							Body: "User-agent: *\nDisallow: /search\n",
						},
						robots: true,
					},

					LinkSpec{
						url: "http://robots.com/search",
					},
					LinkSpec{
						url: "http://robots.com/other",
					},
				},
			},
		},
	}

	//
	// Run the fetcher
	//
	results := runFetcher(tests, t)

	//
	// Make sure expected results are there
	//
	expected := map[string]bool{
		"http://robots.com/other": true,
	}

	for _, fr := range results.handlerCalls() {
		link := fr.URL.String()
		if expected[link] {
			delete(expected, link)
		}
		switch fr.URL.String() {
		case "http://robots.com/other":
		default:
			t.Errorf("Got a Handler.HandleResponse call we didn't expect: %v", fr)
		}
	}

	for link := range expected {
		t.Errorf("Didn't find %q in handlerCalls, but should have", link)
	}

	results.assertExpectations(t)
}

func TestBasicMimeType(t *testing.T) {
	orig := Config.Fetcher.AcceptFormats
	defer func() {
		Config.Fetcher.AcceptFormats = orig
	}()
	Config.Fetcher.AcceptFormats = []string{"text/html", "text/plain"}

	const html_body_nolinks string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
<div id="menu">
</div>
</html>`

	tests := TestSpec{
		hasParsedLinks: false,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "accept.com",
				links: []LinkSpec{
					LinkSpec{
						url:      "http://accept.com/robots.txt",
						response: &MockResponse{Status: 404},
						robots:   true,
					},
					LinkSpec{
						url: "http://accept.com/accept_html.html",
						response: &MockResponse{
							ContentType: "text/html; charset=ISO-8859-4",
							Body:        html_body_nolinks,
						},
					},
					LinkSpec{
						url: "http://accept.com/accept_text.txt",
						response: &MockResponse{
							ContentType: "text/plain",
						},
					},
					LinkSpec{
						url: "http://accept.com/donthandle",
						response: &MockResponse{
							ContentType: "foo/bar",
						},
					},
				},
			},
		},
	}

	//
	// Run the fetcher
	//
	results := runFetcher(tests, t)

	//
	// Make sure expected results are there
	//
	recvTextHtml := false
	recvTextPlain := false
	for _, fr := range results.handlerCalls() {
		switch fr.URL.String() {
		case "http://accept.com/accept_html.html":
			recvTextHtml = true
		case "http://accept.com/accept_text.txt":
			recvTextPlain = true
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

	for _, fr := range results.dsStoreURLFetchResultsCalls() {
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

	for link := range expectedMimesFound {
		t.Errorf("StoreURLFetchResults expected to find mime type for link %v, but didn't", link)
	}

	results.assertExpectations(t)
}

func TestBasicLinkTest(t *testing.T) {
	orig := Config.Fetcher.AcceptFormats
	defer func() {
		Config.Fetcher.AcceptFormats = orig
	}()
	Config.Fetcher.AcceptFormats = []string{"text/html", "text/plain"}

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

	tests := TestSpec{
		hasParsedLinks: true,
		hosts: singleLinkDomainSpecArr("http://linktests.com/links/test.html", &MockResponse{
			Body: html_test_links,
		}),
	}

	//
	// Run the fetcher
	//
	results := runFetcher(tests, t)

	//
	// Make sure expected results are there
	//
	for _, fr := range results.handlerCalls() {
		switch fr.URL.String() {
		case "http://linktests.com/links/test.html":
		default:
			t.Errorf("Got a Handler.HandleResponse call we didn't expect: %v", fr)
		}
	}

	expected := map[string]bool{
		"http://linktests.com/links/relative-dir/":           true,
		"http://linktests.com/links/relative-page/page.html": true,
		"http://linktests.com/abs-relative-dir/":             true,
		"http://linktests.com/abs-relative-page/page.html":   true,
		"https://other.org/abs-dir/":                         true,
		"https://other.org/abs-page/page.html":               true,
		"http:donot/ignore.html":                             true,
	}

	ulst, frlst := results.dsStoreParsedURLCalls()
	for i := range ulst {
		u := ulst[i]
		fr := frlst[i]
		if fr.URL.String() != "http://linktests.com/links/test.html" {
			t.Fatalf("Expected linktest source only")
		}
		if expected[u.String()] {
			delete(expected, u.String())
		} else {
			t.Errorf("StoreParsedURL call we didn't expect: %v", u)
		}
	}

	for link := range expected {
		t.Errorf("Expected to find %q in expected map, but didn't", link)
	}

	results.assertExpectations(t)
}

func TestStillCrawlWhenDomainUnreachable(t *testing.T) {
	orig := Config.Fetcher.BlacklistPrivateIPs
	defer func() { Config.Fetcher.BlacklistPrivateIPs = orig }()
	Config.Fetcher.BlacklistPrivateIPs = true

	tests := TestSpec{
		hasNoLinks: true,
		hosts: []DomainSpec{
			singleLinkDomainSpec("http://private.com/page1.html", nil),
			singleLinkDomainSpec("http://a1234567890bcde.com/page1.html", nil),
		},
	}

	results := runFetcher(tests, t)

	if len(results.handlerCalls()) != 0 || len(results.dsStoreURLFetchResultsCalls()) != 0 {
		t.Error("Did not expect any handler calls due to host resolving to private IP")
	}

	results.assertExpectations(t)
	results.datastore.AssertNotCalled(t, "LinksForHost", "private.com")
}

func TestcherCreatesTransport(t *testing.T) {
	orig := Config.Fetcher.BlacklistPrivateIPs
	defer func() { Config.Fetcher.BlacklistPrivateIPs = orig }()
	Config.Fetcher.BlacklistPrivateIPs = false

	tests := TestSpec{
		hasParsedLinks:    false,
		suppressTransport: true,
		hosts:             singleLinkDomainSpecArr("http://localhost.localdomain/", &MockResponse{Status: 404}),
	}

	results := runFetcher(tests, t)

	if results.manager.Transport == nil {
		t.Fatalf("Expected Transport to get set")
	}
	_, ok := results.manager.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Expected Transport to get set to a *http.Transport")
	}

	// It would be great to check that the DNS cache actually got used here,
	// but with the current design there seems to be no way to check it

	results.assertExpectations(t)
}

func TestRedirects(t *testing.T) {
	link := func(index int) string {
		return fmt.Sprintf("http://sub.dom.com/page%d.html", index)
	}

	roundTriper := mapRoundTrip{
		Responses: map[string]*http.Response{
			link(1): response307(link(2)),
			link(2): response307(link(3)),
			link(3): response200(),
		},
	}

	tests := TestSpec{
		hasParsedLinks: false,
		transport:      &roundTriper,
		hosts:          singleLinkDomainSpecArr(link(1), nil),
	}

	results := runFetcher(tests, t)

	frs := results.handlerCalls()
	if len(frs) < 1 {
		t.Fatalf("Expected to find calls made to handler, but didn't")
	}
	fr := frs[0]

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

	results.assertExpectations(t)

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

	tests := TestSpec{
		hasParsedLinks: true,
		hosts: singleLinkDomainSpecArr(testPage, &MockResponse{
			ContentType: "text/html",
			Body:        html_with_href_space,
		}),
	}

	results := runFetcher(tests, t)

	foundTCom := false
	for _, fr := range results.handlerCalls() {
		if fr.URL.String() == testPage {
			foundTCom = true
			break
		}
	}
	if !foundTCom {
		t.Fatalf("Failed to find pushed link %q", testPage)
	}

	expected := map[string]bool{
		"http://t.com/relative-dir/":               true,
		"http://t.com/relative-page/page.html":     true,
		"http://t.com/abs-relative-dir/":           true,
		"http://t.com/abs-relative-page/page.html": true,
		"https://other.org/abs-dir/":               true,
		"https://other.org/abs-page/page.html":     true,
	}

	ulst, frlst := results.dsStoreParsedURLCalls()
	for i := range ulst {
		u := ulst[i]
		fr := frlst[i]
		if fr.URL.String() == testPage {
			if expected[u.String()] {
				delete(expected, u.String())
			} else {
				t.Errorf("StoreParsedURL mismatch found unexpected link %q", u.String())
			}
		}
	}

	for link := range expected {
		t.Errorf("StoreParsedURL didn't find link %q", link)
	}

	results.assertExpectations(t)
}

func TestHTTPTimeout(t *testing.T) {
	origTimeout := Config.Fetcher.HTTPTimeout
	defer func() {
		Config.Fetcher.HTTPTimeout = origTimeout
	}()
	Config.Fetcher.HTTPTimeout = "200ms"
	for _, timeoutType := range []string{"wontConnect", "stalledRead"} {

		var transport *cancelTrackingTransport
		var closer io.Closer
		if timeoutType == "wontConnect" {
			transport, closer = getWontConnectTransport()
		} else {
			transport, closer = getStallingReadTransport()
		}

		tests := TestSpec{
			hasParsedLinks:     true,
			transport:          transport,
			suppressMockServer: true,
			hosts: []DomainSpec{
				singleLinkDomainSpec("http://t1.com/page1.html", nil),
				singleLinkDomainSpec("http://t2.com/page1.html", nil),
				singleLinkDomainSpec("http://t3.com/page1.html", nil),
			},
		}

		results := runFetcher(tests, t)
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

		if len(results.handlerCalls()) > 0 {
			t.Fatalf("For timeoutType %q Fetcher shouldn't have been able to connect, but did", timeoutType)
		}
	}
}

func TestMetaNos(t *testing.T) {
	origHonorNoindex := Config.Fetcher.HonorMetaNoindex
	origHonorNofollow := Config.Fetcher.HonorMetaNofollow
	defer func() {
		Config.Fetcher.HonorMetaNoindex = origHonorNoindex
		Config.Fetcher.HonorMetaNofollow = origHonorNofollow
	}()
	Config.Fetcher.HonorMetaNoindex = true
	Config.Fetcher.HonorMetaNofollow = true

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
<div id="menu">
	<a href="relative-dirX/">link</a>
	<a href="relative-pageX/page.html">link</a>
	<a href="/abs-relative-dirX/">link</a>
	<a href="/abs-relative-pageX/page.html">link</a>
	<a href="https://other.org/abs-dirX/">link</a>
	<a href="https://other.org/abs-pageX/page.html">link</a>
</div>
</html>`

	tests := TestSpec{
		hasParsedLinks: false,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "t1.com",
				links: []LinkSpec{
					LinkSpec{
						url: "http://t1.com/nofollow.html",
						response: &MockResponse{
							Body: nofollowHtml,
						},
					},
					LinkSpec{
						url: "http://t1.com/noindex.html",
						response: &MockResponse{
							Body: noindexHtml,
						},
					},

					LinkSpec{
						url: "http://t1.com/both.html",
						response: &MockResponse{
							Body: bothHtml,
						},
					},
				},
			},
		},
	}

	results := runFetcher(tests, t)

	// Did the fetcher honor noindex (if noindex is set
	// the handler shouldn't be called)
	callCount := 0
	for _, fr := range results.handlerCalls() {
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
	ulst, _ := results.dsStoreParsedURLCalls()
	callCount = len(ulst)
	if callCount != 0 {
		t.Errorf("Fetcher did not honor nofollow in meta: expected 0 callCount, found %d", callCount)
	}
}

func TestchManagerFastShutdown(t *testing.T) {
	tests := TestSpec{
		hasParsedLinks: false,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "test.com",
				links: []LinkSpec{
					LinkSpec{
						url: "http://test.com/robots.txt",
						response: &MockResponse{
							Body: "User-agent: *\nCrawl-delay: 1\n", // this is 120 seconds
						},
						robots: true,
					},
					LinkSpec{
						url:      "http://test.com/page1.html",
						response: &MockResponse{Status: 404},
					},
					LinkSpec{
						url:      "http://test.com/page2.html",
						response: &MockResponse{Status: 404},
					},
				},
			},
		},
	}

	results := runFetcher(tests, t) //compare duration here with Crawl-delay

	expectedCall := false
	for _, fr := range results.dsStoreURLFetchResultsCalls() {
		link := fr.URL.String()
		switch link {
		case "http://test.com/page1.html":
			expectedCall = true
		default:
			t.Errorf("Got unexpected StoreURLFetchResults call for %v", link)
		}
	}
	if !expectedCall {
		t.Errorf("Did not get expected StoreURLFetchResults call for http://test.com/page1.html")
	}

	results.assertExpectations(t)
}

func TestObjectEmbedIframeTags(t *testing.T) {
	origHonorNoindex := Config.Fetcher.HonorMetaNoindex
	origHonorNofollow := Config.Fetcher.HonorMetaNofollow
	defer func() {
		Config.Fetcher.HonorMetaNoindex = origHonorNoindex
		Config.Fetcher.HonorMetaNofollow = origHonorNofollow
	}()
	Config.Fetcher.HonorMetaNoindex = true
	Config.Fetcher.HonorMetaNofollow = true

	const html string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
<body>
	<object data="/object_data/page.html" />
	<iframe src="/iframe_src/page.html"> </iframe>
	<embed src="/embed_src/page.html" />
	<iframe srcdoc="<a href=/iframe_srcdoc/page.html > Link </a>" />
</body>
</html>`

	// The ifframe that looks like this
	//    <iframe srcdoc="<a href = \"/iframe_srcdoc/page.html\" > Link </a>" />
	// does not appear to be handled correctly by golang-html. The embedded quotes
	// are failing. But the version I have above does work (even though it's wonky)
	tests := TestSpec{
		hasParsedLinks: true,
		hosts:          singleLinkDomainSpecArr("http://t1.com/target.html", &MockResponse{Body: html}),
	}

	results := runFetcher(tests, t)

	expectedStores := map[string]bool{
		"http://t1.com/object_data/page.html":   true,
		"http://t1.com/iframe_srcdoc/page.html": true,
		"http://t1.com/iframe_src/page.html":    true,
		"http://t1.com/embed_src/page.html":     true,
	}

	ulst, _ := results.dsStoreParsedURLCalls()
	for _, u := range ulst {
		if expectedStores[u.String()] {
			delete(expectedStores, u.String())
		}
	}

	for link := range expectedStores {
		t.Errorf("Expected to encounter link %q, but didn't", link)
	}
}

func TestPathInclusion(t *testing.T) {
	origHonorNoindex := Config.Fetcher.ExcludeLinkPatterns
	origHonorNofollow := Config.Fetcher.IncludeLinkPatterns
	defer func() {
		Config.Fetcher.ExcludeLinkPatterns = origHonorNoindex
		Config.Fetcher.IncludeLinkPatterns = origHonorNofollow
	}()
	Config.Fetcher.ExcludeLinkPatterns = []string{`\.mov$`, "janky", `\/foo\/bang`, `^\/root$`}
	Config.Fetcher.IncludeLinkPatterns = []string{`\.keep$`}

	const html string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
<body>
	<div id="menu">
		<a href="/foo/bar.html">yes</a>
		<a href="/foo/bar.mov">no</a>
		<a href="/foo/mov.bar">yes</a>
		<a href="/janky/page.html">no</a>
		<a href="/foo/janky.html">no</a>
		<a href="/foo/bang/baz.html">no</a>
		<a href="/foo/bang/baz.keep">yes</a>
		<a href="/root">no</a>
		<a href="/root/more">yes</a>
	</div>
</body>
</html>`

	tests := TestSpec{
		hasParsedLinks: true,
		hosts:          singleLinkDomainSpecArr("http://t1.com/target.html", &MockResponse{Body: html}),
	}

	results := runFetcher(tests, t)

	expectedPaths := map[string]bool{
		"/foo/bar.html":      true,
		"/foo/mov.bar":       true,
		"/foo/bang/baz.keep": true,
		"/root/more":         true,
	}

	ulst, _ := results.dsStoreParsedURLCalls()
	for _, u := range ulst {
		if expectedPaths[u.RequestURI()] {
			delete(expectedPaths, u.RequestURI())
		} else {
			t.Errorf("Unexected call to StoreParsedURL for link %v", u)
		}
	}

	for path := range expectedPaths {
		t.Errorf("StoreParsedURL not called for %v, but should have been", path)
	}

}

func TestMaxCrawlDelay(t *testing.T) {
	// The approach to this test is simple. Set a very high Crawl-delay from
	// the host, and set a small MaxCrawlDelay in config. Then only allow the
	// fetcher to run long enough to get all the links IF the fetcher is honoring
	// the MaxCrawlDelay
	origDefaultCrawlDelay := Config.Fetcher.DefaultCrawlDelay
	origMaxCrawlDelay := Config.Fetcher.MaxCrawlDelay
	defer func() {
		Config.Fetcher.DefaultCrawlDelay = origDefaultCrawlDelay
		Config.Fetcher.MaxCrawlDelay = origMaxCrawlDelay
	}()
	Config.Fetcher.MaxCrawlDelay = "100ms" //compare this with the Crawl-delay below
	Config.Fetcher.DefaultCrawlDelay = "0s"

	tests := TestSpec{
		hasParsedLinks: true,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "a.com",
				links: []LinkSpec{
					LinkSpec{
						url: "http://a.com/robots.txt",
						response: &MockResponse{
							Body: "User-agent: *\nCrawl-delay: 120\n", // this is 120 seconds, compare to MaxCrawlDelay above
						},
						robots: true,
					},
					LinkSpec{
						url: "http://a.com/page1.html",
					},
					LinkSpec{
						url: "http://a.com/page2.html",
					},
					LinkSpec{
						url: "http://a.com/page3.html",
					},
				},
			},
		},
	}

	results := runFetcher(tests, t)

	expectedPages := map[string]bool{
		"/page1.html": true,
		"/page2.html": true,
		"/page3.html": true,
	}

	for _, fr := range results.dsStoreURLFetchResultsCalls() {
		domain, err := fr.URL.ToplevelDomainPlusOne()
		if err != nil {
			panic(err)
		}
		path := fr.URL.RequestURI()
		if domain != "a.com" {
			t.Fatalf("Domain mismatch -- this shouldn't happen")
		}
		if !expectedPages[path] {
			t.Errorf("Path mistmatch, didn't find path %q in expectedPages", path)
		} else {
			delete(expectedPages, path)
		}
	}

	for path := range expectedPages {
		t.Errorf("Didn't find expected page %q in mock data store", path)
	}

}

func TestFnvFingerprint(t *testing.T) {
	html := `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
<div>
	Roses are red, violets are blue, golang is the bomb, aint it so true!
</div>
</html>`
	tests := TestSpec{
		hasParsedLinks: true,
		hosts:          singleLinkDomainSpecArr("http://a.com/page1.html", &MockResponse{Body: html}),
	}

	results := runFetcher(tests, t)

	fnv := fnv.New64()
	fnv.Write([]byte(html))
	fp := int64(fnv.Sum64())

	expectedFps := map[string]int64{
		"/page1.html": fp,
	}

	for _, fr := range results.dsStoreURLFetchResultsCalls() {
		path := fr.URL.RequestURI()
		expFp, expFpOk := expectedFps[path]
		if !expFpOk {
			t.Errorf("Path mismatch, didn't find path %q in expectedFps", path)
			continue
		}

		if expFp != fr.FnvFingerprint {
			t.Errorf("Fingerprint mismatch, got %x, expected %x", fr.FnvFingerprint, expFp)
		}

		delete(expectedFps, path)
	}

	for path := range expectedFps {
		t.Errorf("Didn't find expected page %q in mock data store", path)
	}
}

func TestIfModifiedSince(t *testing.T) {
	link := "http://a.com/page1.html"
	lastCrawled := time.Now()
	tests := TestSpec{
		hasParsedLinks: true,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "a.com",
				links: []LinkSpec{
					LinkSpec{
						url:         "http://a.com/page1.html",
						response:    &MockResponse{Status: 304},
						lastCrawled: lastCrawled,
					},
				},
			},
		},
	}

	results := runFetcher(tests, t)

	//
	// Did the server see the header
	//
	headers, err := results.server.Headers("GET", link, -1)
	if err != nil {
		t.Fatalf("results.server.rs.Headers failed %v", err)
	}
	mod, modOk := headers["If-Modified-Since"]
	if !modOk {
		t.Fatalf("Failed to find If-Modified-Since in request header for link %q", link)
	} else if lm := lastCrawled.Format(time.RFC1123); lm != mod[0] {
		t.Errorf("If-Modified-Since has bad format, got %q, expected %q", mod[0], lm)
	}

	//
	// Did the data store get called correctly
	//
	count := 0
	for _, fr := range results.dsStoreURLFetchResultsCalls() {
		count++
		if fr.URL.String() != link {
			t.Errorf("DS URL link mismatch: got %q, expected %q", fr.URL.String(), link)
		}
		if fr.Response.StatusCode != 304 {
			t.Errorf("DS StatusCode mismatch: got %d, expected %d", fr.Response.StatusCode, 304)
		}
	}
	if count < 1 {
		t.Errorf("Expected to find DS call, but didn't")
	}

	//
	// Did the handler get called
	//
	count = 0
	for _, fr := range results.dsStoreURLFetchResultsCalls() {
		count++
		if fr.URL.String() != link {
			t.Errorf("Handler URL link mismatch: got %q, expected %q", fr.URL.String(), link)
		}
		if fr.Response.StatusCode != 304 {
			t.Errorf("Handler StatusCode mismatch: got %d, expected %d", fr.Response.StatusCode, 304)
		}
	}
	if count < 1 {
		t.Errorf("Expected to find Handler call, but didn't")
	}
}

func TestNestedRobots(t *testing.T) {
	tests := TestSpec{
		hasParsedLinks: true,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "a.com",
				links: []LinkSpec{
					LinkSpec{
						url: "http://dom.com/robots.txt",
						response: &MockResponse{
							Body: "User-agent: *\n",
						},
						robots: true,
					},

					LinkSpec{
						url:    "http://ok.dom.com/robots.txt",
						robots: true,
					},

					LinkSpec{
						url: "http://blocked.dom.com/robots.txt",
						response: &MockResponse{
							Body: "User-agent: *\nDisallow: /\n",
						},
						robots: true,
					},

					LinkSpec{
						url: "http://dom.com/page1.html",
					},

					LinkSpec{
						url: "http://ok.dom.com/page1.html",
					},

					LinkSpec{
						url: "http://blocked.dom.com/page1.html",
					},
				},
			},
		},
	}

	results := runFetcher(tests, t)

	//
	// Now check that the correct requests where made
	//
	testRes := []struct {
		link    string
		fetched bool
	}{
		{"http://notinvolved.com/page1.html", false},

		{"http://dom.com/robots.txt", true},
		{"http://ok.dom.com/robots.txt", true},
		{"http://blocked.dom.com/robots.txt", true},

		{"http://dom.com/page1.html", true},
		{"http://ok.dom.com/page1.html", true},
		{"http://blocked.dom.com/page1.html", false},
	}

	for _, tst := range testRes {
		req := results.server.Requested("GET", tst.link)
		if tst.fetched && !req {
			t.Errorf("Expected to have requested link %q, but didn't", tst.link)
		} else if !tst.fetched && req {
			t.Errorf("Expected NOT to have requested link %q, but did", tst.link)
		}
	}
}

func TestMaxContentSize(t *testing.T) {
	orig := Config.Fetcher.MaxHTTPContentSizeBytes
	defer func() {
		Config.Fetcher.MaxHTTPContentSizeBytes = orig
	}()
	Config.Fetcher.MaxHTTPContentSizeBytes = 10

	html := `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
<div>
	Roses are red, violets are blue, golang is the bomb, aint it so true!
</div>
</html>`

	tests := TestSpec{
		hasParsedLinks: true,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "a.com",
				links: []LinkSpec{
					LinkSpec{
						url: "http://a.com/page1.html",
						response: &MockResponse{
							Body: html,
						},
					},

					LinkSpec{
						url: "http://a.com/page2.html",
						response: &MockResponse{
							Body:          "0123456789 ",
							ContentType:   "text/html",
							ContentLength: 11,
						},
					},
				},
			},
		},
	}

	results := runFetcher(tests, t)

	hcalls := results.handlerCalls()
	if len(hcalls) != 0 {
		links := ""
		for _, fr := range hcalls {
			links += "\t"
			links += fr.URL.String()
			links += "\n"
		}
		t.Fatalf("Expected handler to be called 0 times, instead it was called %d times for links\n%s\n", len(hcalls), links)
	}

	page1Ok := false
	page2Ok := false
	for _, fr := range results.dsStoreURLFetchResultsCalls() {
		link := fr.URL.String()
		switch link {
		case "http://a.com/page1.html":
			page1Ok = true
		case "http://a.com/page2.html":
			page2Ok = true
		default:
			t.Errorf("Unexpected stored url %q", link)
		}
	}
	if !page1Ok {
		t.Errorf("Didn't find link http://a.com/page1.html in datastore calls, but expected too")
	}
	if !page2Ok {
		t.Errorf("Didn't find link http://a.com/page2.html in datastore calls, but expected too")
	}
}

func TestKeepAlive(t *testing.T) {
	orig := Config.Fetcher.ActiveFetchersTTL
	defer func() {
		Config.Fetcher.ActiveFetchersTTL = orig
	}()
	Config.Fetcher.ActiveFetchersTTL = "1s"

	tests := TestSpec{
		hosts: singleLinkDomainSpecArr("http://t1.com/page1.html", nil),
	}

	results := runFetcherTimed(tests, 3*time.Second, t)

	kacount := results.dsCountKeepAliveCalls()
	if kacount < 2 {
		t.Errorf("Expected two calls to keep alive, found only %d calls", kacount)
	}
}

func TestStoreBody(t *testing.T) {
	orig := Config.Cassandra.StoreResponseBody
	defer func() {
		Config.Cassandra.StoreResponseBody = orig
	}()
	Config.Cassandra.StoreResponseBody = true
	html := `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
<div>
	Roses are red, violets are blue, golang is the bomb, aint it so true!
</div>
</html>`

	tests := TestSpec{
		hasParsedLinks: true,
		hosts: singleLinkDomainSpecArr("http://a.com/page1.com", &MockResponse{
			Body: html,
		}),
	}

	//
	// Run the fetcher
	//
	results := runFetcher(tests, t)

	stores := results.dsStoreURLFetchResultsCalls()
	if len(stores) != 1 {
		t.Fatalf("Expected select for a.com to render a single result, instead got %d results", len(stores))
	}
	fr := stores[0]
	if fr.Body != html {
		t.Fatalf("Failed to match stored body: --expected--\n%q\n--got--:\n%q", html, fr.Body)
	}
}

func TestKeepAliveThreshold(t *testing.T) {
	origKeepAlive := Config.Fetcher.HTTPKeepAlive
	origThreshold := Config.Fetcher.HTTPKeepAliveThreshold
	origSimul := Config.Fetcher.NumSimultaneousFetchers
	defer func() {
		Config.Fetcher.HTTPKeepAlive = origKeepAlive
		Config.Fetcher.HTTPKeepAliveThreshold = origThreshold
		Config.Fetcher.NumSimultaneousFetchers = origSimul
	}()
	Config.Fetcher.HTTPKeepAlive = "threshold"
	Config.Fetcher.HTTPKeepAliveThreshold = "500ms"
	Config.Fetcher.NumSimultaneousFetchers = 1

	transport := getRecordingTransport("transport")
	transNoKeepAlive := getRecordingTransport("transNoKeepAlive")

	tests := TestSpec{
		hasParsedLinks:   false,
		transport:        transport,
		transNoKeepAlive: transNoKeepAlive,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "a.com",
				links: []LinkSpec{
					LinkSpec{
						url: "http://a.com/robots.txt",
						response: &MockResponse{
							Body: "User-agent: *\nCrawl-delay: 0\n",
						},
						robots: true,
					},

					LinkSpec{
						url: "http://a.com/page1.html",
					},

					LinkSpec{
						url: "http://a.com/page2.html",
					},
				},
			},

			DomainSpec{
				domain: "b.com",
				links: []LinkSpec{
					LinkSpec{
						url: "http://b.com/robots.txt",
						response: &MockResponse{
							Body: "User-agent: *\nCrawl-delay: 1\n",
						},
						robots: true,
					},

					LinkSpec{
						url: "http://b.com/page1.html",
					},
				},
			},
		},
	}

	runFetcher(tests, t)

	expectInTransport := map[string]bool{
		"http://a.com/page1.html": true,
		"http://a.com/page2.html": true,
	}

	expectInTransNoKeepAlive := map[string]bool{
		"http://a.com/robots.txt": true,
		"http://b.com/robots.txt": true,
		"http://b.com/page1.html": true,
	}

	for _, v := range transport.Record {
		if expectInTransport[v] {
			delete(expectInTransport, v)
		} else {
			t.Errorf("Unknown link found in transport: %v", v)
		}
	}
	for v := range expectInTransport {
		t.Errorf("Expected to find link %v in transport, but didn't", v)
	}

	for _, v := range transNoKeepAlive.Record {
		if expectInTransNoKeepAlive[v] {
			delete(expectInTransNoKeepAlive, v)
		} else {
			t.Errorf("Unknown link found in transNoKeepAlive: %v", v)
		}
	}
	for v := range expectInTransNoKeepAlive {
		t.Errorf("Expected to find link %v in transNoKeepAlive, but didn't", v)
	}
}

func TestMaxPathLength(t *testing.T) {
	orig := Config.Fetcher.MaxPathLength
	defer func() {
		Config.Fetcher.MaxPathLength = orig
	}()
	Config.Fetcher.MaxPathLength = 6

	const html string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>Title</title>
</head>
<body>
	<div id="menu">
		<a href="/01234">yes</a>
		<a href="/0123/4567">no</a>		
		<a href="/0?a=b">yes</a>
		<a href="/0?apple=orange">no</a>
	</div>
</body>
</html>`

	tests := TestSpec{
		hasParsedLinks: true,
		hosts:          singleLinkDomainSpecArr("http://t1.com/target.html", &MockResponse{Body: html}),
	}

	results := runFetcher(tests, t)

	expected := map[string]bool{
		"http://t1.com/01234": true,
		"http://t1.com/0?a=b": true,
	}

	ulst, _ := results.dsStoreParsedURLCalls()
	for i := range ulst {
		u := ulst[i]
		if expected[u.String()] {
			delete(expected, u.String())
		} else {
			t.Errorf("StoreParsedURL mismatch found unexpected link %q", u.String())
		}
	}

	for e := range expected {
		t.Errorf("StoreParsedURL expected to see %q, but didn't", e)
	}
}

func TestParseHttpEquiv(t *testing.T) {
	const html string = `<!DOCTYPE html>
<html>
<head>
<meta http-equiv="refresh" content="5; url=http://a.com/page1.html">
<title>Title</title>
</head>
<body>
Some text here.
</body>
</html>`

	tests := TestSpec{
		hasParsedLinks: true,
		hosts:          singleLinkDomainSpecArr("http://t1.com/target.html", &MockResponse{Body: html}),
	}

	results := runFetcher(tests, t)

	expected := map[string]bool{
		"http://a.com/page1.html": true,
	}

	ulst, _ := results.dsStoreParsedURLCalls()
	for i := range ulst {
		u := ulst[i]
		if expected[u.String()] {
			delete(expected, u.String())
		} else {
			t.Errorf("StoreParsedURL mismatch found unexpected link %q", u.String())
		}
	}

	for e := range expected {
		t.Errorf("StoreParsedURL expected to see %q, but didn't", e)
	}
}

func TestBugTrn210(t *testing.T) {
	tests := TestSpec{
		hasParsedLinks: false,
		hosts: []DomainSpec{
			DomainSpec{
				domain: "a.com",
				links: []LinkSpec{

					LinkSpec{
						url: "http://a.com/robots.txt",
						response: &MockResponse{
							Body: "User-agent: *\nDisallow: /\n",
						},
						robots: true,
					},

					LinkSpec{
						url: "http://a.com/",
					},

					LinkSpec{
						url: "http://a.com/page1.html",
					},
				},
			},
		},
	}

	results := runFetcher(tests, t)
	stores := results.dsStoreURLFetchResultsCalls()

	expected := map[string]bool{
		"http://a.com/":           true,
		"http://a.com/page1.html": true,
	}

	for _, fr := range stores {
		if expected[fr.URL.String()] {
			delete(expected, fr.URL.String())
		} else {
			t.Errorf("Unexpected link found: %v", fr.URL.String())
		}
		if fr.FetchTime != NotYetCrawled {
			t.Errorf("Bad FetchTime for %v", fr.URL.String())
		}
		if !fr.ExcludedByRobots {
			t.Errorf("Bad ExcludedByRobots for %v", fr.URL.String())
		}
	}
	for link := range expected {
		t.Errorf("Failed to find link %v", link)
	}
}
