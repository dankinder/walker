// +build cassandra

package cassandra

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"code.google.com/p/log4go"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/helpers"
)

func init() {
	helpers.LoadTestConfig("test-walker.yaml")

	// For tests it's useful to see more than the default INFO
	log4go.AddFilter("stdout", log4go.DEBUG, log4go.NewConsoleLogWriter())
}

// getDS is a convenience function for getting a cassandra datastore and failing
// if we couldn't
func getDS(t *testing.T) *Datastore {
	ds, err := NewDatastore()
	if err != nil {
		t.Fatalf("Failed to create Datastore: %v", err)
	}
	err = ds.db.Query("INSERT INTO walker_globals (key, val) VALUES (?, ?)", "max_priority", 10).Exec()
	if err != nil {
		t.Fatalf("Failed to insert max_priority: %v", err)
	}
	return ds
}

//TODO: test with query params

var page1URL *walker.URL
var page1Fetch *walker.FetchResults
var page2URL *walker.URL
var page2Fetch *walker.FetchResults

func init() {
	page1URL = helpers.Parse("http://test.com/page1.html")
	page1Fetch = &walker.FetchResults{
		URL:       page1URL,
		FetchTime: time.Now(),
		Response: &http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: 18,
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
	page2URL = helpers.Parse("http://test.com/page2.html")
	page2Fetch = &walker.FetchResults{
		URL:       page2URL,
		FetchTime: time.Now(),
		Response: &http.Response{
			Status:        "200 OK",
			StatusCode:    200,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			ContentLength: 18,
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

}

func TestDatastoreBasic(t *testing.T) {
	largePriority := 1000
	db := GetTestDB()
	ds := getDS(t)

	insertDomainInfo := `INSERT INTO domain_info (dom, claim_tok, priority, dispatched)
								VALUES (?, ?, ?, ?)`
	insertSegment := `INSERT INTO segments (dom, subdom, path, proto)
						VALUES (?, ?, ?, ?)`
	insertLink := `INSERT INTO links (dom, subdom, path, proto, time)
						VALUES (?, ?, ?, ?, ?)`

	queries := []*gocql.Query{
		db.Query(insertDomainInfo, "test.com", gocql.UUID{}, largePriority, true),
		db.Query(insertDomainInfo, "test2.com", gocql.UUID{}, largePriority, true),
		db.Query(insertSegment, "test.com", "", "page1.html", "http"),
		db.Query(insertSegment, "test.com", "", "page2.html", "http"),
		db.Query(insertLink, "test.com", "", "page1.html", "http", walker.NotYetCrawled),
		db.Query(insertLink, "test.com", "", "page2.html", "http", walker.NotYetCrawled),
	}
	for _, q := range queries {
		err := q.Exec()
		if err != nil {
			t.Fatalf("Failed to insert test data: %v\nQuery: %v", err, q)
		}
	}

	host := ds.ClaimNewHost()
	if host != "test2.com" {
		t.Errorf("Expected test2.com but got %q", host)
	}

	host = ds.ClaimNewHost()
	if host != "test.com" {
		t.Errorf("Expected test.com but got %q", host)
	}

	links := map[url.URL]bool{}
	expectedLinks := map[url.URL]bool{
		*page1URL.URL: true,
		*page2URL.URL: true,
	}
	for u := range ds.LinksForHost("test.com") {
		links[*u.URL] = true
	}
	if !reflect.DeepEqual(links, expectedLinks) {
		t.Errorf("Expected links from LinksForHost: %v\nBut got: %v", expectedLinks, links)
	}

	ds.StoreURLFetchResults(page1Fetch)
	ds.StoreURLFetchResults(page2Fetch)

	expectedResults := map[url.URL]int{
		*page1URL.URL: 200,
		*page2URL.URL: 200,
	}
	iter := db.Query(`SELECT dom, subdom, path, proto, time, stat
						FROM links WHERE dom = 'test.com'`).Iter()
	var linkdomain, subdomain, path, protocol string
	var status int
	var crawl_time time.Time
	results := map[url.URL]int{}
	for iter.Scan(&linkdomain, &subdomain, &path, &protocol, &crawl_time, &status) {
		if !crawl_time.Equal(walker.NotYetCrawled) {
			u, _ := walker.CreateURL(linkdomain, subdomain, path, protocol, crawl_time)
			results[*u.URL] = status
		}
	}
	if !reflect.DeepEqual(results, expectedResults) {
		t.Errorf("Expected results from StoreURLFetchResults: %v\nBut got: %v",
			expectedResults, results)
	}

	ds.StoreParsedURL(helpers.Parse("http://test2.com/page1-1.html"), page1Fetch)
	ds.StoreParsedURL(helpers.Parse("http://test2.com/page2-1.html"), page2Fetch)

	var count int
	db.Query(`SELECT COUNT(*) FROM links WHERE dom = 'test2.com'`).Scan(&count)
	if count != 2 {
		t.Errorf("Expected 2 parsed links to be inserted for test2.com, found %v", count)
	}

	ds.UnclaimHost("test.com")

	db.Query(`SELECT COUNT(*) FROM segments WHERE dom = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Errorf("Expected links from unclaimed domain to be deleted, found %v", count)
	}

	err := db.Query(`SELECT COUNT(*) FROM domain_info
						WHERE dom = 'test.com'
						AND claim_tok = 00000000-0000-0000-0000-000000000000
						AND dispatched = false ALLOW FILTERING`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query for test.com in domain_info: %v", err)
	}
	if count != 1 {
		t.Fatalf("test.com has incorrect values in domain_info after unclaim")
	}
}

func TestNewDomainAdditions(t *testing.T) {
	db := GetTestDB()
	ds := getDS(t)

	origAddNewDomains := walker.Config.Cassandra.AddNewDomains
	defer func() { walker.Config.Cassandra.AddNewDomains = origAddNewDomains }()

	walker.Config.Cassandra.AddNewDomains = false
	ds.StoreParsedURL(helpers.Parse("http://test.com/page1-1.html"), page1Fetch)

	var count int
	db.Query(`SELECT COUNT(*) FROM domain_info WHERE dom = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Error("Expected test.com not to be added to domain_info")
	}

	db.Query(`SELECT COUNT(*) FROM links WHERE dom = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Errorf("Expected parsed link not to be inserted for test.com, found %v", count)
	}

	walker.Config.Cassandra.AddNewDomains = true
	ds.StoreParsedURL(helpers.Parse("http://test.com/page1-1.html"), page1Fetch)

	err := db.Query(`SELECT COUNT(*) FROM domain_info
						WHERE dom = 'test.com'
						AND claim_tok = 00000000-0000-0000-0000-000000000000
						AND dispatched = false
						AND priority = 1 ALLOW FILTERING`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query for test.com in domain_info: %v", err)
	}
	if count != 1 {
		t.Fatalf("test.com not added to domain_info (possibly incorrect field values)")
	}

	db.Query(`DELETE FROM domain_info WHERE dom = 'test.com'`).Exec()
	ds.StoreParsedURL(helpers.Parse("http://test.com/page1-1.html"), page1Fetch)
	db.Query(`SELECT COUNT(*) FROM domain_info WHERE dom = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Error("Expected test.com not to be added to domain_info due to cache")
	}
}

type StoreURLExpectation struct {
	Input    *walker.FetchResults
	Expected *LinksExpectation
}

// The results we expect in the database for various fields. Non-primary
// keys are pointers so we can expect NULL for any of them
type LinksExpectation struct {
	Domain           string
	Subdomain        string
	Path             string
	Protocol         string
	CrawlTime        time.Time
	FetchError       string
	ExcludedByRobots bool
	Status           int
	MimeType         string
	FnvFingerprint   uint64
	Body             string
	Headers          map[string]string
}

var StoreURLExpectations []StoreURLExpectation

func init() {
	StoreURLExpectations = []StoreURLExpectation{
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       helpers.Parse("http://test.com/page1.html"),
				FetchTime: time.Unix(0, 0),
				Response: &http.Response{
					StatusCode: 200,
					Request: &http.Request{
						Host: "test.com",
					},
				},
				MimeType:       "text/html; charset=ISO-8859-4",
				FnvFingerprint: 1,
			},
			Expected: &LinksExpectation{
				Domain:         "test.com",
				Path:           "/page1.html",
				Protocol:       "http",
				CrawlTime:      time.Unix(0, 0),
				Status:         200,
				MimeType:       "text/html; charset=ISO-8859-4",
				FnvFingerprint: 1,
			},
		},
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       helpers.Parse("http://test.com/page2.html?var1=abc&var2=def"),
				FetchTime: time.Unix(0, 0),
				Response: &http.Response{
					StatusCode: 200,
				},
				MimeType:       "foo/bar",
				FnvFingerprint: 2,
			},
			Expected: &LinksExpectation{
				Domain:         "test.com",
				Path:           "/page2.html?var1=abc&var2=def",
				Protocol:       "http",
				CrawlTime:      time.Unix(0, 0),
				Status:         200,
				MimeType:       "foo/bar",
				FnvFingerprint: 2,
			},
		},
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:              helpers.Parse("http://test.com/page3.html"),
				ExcludedByRobots: true,
				FnvFingerprint:   3,
			},
			Expected: &LinksExpectation{
				Domain:           "test.com",
				Path:             "/page3.html",
				Protocol:         "http",
				CrawlTime:        time.Unix(0, 0),
				ExcludedByRobots: true,
				FnvFingerprint:   3,
			},
		},
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       helpers.Parse("http://test.com/page4.html"),
				FetchTime: time.Unix(1234, 5678),
				Response: &http.Response{
					StatusCode: 200,
				},
				FnvFingerprint: 4,
			},
			Expected: &LinksExpectation{
				Domain:         "test.com",
				Path:           "/page4.html",
				Protocol:       "http",
				CrawlTime:      time.Unix(1234, 5678),
				Status:         200,
				FnvFingerprint: 4,
			},
		},
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       helpers.Parse("https://test.com/page5.html"),
				FetchTime: time.Unix(0, 0),
				Response: &http.Response{
					StatusCode: 200,
				},
				FnvFingerprint: 5,
			},
			Expected: &LinksExpectation{
				Domain:         "test.com",
				Path:           "/page5.html",
				Protocol:       "https",
				CrawlTime:      time.Unix(0, 0),
				Status:         200,
				FnvFingerprint: 5,
			},
		},
		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       helpers.Parse("https://sub.dom1.test.com/page5.html"),
				FetchTime: time.Unix(0, 0),
				Response: &http.Response{
					StatusCode: 200,
				},
				FnvFingerprint: 6,
			},
			Expected: &LinksExpectation{
				Domain:         "test.com",
				Subdomain:      "sub.dom1",
				Path:           "/page5.html",
				Protocol:       "https",
				CrawlTime:      time.Unix(0, 0),
				Status:         200,
				FnvFingerprint: 6,
			},
		},

		StoreURLExpectation{
			Input: &walker.FetchResults{
				URL:       helpers.Parse("https://sub.dom1.test.com/page5.html"),
				FetchTime: time.Unix(0, 0),
				Response: &http.Response{
					StatusCode: 200,
					Header: http.Header{
						"foo": []string{"bar"},
						"baz": []string{"click", "clack"},
					},
				},
				FnvFingerprint: 6,
				Body:           "The Body of the HTTP pull",
			},
			Expected: &LinksExpectation{
				Domain:         "test.com",
				Subdomain:      "sub.dom1",
				Path:           "/page5.html",
				Protocol:       "https",
				CrawlTime:      time.Unix(0, 0),
				Status:         200,
				FnvFingerprint: 6,
				Body:           "The Body of the HTTP pull",
				Headers: map[string]string{
					"foo": "bar",
					"baz": "click\000clack",
				},
			},
		},
	}
}

func TestStoreURLFetchResults(t *testing.T) {
	origBody := walker.Config.Cassandra.StoreResponseBody
	origHeaders := walker.Config.Cassandra.StoreResponseHeaders
	defer func() {
		walker.Config.Cassandra.StoreResponseBody = origBody
		walker.Config.Cassandra.StoreResponseHeaders = origHeaders
	}()
	walker.Config.Cassandra.StoreResponseBody = true
	walker.Config.Cassandra.StoreResponseHeaders = true

	db := GetTestDB()
	ds := getDS(t)

	for _, tcase := range StoreURLExpectations {
		ds.StoreURLFetchResults(tcase.Input)
		exp := tcase.Expected

		actual := &LinksExpectation{}

		err := db.Query(
			`SELECT err, robot_ex, stat, mime, fnv, body, headers FROM links
			WHERE dom = ? AND subdom = ? AND path = ? AND proto = ?`, // AND time = ?`,
			exp.Domain,
			exp.Subdomain,
			exp.Path,
			exp.Protocol,
			//exp.CrawlTime,
		).Scan(&actual.FetchError, &actual.ExcludedByRobots, &actual.Status, &actual.MimeType, &actual.FnvFingerprint,
			&actual.Body, &actual.Headers)
		if err != nil {
			t.Errorf("Did not find row in links: %+v\nInput: %+v\nError: %v", exp, tcase.Input, err)
		}

		if exp.FetchError != actual.FetchError {
			t.Errorf("Expected err: %v\nBut got: %v\nFor input: %+v",
				exp.FetchError, actual.FetchError, tcase.Input)
		}
		if exp.ExcludedByRobots != actual.ExcludedByRobots {
			t.Errorf("Expected robot_ex: %v\nBut got: %v\nFor input: %+v",
				exp.ExcludedByRobots, actual.ExcludedByRobots, tcase.Input)
		}
		if exp.Status != actual.Status {
			t.Errorf("Expected stat: %v\nBut got: %v\nFor input: %+v",
				exp.Status, actual.Status, tcase.Input)
		}
		if exp.MimeType != actual.MimeType {
			t.Errorf("Expected mime: %v\nBut got: %v\nFor input: %+v",
				exp.MimeType, actual.MimeType, tcase.Input)
		}
		if exp.FnvFingerprint != actual.FnvFingerprint {
			t.Errorf("Expected FnvFingerprint: %v\nBut got: %v\nFor input: %+v",
				exp.FnvFingerprint, actual.FnvFingerprint, tcase.Input)
		}
		if exp.Body != actual.Body {
			t.Errorf("Expected Body: %v\nBut got: %v\nFor input: %+v",
				exp.Body, actual.Body, tcase.Input)
		}

		if exp.Headers != nil && actual.Headers == nil {
			t.Fatalf("Oops big trouble with actual.Headers")
		} else if exp.Headers == nil && actual.Headers != nil {
			t.Errorf("Headers mismatch: expected no headers, found some")
			continue
		}

		found := map[string]bool{}
		for k, e := range exp.Headers {
			g, gok := actual.Headers[k]
			if !gok {
				t.Errorf("Failed to find key %v in actual.Headers", k)
				continue
			}
			found[k] = true
			if g != e {
				t.Errorf("Headers mismatch: got %q, expected %q", g[0], e)
			}
		}
		if actual.Headers != nil {
			for k := range actual.Headers {
				if !found[k] {
					t.Errorf("Headers mismatch: actual had key %v, but expected did not", k)
				}
			}
		}
	}
}

func TestURLCreation(t *testing.T) {
	url1, err := url.Parse("http://sub1.test.com/thepath?query=blah")
	if err != nil {
		t.Fatal(err)
	}
	wurl1, err := walker.ParseURL("http://sub1.test.com/thepath?query=blah")
	if err != nil {
		t.Fatal(err)
	}
	if url1.String() != wurl1.String() {
		t.Errorf("URLs should be the same: %v\nAnd: %v")
	}

	created, err := walker.CreateURL("test.com", "sub1", "thepath?query=blah", "http",
		walker.NotYetCrawled)
	if err != nil {
		t.Fatal(err)
	}
	if created.String() != wurl1.String() {
		t.Errorf("Expected CreateURL to return %v\nBut got: %v", wurl1, created)
	}
}

var tldtests = []struct {
	URL                string
	ExpectedTLDPlusOne string
	ExpectedSubdomain  string
	ErrorExpected      bool
}{
	{"http://sub1.test.com/thepath?query=blah", "test.com", "sub1", false},
	{"http://foo", "", "", true},
}

func TestURLTLD(t *testing.T) {
	for _, dt := range tldtests {
		u, err := walker.ParseURL(dt.URL)
		if err != nil {
			if !dt.ErrorExpected {
				t.Errorf("Did not expect error parsing %v: %v", dt.URL, err)
			}
			continue
		}

		dom, err := u.ToplevelDomainPlusOne()
		if err != nil && !dt.ErrorExpected {
			t.Errorf("Did not expect error getting TLD+1: %v", err)
		}
		if dom != dt.ExpectedTLDPlusOne {
			t.Errorf("Expected ToplevelDomainPlusOne to be %v\nBut got: %v",
				dt.ExpectedTLDPlusOne, dom)
		}
		subdom, err := u.Subdomain()
		if err != nil && !dt.ErrorExpected {
			t.Errorf("Did not expect error getting subdomain: %v", err)
		}
		if subdom != dt.ExpectedSubdomain {
			t.Errorf("Expected Subdomain to be %v\nBut got: %v",
				dt.ExpectedSubdomain, subdom)
		}

		dom2, subdom2, err := u.TLDPlusOneAndSubdomain()
		if err != nil && !dt.ErrorExpected {
			t.Errorf("Did not expect error getting TLD+1 and subdomain: %v", err)
		}
		if dom2 != dt.ExpectedTLDPlusOne {
			t.Errorf("Expected TLDPlusOneAndSubdomain to give domain %v\nBut got: %v",
				dt.ExpectedTLDPlusOne, dom2)
		}
		if subdom2 != dt.ExpectedSubdomain {
			t.Errorf("Expected TLDPlusOneAndSubdomain to give subdomain %v\nBut got: %v",
				dt.ExpectedSubdomain, subdom2)
		}
	}
}

func TestAddingRedirects(t *testing.T) {
	db := GetTestDB()
	ds := getDS(t)

	link := func(index int) string {
		return fmt.Sprintf("http://subdom.dom.com/page%d.html", index)
	}

	fr := walker.FetchResults{
		URL:            helpers.Parse(link(1)),
		RedirectedFrom: []*walker.URL{helpers.Parse(link(2)), helpers.Parse(link(3))},
		FetchTime:      time.Unix(0, 0),
	}

	ds.StoreURLFetchResults(&fr)

	expected := []struct {
		link  string
		redto string
	}{
		{link: link(1), redto: link(2)},
		{link: link(2), redto: link(3)},
		{link: link(3), redto: ""},
	}

	for _, exp := range expected {
		url := helpers.Parse(exp.link)

		dom, subdom, _ := url.TLDPlusOneAndSubdomain()
		itr := db.Query("SELECT redto_url FROM links WHERE dom = ? AND subdom = ? AND path = ? AND proto = ?",
			dom,
			subdom,
			url.RequestURI(),
			url.Scheme).Iter()
		var redto string
		if !itr.Scan(&redto) {
			t.Errorf("Failed to find link %q", exp.link)
			continue
		}

		err := itr.Close()
		if err != nil {
			t.Errorf("Iterator returned error %v", err)
			continue
		}

		if redto != exp.redto {
			t.Errorf("Redirect mismatch: got %q, expected %q", redto, exp.redto)
		}
	}
}

func TestUnclaimAll(t *testing.T) {
	db := GetTestDB()
	ds := getDS(t)

	insertDomainInfo := `INSERT INTO domain_info (dom, claim_tok, priority, dispatched)
								VALUES (?, ?, ?, ?)`
	insertSegment := `INSERT INTO segments (dom, subdom, path, proto)
						VALUES (?, ?, ?, ?)`

	queries := []*gocql.Query{
		db.Query(insertDomainInfo, "test.com", gocql.TimeUUID(), 1, true),
		db.Query(insertSegment, "test.com", "", "page1.html", "http"),
		db.Query(insertSegment, "test.com", "", "page2.html", "http"),
	}
	for _, q := range queries {
		err := q.Exec()
		if err != nil {
			t.Fatalf("Failed to insert test data: %v\nQuery: %v", err, q)
		}
	}

	ds.UnclaimAll()

	var count int
	db.Query(`SELECT COUNT(*) FROM segments WHERE dom = 'test.com'`).Scan(&count)
	if count != 0 {
		t.Errorf("Expected links from unclaimed domain to be deleted, found %v", count)
	}

	err := db.Query(`SELECT COUNT(*) FROM domain_info
						WHERE dom = 'test.com'
						AND claim_tok = 00000000-0000-0000-0000-000000000000
						AND dispatched = false ALLOW FILTERING`).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query for test.com in domain_info: %v", err)
	}
	if count != 1 {
		t.Fatalf("test.com has not been correctly undispatched in domain_info after unclaim all")
	}
}

func TestClaimHostConcurrency(t *testing.T) {
	largePriority := 10
	numInstances := 10
	numDomain := 1000

	db := GetTestDB()
	insertDomainInfo := `INSERT INTO domain_info (dom, claim_tok, dispatched, priority) VALUES (?, 00000000-0000-0000-0000-000000000000, true, ?)`
	for i := 0; i < numDomain; i++ {
		err := db.Query(insertDomainInfo, fmt.Sprintf("d%d.com", i), largePriority).Exec()
		if err != nil {
			t.Fatalf("Failed to insert domain d%d.com: %v", i, err)
		}
	}
	db.Close()

	var startWg, finishWg sync.WaitGroup
	var hosts [][]string = make([][]string, numInstances)
	for i := 0; i < numInstances; i++ {
		finishWg.Add(1)
		startWg.Add(1)
		go func(index int) {
			ds := getDS(t)
			startWg.Done()
			startWg.Wait()
			var h []string
			for {
				host := ds.ClaimNewHost()
				if host == "" {
					break
				}
				h = append(h, host)
			}
			hosts[index] = h
			ds.Close()
			finishWg.Done()
		}(i)
	}
	finishWg.Wait()

	allDomains := map[string]bool{}
	for _, hlist := range hosts {
		for _, host := range hlist {
			if allDomains[host] {
				t.Fatalf("Double counted domain %s", host)
			}
			allDomains[host] = true
		}
	}
	for i := 0; i < numDomain; i++ {
		host := fmt.Sprintf("d%d.com", i)
		if !allDomains[host] {
			t.Fatalf("Failed to claim domain %s", host)
		}
	}
}

func TestDomainPriority(t *testing.T) {
	// This is a simple priority test. It's set up so all of the yes.com domains should be served by ClaimNewHost,
	// and all the NO.com's should NOT be served.
	rand.Seed(12345)

	// I can't pull in cassandra.MaxPriority because it becomes cyclic dependency.
	maxPriority := 10
	numElements := 50

	db := GetTestDB()
	ds := getDS(t)

	insertDomainInfo := `INSERT INTO domain_info (dom, priority, claim_tok, dispatched) VALUES (?, ?, 00000000-0000-0000-0000-000000000000, true)`
	count := 0
	for i := 0; i < numElements; i++ {
		if rand.Float32() < 0.5 {
			count++
			host := fmt.Sprintf("yes%d.com", i)
			err := db.Query(insertDomainInfo, host, maxPriority).Exec()
			if err != nil {
				t.Fatalf("Failed to insert domain yes.com: %v", err)
			}
		} else {
			host := fmt.Sprintf("NO%d.com", i)
			err := db.Query(insertDomainInfo, host, 0).Exec() // This isn't legal in normal use, but it'll be fine here
			if err != nil {
				t.Fatalf("Failed to insert domain NO.com: %v", err)
			}
		}
	}

	ncount := 0
	for {
		host := ds.ClaimNewHost()
		if host == "" {
			break
		}
		ncount++
		if !strings.HasPrefix(host, "yes") {
			t.Errorf("Bad domain found: %q", host)
		}
	}

	if ncount != count {
		t.Fatalf("Count mismatch: got %d, expected %d", ncount, count)
	}
}

func TestDomainPriorityRatio(t *testing.T) {
	// This test checks the ratio of fetched domains. The deal is we have two domains, d1 and d2, with priorities, p1
	// and p2. The number of fetches of d1 (d2) is f1 (f2): that is the number of times d1 is claimed (by ClaimNewHost)
	// is f1. In this case if the priority system is working correctly it should be that p1/p2 is approximately equal
	// to f1/f2
	rand.Seed(12345)
	db := GetTestDB()
	ds := getDS(t)
	maxPriority := ds.MaxPriority()

	type Dom struct {
		name     string
		priority int
	}
	doms := []Dom{
		Dom{"dom1.com", maxPriority / 2},
		Dom{"dom2.com", maxPriority},
	}

	insertDomainInfo := `INSERT INTO domain_info (dom, priority, claim_tok, dispatched) VALUES (?, ?, 00000000-0000-0000-0000-000000000000, true)`
	for _, d := range doms {
		err := db.Query(insertDomainInfo, d.name, d.priority).Exec()
		if err != nil {
			t.Fatalf("Failed to insert domain yes.com: %v", err)
		}
	}

	numRuns := 1000
	hosts := 0
	got := map[string]int{}
	for i := 0; i < numRuns; i++ {
		host := ds.ClaimNewHost()
		if host == "" {
			continue
		}
		hosts++
		cnt, cntOk := got[host]
		if !cntOk {
			cnt = 0
		}
		got[host] = cnt + 1

		ds.UnclaimHost(host)
		update := `UPDATE domain_info 
				   SET 
				   	claim_tok = 00000000-0000-0000-0000-000000000000, 
				   	dispatched = true
				   WHERE 
				   	dom = ?`

		err := db.Query(update, host).Exec()
		if err != nil {
			t.Fatalf("Failed to update domain_info: %v", err)
		}
	}

	prio1 := doms[0].priority
	prio2 := doms[1].priority
	expRatio := float64(prio1) / float64(prio2)

	count1, count1Ok := got[doms[0].name]
	if !count1Ok {
		t.Fatalf("Failed to find domain %q", doms[0].name)
	} else {
		delete(got, doms[0].name)
	}

	count2, count2Ok := got[doms[1].name]
	if !count2Ok {
		t.Fatalf("Failed to find domain %q", doms[1].name)
	} else {
		delete(got, doms[1].name)
	}

	gotRatio := float64(count1) / float64(count2)

	tol := 0.01
	if gotRatio < expRatio-tol || gotRatio > expRatio+tol {
		t.Fatalf("Bad ratio: expected %f, but got %f: for a difference of %f using tol %f", expRatio, gotRatio,
			expRatio-gotRatio, tol)
	}

	for k := range got {
		t.Errorf("Unexpected domain %q found", k)
	}
}

func TestKeepAlive(t *testing.T) {
	orig := walker.Config.Fetcher.ActiveFetchersTTL
	defer func() {
		walker.Config.Fetcher.ActiveFetchersTTL = orig
	}()
	walker.Config.Fetcher.ActiveFetchersTTL = "1s"

	db := GetTestDB()
	ds := getDS(t)
	read := func() (int, gocql.UUID) {
		itr := db.Query(`SELECT tok FROM active_fetchers`).Iter()
		var tok gocql.UUID
		count := 0
		for itr.Scan(&tok) {
			count++
		}
		err := itr.Close()
		if err != nil {
			panic(err)
		}
		return count, tok
	}

	keepAlive := func() {
		err := ds.KeepAlive()
		if err != nil {
			t.Fatalf("Failed KeepAlive: %v", err)
		}
	}

	count, tok := read()
	if count != 0 {
		t.Fatalf("Expected active_fetchers to be empty, but it wasn't")
	}
	keepAlive()
	count, tok = read()
	if count != 1 {
		t.Fatalf("Failed to add tok to active_fetchers correctly, count was %d", count)
	}

	keepAlive()
	count, tok2 := read()
	if count != 1 {
		t.Fatalf("Failed to reread count")
	}
	if tok != tok2 {
		t.Fatalf("Failed to reread token")
	}

	keepAlive()
	time.Sleep(1500 * time.Millisecond)
	count, _ = read()
	if count != 0 {
		t.Fatalf("Failed to expire from active_fetchers")
	}
}

func TestUpdateDomain(t *testing.T) {

	// Check variables
	excluded := false
	excludeReason := ""
	priority := 1

	// Other variables
	domain := "foo.com"
	insertDomainInfo := `INSERT INTO domain_info (dom, excluded, exclude_reason, priority)
						     VALUES (?, ?, ?, ?)`
	ds := getDS(t)

	// the check function will verify that the DomainInfo found in the database for foo.com, has the expected values
	// (see the Check variables above)
	check := func(tag string) {
		dinfo, err := ds.FindDomain(domain)
		if dinfo == nil || err != nil {
			t.Fatalf("Failed to FindDomain for tag %s: %v", tag, err)
		}

		var buffer string

		if dinfo.Excluded != excluded {
			buffer += fmt.Sprintf("\tExcluded mismatch: got %v, expected %v\n", dinfo.Excluded, excluded)
		}

		if dinfo.ExcludeReason != excludeReason {
			buffer += fmt.Sprintf("\tExcludeReason mismatch: got %q, expected %q\n", dinfo.ExcludeReason, excludeReason)
		}

		if dinfo.Priority != priority {
			buffer += fmt.Sprintf("\tPriority mismatch: got %d, expected %d\n", dinfo.Priority, priority)
		}

		if buffer != "" {
			t.Errorf("Domain info had unexpected problems for tag %s:\n%s", tag, buffer)
		}
	}

	db := GetTestDB()
	err := db.Query(insertDomainInfo, domain, excluded, excludeReason, priority).Exec()
	if err != nil {
		t.Fatalf("Unexpected error during domain insert: %v", err)
	}
	check("First Insert")

	excluded = true
	excludeReason = "Excluded reason"
	ds.UpdateDomain(domain, &DomainInfo{Excluded: excluded, ExcludeReason: excludeReason},
		DomainInfoUpdateConfig{Exclude: true})
	check("Exclude updated")

	excluded = false
	excludeReason = ""
	ds.UpdateDomain(domain, &DomainInfo{Excluded: excluded, ExcludeReason: excludeReason},
		DomainInfoUpdateConfig{Exclude: true})
	check("clear 1")

	priority = 4
	ds.UpdateDomain(domain, &DomainInfo{Priority: priority}, DomainInfoUpdateConfig{Priority: true})
	check("Priority")

	priority = 1
	ds.UpdateDomain(domain, &DomainInfo{Priority: priority}, DomainInfoUpdateConfig{Priority: true})
	check("clear 2")

	excluded = true
	excludeReason = "Excluded reason"
	priority = 4
	ds.UpdateDomain(domain, &DomainInfo{
		Excluded:      excluded,
		ExcludeReason: excludeReason,
		Priority:      priority,
	}, DomainInfoUpdateConfig{Exclude: true, Priority: true})
	check("Priority & Exclude")

}
