// +build cassandra

package cassandra

import (
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/helpers"
)

type DispatcherTest struct {
	Tag                  string
	ExistingDomainInfos  []ExistingDomainInfo
	ExistingLinks        []ExistingLink
	ExpectedSegmentLinks []walker.URL

	// Use to indicate that we do not expect a domain to end up dispatched.
	// Generally left out, we do usually expect a dispatch to happen
	NoDispatchExpected bool
}

type ExistingDomainInfo struct {
	Dom        string
	ClaimTok   gocql.UUID
	Priority   int
	Dispatched bool
	Excluded   bool
}

type ExistingLink struct {
	URL    walker.URL
	Status int // -1 indicates this is a parsed link, not yet fetched
	GetNow bool
}

var DispatcherTests = []DispatcherTest{
	DispatcherTest{
		Tag: "BasicTest",

		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},

		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},

		ExpectedSegmentLinks: []walker.URL{
			{URL: helpers.UrlParse("http://test.com/"),
				LastCrawled: walker.NotYetCrawled},
		},
	},

	DispatcherTest{
		Tag: "NothingToDispatch",

		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},
		ExistingLinks:        []ExistingLink{},
		ExpectedSegmentLinks: []walker.URL{},
		NoDispatchExpected:   true,
	},

	// This test is complicated, so I describe it in this comment. Below you'll
	// see we set
	//   Config.Dispatcher.MaxLinksPerSegment = 9
	//   Config.Dispatcher.RefreshPercentage = 33
	//
	// Below you see 3 GetNow links which will for sure be in segments.  That
	// means there are 6 additional links to push to segments. Of those 33%
	// should be refresh links: or 2 ( = 6 * 0.33) already crawled links. And
	// 4 (= 6-2) links should be not-yet-crawled links. And that is the
	// composition of the first tests expected.
	DispatcherTest{
		Tag: "MultipleLinksTest",

		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},

		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page2.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page404.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page500.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},

			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled2.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled3.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled4.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled5.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},

			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page1.html"),
				LastCrawled: time.Now().AddDate(0, 0, -4)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page2.html"),
				LastCrawled: time.Now().AddDate(0, 0, -3)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page404.html"),
				LastCrawled: time.Now().AddDate(0, 0, -2)}, Status: http.StatusNotFound},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page500.html"),
				LastCrawled: time.Now().AddDate(0, 0, -1)}, Status: http.StatusInternalServerError},

			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/getnow1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1, GetNow: true},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/getnow2.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1, GetNow: true},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/getnow3.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1, GetNow: true},
		},

		ExpectedSegmentLinks: []walker.URL{
			// The two oldest already crawled links
			{URL: helpers.UrlParse("http://test.com/page1.html"),
				LastCrawled: time.Now().AddDate(0, 0, -4)},
			{URL: helpers.UrlParse("http://test.com/page2.html"),
				LastCrawled: time.Now().AddDate(0, 0, -3)},

			// 4 uncrawled links
			{URL: helpers.UrlParse("http://test.com/notcrawled1.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled2.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled3.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled4.html"),
				LastCrawled: walker.NotYetCrawled},

			// all of the getnow links
			{URL: helpers.UrlParse("http://test.com/getnow1.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/getnow2.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/getnow3.html"),
				LastCrawled: walker.NotYetCrawled},
		},
	},

	// Similar to above test, but now there are no getnows, so you
	// should have 6 not-yet-crawled, and 3 already crawled
	DispatcherTest{
		Tag: "AllCrawledCorrectOrder",

		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},

		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/l.html"),
				LastCrawled: time.Now().AddDate(0, -2, -4)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/m.html"),
				LastCrawled: time.Now().AddDate(0, -3, -1)}, Status: http.StatusOK},

			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/a.html"),
				LastCrawled: time.Now().AddDate(0, 0, -1)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/b.html"),
				LastCrawled: time.Now().AddDate(0, 0, -2)}, Status: http.StatusOK},

			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/c.html"),
				LastCrawled: time.Now().AddDate(0, 0, -3)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/d.html"),
				LastCrawled: time.Now().AddDate(0, 0, -4)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/e.html"),
				LastCrawled: time.Now().AddDate(0, -1, -1)}, Status: http.StatusOK},

			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/f.html"),
				LastCrawled: time.Now().AddDate(0, -1, -2)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/g.html"),
				LastCrawled: time.Now().AddDate(0, -1, -3)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/h.html"),
				LastCrawled: time.Now().AddDate(0, -1, -4)}, Status: http.StatusOK},

			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/i.html"),
				LastCrawled: time.Now().AddDate(0, -2, -1)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/j.html"),
				LastCrawled: time.Now().AddDate(0, -2, -2)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/k.html"),
				LastCrawled: time.Now().AddDate(0, -2, -3)}, Status: http.StatusOK},

			// These two links cover up the previous two l and m.html links.
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/l.html"),
				LastCrawled: time.Now()}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/m.html"),
				LastCrawled: time.Now()}, Status: http.StatusOK},
		},

		ExpectedSegmentLinks: []walker.URL{
			// 9 Oldest links
			{URL: helpers.UrlParse("http://test.com/k.html"),
				LastCrawled: time.Now().AddDate(0, -2, -3)},
			{URL: helpers.UrlParse("http://test.com/j.html"),
				LastCrawled: time.Now().AddDate(0, -2, -2)},
			{URL: helpers.UrlParse("http://test.com/i.html"),
				LastCrawled: time.Now().AddDate(0, -2, -1)},

			{URL: helpers.UrlParse("http://test.com/h.html"),
				LastCrawled: time.Now().AddDate(0, -1, -4)},
			{URL: helpers.UrlParse("http://test.com/g.html"),
				LastCrawled: time.Now().AddDate(0, -1, -3)},
			{URL: helpers.UrlParse("http://test.com/f.html"),
				LastCrawled: time.Now().AddDate(0, -1, -2)},

			{URL: helpers.UrlParse("http://test.com/e.html"),
				LastCrawled: time.Now().AddDate(0, -1, -1)},
			{URL: helpers.UrlParse("http://test.com/d.html"),
				LastCrawled: time.Now().AddDate(0, 0, -4)},
			{URL: helpers.UrlParse("http://test.com/c.html"),
				LastCrawled: time.Now().AddDate(0, 0, -3)},
		},
	},

	DispatcherTest{
		Tag: "NoGetNow",

		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},

		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page2.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page404.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page500.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},

			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled2.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled3.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled4.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled5.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled6.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled7.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled8.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled9.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},

			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page1.html"),
				LastCrawled: time.Now().AddDate(0, 0, -4)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page2.html"),
				LastCrawled: time.Now().AddDate(0, 0, -3)}, Status: http.StatusOK},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page404.html"),
				LastCrawled: time.Now().AddDate(0, 0, -2)}, Status: http.StatusNotFound},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page500.html"),
				LastCrawled: time.Now().AddDate(0, 0, -1)}, Status: http.StatusInternalServerError},
		},

		ExpectedSegmentLinks: []walker.URL{
			// 3 crawled links
			{URL: helpers.UrlParse("http://test.com/page1.html"),
				LastCrawled: time.Now().AddDate(0, 0, -4)},
			{URL: helpers.UrlParse("http://test.com/page2.html"),
				LastCrawled: time.Now().AddDate(0, 0, -3)},
			{URL: helpers.UrlParse("http://test.com/page404.html"),
				LastCrawled: time.Now().AddDate(0, 0, -2)},

			// 6 uncrawled links
			{URL: helpers.UrlParse("http://test.com/notcrawled1.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled2.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled3.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled4.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled5.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled6.html"),
				LastCrawled: walker.NotYetCrawled},
		},
	},

	DispatcherTest{
		Tag: "OnlyUncrawled",

		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},

		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled2.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled3.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled4.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled5.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled6.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled7.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled8.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/notcrawled9.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},

		ExpectedSegmentLinks: []walker.URL{
			{URL: helpers.UrlParse("http://test.com/notcrawled1.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled2.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled3.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled4.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled5.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled6.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled7.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled8.html"),
				LastCrawled: walker.NotYetCrawled},
			{URL: helpers.UrlParse("http://test.com/notcrawled9.html"),
				LastCrawled: walker.NotYetCrawled},
		},
	},

	DispatcherTest{ // Verifies that we work with query parameters properly
		Tag: "QueryParmsOK",
		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},
		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page1.html?p=v"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},
		ExpectedSegmentLinks: []walker.URL{
			{URL: helpers.UrlParse("http://test.com/page1.html?p=v"),
				LastCrawled: walker.NotYetCrawled},
		},
	},

	DispatcherTest{ // Verifies that we don't generate an already-dispatched domain
		Tag: "NoAlreadyDispatched",
		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com", Dispatched: true},
		},
		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},
		ExpectedSegmentLinks: []walker.URL{},
	},

	DispatcherTest{
		Tag: "ShouldBeExcluded",
		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com", Excluded: true},
		},
		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page1.html"),
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},
		ExpectedSegmentLinks: []walker.URL{},
		NoDispatchExpected:   true,
	},
}

func TestDispatcherBasic(t *testing.T) {
	// These config settings MUST be here. The results of the test
	// change if these are changed.
	origMaxLinksPerSegment := walker.Config.Dispatcher.MaxLinksPerSegment
	origRefreshPercentage := walker.Config.Dispatcher.RefreshPercentage
	defer func() {
		walker.Config.Dispatcher.MaxLinksPerSegment = origMaxLinksPerSegment
		walker.Config.Dispatcher.RefreshPercentage = origRefreshPercentage
	}()
	walker.Config.Dispatcher.MaxLinksPerSegment = 9
	walker.Config.Dispatcher.RefreshPercentage = 33

	var q *gocql.Query

	for _, dt := range DispatcherTests {
		db := GetTestDB() // runs between tests to reset the db

		for _, edi := range dt.ExistingDomainInfos {
			q = db.Query(`INSERT INTO domain_info (dom, claim_tok, priority, dispatched, excluded)
							VALUES (?, ?, ?, ?, ?)`,
				edi.Dom, edi.ClaimTok, edi.Priority, edi.Dispatched, edi.Excluded)
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test domain info: %v\nQuery: %v", err, q)
			}
		}

		for _, el := range dt.ExistingLinks {
			dom, subdom, _ := el.URL.TLDPlusOneAndSubdomain()
			if el.Status == -1 {
				q = db.Query(`INSERT INTO links (dom, subdom, path, proto, time, getnow)
								VALUES (?, ?, ?, ?, ?, ?)`,
					dom,
					subdom,
					el.URL.RequestURI(),
					el.URL.Scheme,
					el.URL.LastCrawled,
					el.GetNow)
			} else {
				q = db.Query(`INSERT INTO links (dom, subdom, path, proto, time, stat, getnow)
								VALUES (?, ?, ?, ?, ?, ?, ?)`,
					dom,
					subdom,
					el.URL.RequestURI(),
					el.URL.Scheme,
					el.URL.LastCrawled,
					el.Status,
					el.GetNow)
			}
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test links: %v\nQuery: %v", err, q)
			}
		}

		d := &Dispatcher{}
		go d.StartDispatcher()
		time.Sleep(time.Millisecond * 100)
		d.StopDispatcher()

		expectedResults := map[url.URL]bool{}
		for _, esl := range dt.ExpectedSegmentLinks {
			expectedResults[*esl.URL] = true
		}

		results := map[url.URL]bool{}
		iter := db.Query(`SELECT dom, subdom, path, proto
							FROM segments WHERE dom = 'test.com'`).Iter()
		var linkdomain, subdomain, path, protocol string
		for iter.Scan(&linkdomain, &subdomain, &path, &protocol) {
			u, _ := walker.CreateURL(linkdomain, subdomain, path, protocol, walker.NotYetCrawled)
			results[*u.URL] = true
		}
		if !reflect.DeepEqual(results, expectedResults) {
			t.Errorf("For tag %q expected results in segments: %v\nBut got: %v",
				dt.Tag, expectedResults, results)
		}

		for _, edi := range dt.ExistingDomainInfos {
			q = db.Query(`SELECT dispatched FROM domain_info WHERE dom = ?`, edi.Dom)
			var dispatched bool
			if err := q.Scan(&dispatched); err != nil {
				t.Fatalf("For tag %q failed to insert find domain info: %v\nQuery: %v", dt.Tag, err, q)
			}
			if dt.NoDispatchExpected {
				if dispatched {
					t.Errorf("For tag %q `dispatched` flag got set on domain: %v", dt.Tag, edi.Dom)
				}
			} else if !dispatched {
				t.Errorf("For tag %q `dispatched` flag not set on domain: %v", dt.Tag, edi.Dom)
			}
		}
	}
}

func TestDispatcherDispatchedFalseIfNoLinks(t *testing.T) {
	db := GetTestDB()
	q := db.Query(`INSERT INTO domain_info (dom, claim_tok, priority, dispatched)
					VALUES (?, ?, ?, ?)`, "test.com", gocql.UUID{}, 0, false)
	if err := q.Exec(); err != nil {
		t.Fatalf("Failed to insert test domain info: %v\nQuery: %v", err, q)
	}

	d := &Dispatcher{}
	go d.StartDispatcher()
	// Pete says this time used to be 10 millis, but I was observing spurious nil channel
	// panics. Increased it to 100 to see if that would help.
	time.Sleep(time.Millisecond * 100)
	d.StopDispatcher()

	q = db.Query(`SELECT dispatched FROM domain_info WHERE dom = ?`, "test.com")
	var dispatched bool
	if err := q.Scan(&dispatched); err != nil {
		t.Fatalf("Failed to find domain info: %v\nQuery: %v", err, q)
	}
	if dispatched {
		t.Errorf("`dispatched` flag set to true when no links existed")
	}
}

func TestMinLinkRefreshTime(t *testing.T) {
	origMinLinkRefreshTime := walker.Config.Dispatcher.MinLinkRefreshTime
	defer func() {
		walker.Config.Dispatcher.MinLinkRefreshTime = origMinLinkRefreshTime
	}()
	walker.Config.Dispatcher.MinLinkRefreshTime = "49h"

	var now = time.Now()
	var tests = []DispatcherTest{
		DispatcherTest{
			Tag: "BasicTest",

			ExistingDomainInfos: []ExistingDomainInfo{
				{Dom: "test.com"},
			},

			ExistingLinks: []ExistingLink{
				{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page1.html"),
					LastCrawled: now.AddDate(0, 0, -1)}, Status: -1},
				{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page2.html"),
					LastCrawled: now.AddDate(0, 0, -2)}, Status: -1},
				{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page3.html"),
					LastCrawled: now.AddDate(0, 0, -3)}, Status: -1},
				{URL: walker.URL{URL: helpers.UrlParse("http://test.com/page4.html"),
					LastCrawled: now.AddDate(0, 0, -4)}, Status: -1},
			},

			ExpectedSegmentLinks: []walker.URL{
				{URL: helpers.UrlParse("http://test.com/page3.html"),
					LastCrawled: now.AddDate(0, 0, -3)},
				{URL: helpers.UrlParse("http://test.com/page4.html"),
					LastCrawled: now.AddDate(0, 0, -4)},
			},
		},
	}

	var q *gocql.Query
	for _, dt := range tests {
		db := GetTestDB() // runs between tests to reset the db

		for _, edi := range dt.ExistingDomainInfos {
			q = db.Query(`INSERT INTO domain_info (dom, claim_tok, priority, dispatched, excluded)
							VALUES (?, ?, ?, ?, ?)`,
				edi.Dom, edi.ClaimTok, edi.Priority, edi.Dispatched, edi.Excluded)
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test domain info: %v\nQuery: %v", err, q)
			}
		}

		for _, el := range dt.ExistingLinks {
			dom, subdom, _ := el.URL.TLDPlusOneAndSubdomain()
			if el.Status == -1 {
				q = db.Query(`INSERT INTO links (dom, subdom, path, proto, time, getnow)
								VALUES (?, ?, ?, ?, ?, ?)`,
					dom,
					subdom,
					el.URL.RequestURI(),
					el.URL.Scheme,
					el.URL.LastCrawled,
					el.GetNow)
			} else {
				q = db.Query(`INSERT INTO links (dom, subdom, path, proto, time, stat, getnow)
								VALUES (?, ?, ?, ?, ?, ?, ?)`,
					dom,
					subdom,
					el.URL.RequestURI(),
					el.URL.Scheme,
					el.URL.LastCrawled,
					el.Status,
					el.GetNow)
			}
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test links: %v\nQuery: %v", err, q)
			}
		}

		d := &Dispatcher{}
		go d.StartDispatcher()
		time.Sleep(time.Millisecond * 100)
		d.StopDispatcher()

		expectedResults := map[url.URL]bool{}
		for _, esl := range dt.ExpectedSegmentLinks {
			expectedResults[*esl.URL] = true
		}

		results := map[url.URL]bool{}
		iter := db.Query(`SELECT dom, subdom, path, proto
							FROM segments WHERE dom = 'test.com'`).Iter()
		var linkdomain, subdomain, path, protocol string
		for iter.Scan(&linkdomain, &subdomain, &path, &protocol) {
			u, _ := walker.CreateURL(linkdomain, subdomain, path, protocol, walker.NotYetCrawled)
			results[*u.URL] = true
		}
		if !reflect.DeepEqual(results, expectedResults) {
			t.Errorf("For tag %q expected results in segments: %v\nBut got: %v",
				dt.Tag, expectedResults, results)
		}

	}

}

func TestAutoUnclaim(t *testing.T) {
	// This test shows that the dispatcher will reclaim the dead.com links,
	// but leave the ok.com links alone.
	makeUuid := func() gocql.UUID {
		uuid, err := gocql.RandomUUID()
		if err != nil {
			panic(err)
		}
		return uuid
	}
	okUuid := makeUuid()
	deadUuid := makeUuid()
	flagTime := time.Now()
	var tests = []DispatcherTest{
		DispatcherTest{
			Tag: "UnclaimTest",

			ExistingDomainInfos: []ExistingDomainInfo{
				// Since ok.com is added to active_fetchers, it'll stay in segments
				{
					Dom:        "ok.com",
					ClaimTok:   okUuid,
					Dispatched: true,
				},

				// Since dead.com isn't on active_fetchers, then all the dead.com links
				// should be removed from segments, and the claim_tok of dead.com in
				// domain_info should be zeroed. The dispatcher will pick the dead.com
				// links up again, so we have to use flagTime to verify the condition on
				// segments (see below).
				{
					Dom:        "dead.com",
					ClaimTok:   deadUuid,
					Dispatched: true,
				},
			},

			ExistingLinks: []ExistingLink{
				{URL: walker.URL{URL: helpers.UrlParse("http://ok.com/page1.html"),
					LastCrawled: walker.NotYetCrawled}, Status: -1},
				{URL: walker.URL{URL: helpers.UrlParse("http://ok.com/page2.html"),
					LastCrawled: walker.NotYetCrawled}, Status: -1},
				{URL: walker.URL{URL: helpers.UrlParse("http://dead.com/page3.html"),
					LastCrawled: walker.NotYetCrawled}, Status: -1},
				{URL: walker.URL{URL: helpers.UrlParse("http://dead.com/page4.html"),
					LastCrawled: walker.NotYetCrawled}, Status: -1},
			},
		},
	}

	var q *gocql.Query
	for _, dt := range tests {
		db := GetTestDB()
		for _, edi := range dt.ExistingDomainInfos {
			q = db.Query(`INSERT INTO domain_info (dom, claim_tok, priority, dispatched, excluded)
							VALUES (?, ?, ?, ?, ?)`,
				edi.Dom, edi.ClaimTok, edi.Priority, edi.Dispatched, edi.Excluded)
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test domain info: %v\nQuery: %v", err, q)
			}

			if edi.ClaimTok == okUuid {
				q = db.Query(`INSERT INTO active_fetchers (tok) VALUES (?)`, edi.ClaimTok)
				if err := q.Exec(); err != nil {
					t.Fatalf("Failed to insert into active_fetchers: %v\nQuery: %v", err, q)
				}
			}
		}

		for _, el := range dt.ExistingLinks {
			dom, subdom, _ := el.URL.TLDPlusOneAndSubdomain()
			if el.Status == -1 {
				q = db.Query(`INSERT INTO links (dom, subdom, path, proto, time, getnow)
								VALUES (?, ?, ?, ?, ?, ?)`,
					dom,
					subdom,
					el.URL.RequestURI(),
					el.URL.Scheme,
					el.URL.LastCrawled,
					el.GetNow)
			} else {
				q = db.Query(`INSERT INTO links (dom, subdom, path, proto, time, stat, getnow)
								VALUES (?, ?, ?, ?, ?, ?, ?)`,
					dom,
					subdom,
					el.URL.RequestURI(),
					el.URL.Scheme,
					el.URL.LastCrawled,
					el.Status,
					el.GetNow)
			}
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test links: %v\nQuery: %v", err, q)
			}

			q = db.Query(`INSERT INTO segments (dom, subdom, path, proto, time) VALUES (?, ?, ?, ?, ?)`,
				dom,
				subdom,
				el.URL.RequestURI(),
				el.URL.Scheme,
				flagTime)
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert segments: %v\nQuery: %v", err, q)
			}
		}

		d := &Dispatcher{}
		go d.StartDispatcher()
		time.Sleep(1500 * time.Millisecond)
		d.StopDispatcher()

		// Test that the UUID of dead.com has been cleared
		expectedTok := map[string]gocql.UUID{
			"ok.com":   okUuid,
			"dead.com": gocql.UUID{},
		}

		iter := db.Query(`SELECT dom, claim_tok FROM domain_info`).Iter()
		var dom string
		var claim_tok gocql.UUID
		for iter.Scan(&dom, &claim_tok) {
			exp, expOk := expectedTok[dom]
			if !expOk {
				t.Errorf("Failed to find domain %v in expectedToks", dom)
			} else if claim_tok != exp {
				t.Errorf("claim_tok mismatch for domain %v: got %v, expected %v", dom, claim_tok, exp)
			}
		}
		err := iter.Close()
		if err != nil {
			t.Fatalf("Failed select read: %v", err)
		}

		// Now we look at the time in segments, as you can see above we insert flagTime into the time
		// slot of segments for all links in ok.com, and dead.com. But when the dead.com links are
		// replaced, they're time field will be updated to walker.NotYetCrawled
		expectedTimes := map[string]time.Time{
			"ok.com":   flagTime,
			"dead.com": walker.NotYetCrawled,
		}
		seen := map[string]bool{}
		iter = db.Query(`SELECT dom, time FROM segments`).Iter()
		var got time.Time
		for iter.Scan(&dom, &got) {
			seen[dom] = true
			exp, expOk := expectedTimes[dom]
			if !expOk {
				t.Errorf("Failed to find domain %v in expectedTimes", dom)
			} else {
				delta := got.Sub(exp)
				if delta < 0 {
					delta = -delta
				}
				seconds := delta / time.Second
				if seconds > 3 {
					t.Errorf("time mismatch for domain %v, delta > 3s: time-exp == duration %v", dom, delta)
				}
			}
		}
		err = iter.Close()
		if err != nil {
			t.Fatalf("Failed select from segments: %v", err)
		}

		for dom := range expectedTimes {
			if !seen[dom] {
				t.Errorf("Expected to find domain %v, but didn't", dom)
			}
		}
	}
}

func TestDispatchInterval(t *testing.T) {
	origDispatchInterval := walker.Config.Dispatcher.DispatchInterval
	defer func() {
		walker.Config.Dispatcher.DispatchInterval = origDispatchInterval
	}()
	walker.Config.Dispatcher.DispatchInterval = "500ms"

	GetTestDB() // Clear the database
	ds := getDS(t)
	p := helpers.Parse("http://test.com/")

	ds.InsertLink(p.String(), "")

	d := &Dispatcher{}
	go d.StartDispatcher()
	time.Sleep(time.Millisecond * 200)

	// By now the link should have been dispatched. Pretend we crawled it.
	host := ds.ClaimNewHost()
	for _ = range ds.LinksForHost(host) {
	}
	ds.UnclaimHost(host)

	// Give it time to dispatch again; it should not do it due to 500ms interval
	time.Sleep(time.Millisecond * 200)

	d.StopDispatcher()

	host = ds.ClaimNewHost()
	if host != "" {
		t.Error("Expected host not to be dispatched again due to dispatch interval")
	}
}

func TestURLCorrection(t *testing.T) {
	origPurgeSidList := walker.Config.Fetcher.PurgeSidList
	origCorrectLinkNormalization := walker.Config.Dispatcher.CorrectLinkNormalization
	defer func() {
		walker.Config.Fetcher.PurgeSidList = origPurgeSidList
		walker.Config.Dispatcher.CorrectLinkNormalization = origCorrectLinkNormalization
		walker.PostConfigHooks()
	}()
	walker.Config.Fetcher.PurgeSidList = []string{"jsessionid", "phpsessid"}
	walker.Config.Dispatcher.CorrectLinkNormalization = true
	walker.PostConfigHooks()

	db := GetTestDB()

	tests := []struct {
		tag    string
		input  string
		expect string
	}{
		{
			tag:    "UpCase",
			input:  "HTTP://A1.com/page1.com",
			expect: "http://a1.com/page1.com",
		},
		{
			tag:    "Fragment",
			input:  "http://a2.com/page1.com#Fragment",
			expect: "http://a2.com/page1.com",
		},
		{
			tag:    "PathSID",
			input:  "http://a3.com/page1.com;jsEssIoniD=436100313FAFBBB9B4DC8BA3C2EC267B",
			expect: "http://a3.com/page1.com",
		},
		{
			tag:    "PathSID2",
			input:  "http://a4.com/page1.com;phPseSsId=436100313FAFBBB9B4DC8BA3C2EC267B",
			expect: "http://a4.com/page1.com",
		},
		{
			tag:    "QuerySID",
			input:  "http://a5.com/page1.com?foo=bar&jsessionID=436100313FAFBBB9B4DC8BA3C2EC267B&baz=niffler",
			expect: "http://a5.com/page1.com?baz=niffler&foo=bar",
		},
		{
			tag:    "QuerySID2",
			input:  "http://a6.com/page1.com?PHPSESSID=436100313FAFBBB9B4DC8BA3C2EC267B",
			expect: "http://a6.com/page1.com",
		},
	}

	expected := map[string]int{}
	for _, tst := range tests {
		expected[tst.expect] = 1

		u, err := walker.ParseURL(tst.input)
		if err != nil {
			t.Fatalf("Failed to parse url %v: %v", tst.input, err)
		}

		dom, subdom, err := u.TLDPlusOneAndSubdomain()
		if err != nil {
			t.Fatalf("Failed to find dom/subdom for %v: %v", tst.input, err)
		}
		path := u.RequestURI()
		proto := u.Scheme
		err = db.Query(`INSERT INTO links (dom, subdom, path, proto, time) VALUES (?, ?, ?, ?, ?)`,
			dom, subdom, path, proto, walker.NotYetCrawled).Exec()
		if err != nil {
			t.Fatalf("Failed to insert into links for %v: %v", tst.input, err)
		}
		err = db.Query(`INSERT INTO domain_info (dom) VALUES (?)`, dom).Exec()
		if err != nil {
			t.Fatalf("Failed to insert into domain_info for %v: %v", tst.input, err)
		}
	}

	d := &Dispatcher{}
	go d.StartDispatcher()
	time.Sleep(1500 * time.Millisecond)
	d.StopDispatcher()

	//
	// Verify that the links have been changed correctly
	//
	var dom, subdom, path, proto string
	itr := db.Query("SELECT dom, subdom, path, proto FROM links").Iter()
	for itr.Scan(&dom, &subdom, &path, &proto) {
		u, err := walker.CreateURL(dom, subdom, path, proto, walker.NotYetCrawled)
		if err != nil {
			t.Fatalf("Failed to create url: %v", err)
		}
		count, found := expected[u.String()]
		if !found {
			t.Errorf("Failed to find link %v in post-dispatched links", u.String())
			continue
		} else if count > 1 {
			t.Errorf("Double counted link %v in post-dispatched links", u.String())
			continue
		}
		expected[u.String()] = 2
	}
	err := itr.Close()
	if err != nil {
		t.Fatalf("Failed to iterate over links: %v", err)
	}

	for k, count := range expected {
		if count != 2 {
			t.Errorf("Expected to find link %v in post-dispatched links, but didn't", k)
		}
	}

	//
	// The UpCase tag above changes the domain, which triggers an addition to domain_info. Verify
	// that it is present
	//
	expectedDomainsAdded := map[string]bool{
		"a1.com": true,
	}
	itr = db.Query("SELECT dom FROM domain_info").Iter()
	for itr.Scan(&dom) {
		delete(expectedDomainsAdded, dom)
	}

	for dom := range expectedDomainsAdded {
		t.Errorf("Expected to find %q added to domain_info, but didn't find that", dom)
	}
}
