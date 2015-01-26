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

	// zero value indicates the page is unique and should use a random unique int64
	FnvTextFingerprint int64
}

var MaxPriority = 10

var DispatcherTests = []DispatcherTest{
	DispatcherTest{
		Tag: "BasicTest",

		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},

		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: walker.MustParse("http://test.com/").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},

		ExpectedSegmentLinks: []walker.URL{
			{URL: walker.MustParse("http://test.com/").URL,
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
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page2.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page404.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page500.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},

			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled1.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled2.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled3.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled4.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled5.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},

			{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page2.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -3)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page404.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -2)}, Status: http.StatusNotFound},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page500.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -1)}, Status: http.StatusInternalServerError},

			{URL: walker.URL{URL: walker.MustParse("http://test.com/getnow1.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1, GetNow: true},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/getnow2.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1, GetNow: true},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/getnow3.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1, GetNow: true},
		},

		ExpectedSegmentLinks: []walker.URL{
			// The two oldest already crawled links
			{URL: walker.MustParse("http://test.com/page1.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4)},
			{URL: walker.MustParse("http://test.com/page2.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -3)},

			// 4 uncrawled links
			{URL: walker.MustParse("http://test.com/notcrawled1.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled2.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled3.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled4.html").URL,
				LastCrawled: walker.NotYetCrawled},

			// all of the getnow links
			{URL: walker.MustParse("http://test.com/getnow1.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/getnow2.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/getnow3.html").URL,
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
			{URL: walker.URL{URL: walker.MustParse("http://test.com/l.html").URL,
				LastCrawled: time.Now().AddDate(0, -2, -4)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/m.html").URL,
				LastCrawled: time.Now().AddDate(0, -3, -1)}, Status: http.StatusOK},

			{URL: walker.URL{URL: walker.MustParse("http://test.com/a.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -1)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/b.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -2)}, Status: http.StatusOK},

			{URL: walker.URL{URL: walker.MustParse("http://test.com/c.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -3)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/d.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/e.html").URL,
				LastCrawled: time.Now().AddDate(0, -1, -1)}, Status: http.StatusOK},

			{URL: walker.URL{URL: walker.MustParse("http://test.com/f.html").URL,
				LastCrawled: time.Now().AddDate(0, -1, -2)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/g.html").URL,
				LastCrawled: time.Now().AddDate(0, -1, -3)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/h.html").URL,
				LastCrawled: time.Now().AddDate(0, -1, -4)}, Status: http.StatusOK},

			{URL: walker.URL{URL: walker.MustParse("http://test.com/i.html").URL,
				LastCrawled: time.Now().AddDate(0, -2, -1)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/j.html").URL,
				LastCrawled: time.Now().AddDate(0, -2, -2)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/k.html").URL,
				LastCrawled: time.Now().AddDate(0, -2, -3)}, Status: http.StatusOK},

			// These two links cover up the previous two l and m.html links.
			{URL: walker.URL{URL: walker.MustParse("http://test.com/l.html").URL,
				LastCrawled: time.Now()}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/m.html").URL,
				LastCrawled: time.Now()}, Status: http.StatusOK},
		},

		ExpectedSegmentLinks: []walker.URL{
			// 9 Oldest links
			{URL: walker.MustParse("http://test.com/k.html").URL,
				LastCrawled: time.Now().AddDate(0, -2, -3)},
			{URL: walker.MustParse("http://test.com/j.html").URL,
				LastCrawled: time.Now().AddDate(0, -2, -2)},
			{URL: walker.MustParse("http://test.com/i.html").URL,
				LastCrawled: time.Now().AddDate(0, -2, -1)},

			{URL: walker.MustParse("http://test.com/h.html").URL,
				LastCrawled: time.Now().AddDate(0, -1, -4)},
			{URL: walker.MustParse("http://test.com/g.html").URL,
				LastCrawled: time.Now().AddDate(0, -1, -3)},
			{URL: walker.MustParse("http://test.com/f.html").URL,
				LastCrawled: time.Now().AddDate(0, -1, -2)},

			{URL: walker.MustParse("http://test.com/e.html").URL,
				LastCrawled: time.Now().AddDate(0, -1, -1)},
			{URL: walker.MustParse("http://test.com/d.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4)},
			{URL: walker.MustParse("http://test.com/c.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -3)},
		},
	},

	DispatcherTest{
		Tag: "NoGetNow",

		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},

		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page2.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page404.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page500.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},

			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled1.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled2.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled3.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled4.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled5.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled6.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled7.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled8.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled9.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},

			{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page2.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -3)}, Status: http.StatusOK},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page404.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -2)}, Status: http.StatusNotFound},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page500.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -1)}, Status: http.StatusInternalServerError},
		},

		ExpectedSegmentLinks: []walker.URL{
			// 3 crawled links
			{URL: walker.MustParse("http://test.com/page1.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4)},
			{URL: walker.MustParse("http://test.com/page2.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -3)},
			{URL: walker.MustParse("http://test.com/page404.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -2)},

			// 6 uncrawled links
			{URL: walker.MustParse("http://test.com/notcrawled1.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled2.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled3.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled4.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled5.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled6.html").URL,
				LastCrawled: walker.NotYetCrawled},
		},
	},

	DispatcherTest{
		Tag: "OnlyUncrawled",

		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},

		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled1.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled2.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled3.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled4.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled5.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled6.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled7.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled8.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
			{URL: walker.URL{URL: walker.MustParse("http://test.com/notcrawled9.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},

		ExpectedSegmentLinks: []walker.URL{
			{URL: walker.MustParse("http://test.com/notcrawled1.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled2.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled3.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled4.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled5.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled6.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled7.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled8.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/notcrawled9.html").URL,
				LastCrawled: walker.NotYetCrawled},
		},
	},

	DispatcherTest{ // Verifies that we work with query parameters properly
		Tag: "QueryParmsOK",
		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},
		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html?p=v").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},
		ExpectedSegmentLinks: []walker.URL{
			{URL: walker.MustParse("http://test.com/page1.html?p=v").URL,
				LastCrawled: walker.NotYetCrawled},
		},
	},

	DispatcherTest{ // Verifies that we don't generate an already-dispatched domain
		Tag: "NoAlreadyDispatched",
		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com", Dispatched: true},
		},
		ExistingLinks: []ExistingLink{
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
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
			{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},
		ExpectedSegmentLinks: []walker.URL{},
		NoDispatchExpected:   true,
	},

	DispatcherTest{
		Tag: "BasicQueryParameterFiltering",
		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},
		ExistingLinks: []ExistingLink{
			// First two links should be able to figure out that we don't need 'pag' parameter
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/page1.html").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 12345},
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/page1.html?pag=1").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 12345},
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/page1.html?pag=1&pag=1").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},

			// Page with different path, should still be crawled
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/page2.html?pag=1").URL,
				LastCrawled: walker.NotYetCrawled}, Status: -1},
		},
		ExpectedSegmentLinks: []walker.URL{
			{URL: walker.MustParse("http://test.com/page1.html").URL,
				LastCrawled: walker.NotYetCrawled},
			{URL: walker.MustParse("http://test.com/page2.html?pag=1").URL,
				LastCrawled: walker.NotYetCrawled},
		},
	},

	DispatcherTest{
		Tag: "MultiParameterQueryFiltering",
		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},
		ExistingLinks: []ExistingLink{
			// parameter 'a' is the only consistent one and should remain
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/?a=b&c=d&e=f").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 3456},
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/?e=f&a=b").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 3456},
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/?c=d&a=b").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 3456},
		},
		ExpectedSegmentLinks: []walker.URL{
			{URL: walker.MustParse("http://test.com/?a=b").URL,
				LastCrawled: walker.NotYetCrawled},
		},
	},

	DispatcherTest{
		Tag: "QueryFilteringDistinguishesSubdomains",
		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},
		ExistingLinks: []ExistingLink{
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/?a=b").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 111222},
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 111222},
			{URL: walker.URL{
				URL:         walker.MustParse("http://www.test.com/?a=b").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 111222},
			{URL: walker.URL{
				URL:         walker.MustParse("http://www.test.com/?c=d&e=f").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 1234},
			{URL: walker.URL{
				URL:         walker.MustParse("http://www.test.com/?e=f").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 1234},
		},
		ExpectedSegmentLinks: []walker.URL{
			// Expect parameter 'a' to be gone due to page duplication
			{URL: walker.MustParse("http://test.com/").URL,
				LastCrawled: walker.NotYetCrawled},

			// Still expect this www link, since it's on a different subdomain
			// from the pages with the same fingerprint
			{URL: walker.MustParse("http://www.test.com/?a=b").URL,
				LastCrawled: walker.NotYetCrawled},

			// Expect parameter 'c' to be filtered only for the www links
			{URL: walker.MustParse("http://www.test.com/?e=f").URL,
				LastCrawled: walker.NotYetCrawled},
		},
	},

	DispatcherTest{
		Tag: "QueryFilteringClearsDuplicateLinks",
		ExistingDomainInfos: []ExistingDomainInfo{
			{Dom: "test.com"},
		},
		ExistingLinks: []ExistingLink{
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/?a=b").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 111222},
			{URL: walker.URL{
				URL:         walker.MustParse("http://test.com/").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 111222},
			{URL: walker.URL{
				URL:         walker.MustParse("http://www.test.com/?a=b").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 111222},
			{URL: walker.URL{
				URL:         walker.MustParse("http://www.test.com/?c=d&e=f").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 1234},
			{URL: walker.URL{
				URL:         walker.MustParse("http://www.test.com/?e=f").URL,
				LastCrawled: time.Now().AddDate(0, 0, -4),
			},
				Status:             http.StatusOK,
				FnvTextFingerprint: 1234},
		},
		ExpectedSegmentLinks: []walker.URL{
			// Expect parameter 'a' to be gone due to page duplication
			{URL: walker.MustParse("http://test.com/").URL,
				LastCrawled: walker.NotYetCrawled},

			// Still expect this www link, since it's on a different subdomain
			// from the pages with the same fingerprint
			{URL: walker.MustParse("http://www.test.com/?a=b").URL,
				LastCrawled: walker.NotYetCrawled},

			// Expect parameter 'c' to be filtered only for the www links
			{URL: walker.MustParse("http://www.test.com/?e=f").URL,
				LastCrawled: walker.NotYetCrawled},
		},
	},
}

func runDispatcher(t *testing.T) {
	d, err := NewDispatcher()
	if err != nil {
		t.Fatalf("Failed to create dispatcher: %v", err)
	}
	err = d.oneShot(1)
	if err != nil {
		t.Fatalf("Failed to run dispatcher: %v", err)
	}
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
			priority := edi.Priority
			if priority == 0 {
				priority = MaxPriority
			}

			q = db.Query(`INSERT INTO domain_info (dom, claim_tok, priority, dispatched, excluded)
							VALUES (?, ?, ?, ?, ?)`,
				edi.Dom, edi.ClaimTok, priority, edi.Dispatched, edi.Excluded)
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test domain info: %v\nQuery: %v", err, q)
			}
		}

		nextAutoFingerprint := int64(1000)
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
				if el.FnvTextFingerprint == 0 {
					el.FnvTextFingerprint = nextAutoFingerprint
					nextAutoFingerprint += 1
				}
				q = db.Query(`INSERT INTO links (dom, subdom, path, proto, time, stat, getnow, fnv_txt)
								VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
					dom,
					subdom,
					el.URL.RequestURI(),
					el.URL.Scheme,
					el.URL.LastCrawled,
					el.Status,
					el.GetNow,
					el.FnvTextFingerprint)
			}
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test links: %v\nQuery: %v", err, q)
			}
		}

		runDispatcher(t)

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
					VALUES (?, ?, ?, ?)`, "test.com", gocql.UUID{}, 1, false)
	if err := q.Exec(); err != nil {
		t.Fatalf("Failed to insert test domain info: %v\nQuery: %v", err, q)
	}

	runDispatcher(t)

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
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
					LastCrawled: now.AddDate(0, 0, -1)}, Status: -1},
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page2.html").URL,
					LastCrawled: now.AddDate(0, 0, -2)}, Status: -1},
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page3.html").URL,
					LastCrawled: now.AddDate(0, 0, -3)}, Status: -1},
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page4.html").URL,
					LastCrawled: now.AddDate(0, 0, -4)}, Status: -1},
			},

			ExpectedSegmentLinks: []walker.URL{
				{URL: walker.MustParse("http://test.com/page3.html").URL,
					LastCrawled: now.AddDate(0, 0, -3)},
				{URL: walker.MustParse("http://test.com/page4.html").URL,
					LastCrawled: now.AddDate(0, 0, -4)},
			},
		},
	}

	var q *gocql.Query
	for _, dt := range tests {
		db := GetTestDB() // runs between tests to reset the db

		for _, edi := range dt.ExistingDomainInfos {
			priority := edi.Priority
			if priority == 0 {
				priority = MaxPriority
			}

			q = db.Query(`INSERT INTO domain_info (dom, claim_tok, priority, dispatched, excluded)
							VALUES (?, ?, ?, ?, ?)`,
				edi.Dom, edi.ClaimTok, priority, edi.Dispatched, edi.Excluded)
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

		runDispatcher(t)

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
				{URL: walker.URL{URL: walker.MustParse("http://ok.com/page1.html").URL,
					LastCrawled: walker.NotYetCrawled}, Status: -1},
				{URL: walker.URL{URL: walker.MustParse("http://ok.com/page2.html").URL,
					LastCrawled: walker.NotYetCrawled}, Status: -1},
				{URL: walker.URL{URL: walker.MustParse("http://dead.com/page3.html").URL,
					LastCrawled: walker.NotYetCrawled}, Status: -1},
				{URL: walker.URL{URL: walker.MustParse("http://dead.com/page4.html").URL,
					LastCrawled: walker.NotYetCrawled}, Status: -1},
			},
		},
	}

	var q *gocql.Query
	for _, dt := range tests {
		db := GetTestDB()
		for _, edi := range dt.ExistingDomainInfos {
			priority := edi.Priority
			if priority == 0 {
				priority = MaxPriority
			}
			q = db.Query(`INSERT INTO domain_info (dom, claim_tok, priority, dispatched, excluded)
							VALUES (?, ?, ?, ?, ?)`,
				edi.Dom, edi.ClaimTok, priority, edi.Dispatched, edi.Excluded)
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

		// Yes, you DO need to run the dispatcher for two iterations. The first run
		// will queue the domains, the second will call fetcherIsAlive and
		// cleanStrandedClaims
		d, err := NewDispatcher()
		if err != nil {
			t.Fatalf("Failed to create dispatcher: %v", err)
		}
		err = d.oneShot(2)
		if err != nil {
			t.Fatalf("Failed to run dispatcher: %v", err)
		}

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
		err = iter.Close()
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
	walker.Config.Dispatcher.DispatchInterval = "600ms"

	completedTest := false
	for _ = range []int{0, 1, 2} {
		GetTestDB() // Clear the database
		ds := getDS(t)
		p := walker.MustParse("http://test.com/")

		ds.InsertLink(p.String(), "")

		start := time.Now()

		d, err := NewDispatcher()
		if err != nil {
			t.Fatalf("Failed to create dispatcher: %v", err)
		}
		go d.StartDispatcher()
		time.Sleep(time.Millisecond * 200)

		// By now the link should have been dispatched. Pretend we crawled it.
		host := ds.ClaimNewHost()
		for _ = range ds.LinksForHost(host) {
		}
		ds.UnclaimHost(host)

		// Give it time to dispatch again; it should not do it due to 600ms interval
		time.Sleep(time.Millisecond * 200)
		d.StopDispatcher()

		// For any typical run the variable duration should be << tolerance. But in order to make the
		// test deterministic, in the (very) unlikely event that duration is pathologically long, we accept the
		// test but warn. We include this contingency for hardware that is very resource starved (like Travis)
		duration := time.Since(start)
		tolerance, _ := time.ParseDuration(walker.Config.Dispatcher.DispatchInterval)
		if duration > tolerance {
			t.Logf("WARNING: TestDispatchInterval found unusually long  duration: %v", duration)
			continue // retry
		}

		host = ds.ClaimNewHost()
		if duration <= tolerance && host != "" {
			t.Error("Expected host not to be dispatched again due to dispatch interval")
		}

		completedTest = true
		break
	}
	if !completedTest {
		t.Fatalf("Failed to complete test")
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
		double bool
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

		{
			tag:    "BugFix for TRN-135",
			input:  "http://A7.com/page1.com",
			expect: "http://a7.com/page1.com",
			double: true,
		},
	}

	expected := map[string]int{}
	got := map[string]int{}
	for _, tst := range tests {
		expected[tst.expect] = 1
		got[tst.expect] = 0

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
		if tst.double {
			err = db.Query(`INSERT INTO links (dom, subdom, path, proto, time) VALUES (?, ?, ?, ?, ?)`,
				dom, subdom, path, proto, time.Now()).Exec()
			if err != nil {
				t.Fatalf("Failed to insert into links (2nd time) for %v: %v", tst.input, err)
			}
			expected[tst.expect] = 2
		}
		err = db.Query(`INSERT INTO domain_info (dom, priority) VALUES (?, ?)`, dom, MaxPriority).Exec()
		if err != nil {
			t.Fatalf("Failed to insert into domain_info for %v: %v", tst.input, err)
		}
	}

	runDispatcher(t)

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
		count, found := got[u.String()]
		if !found {
			t.Errorf("Unexpected link %v in post-dispatched links", u.String())
			continue
		}
		got[u.String()] = count + 1

		if got[u.String()] > expected[u.String()] {
			t.Errorf("Multi counted link %v in post-dispatched links", u.String())
			continue
		}
	}
	err := itr.Close()
	if err != nil {
		t.Fatalf("Failed to iterate over links: %v", err)
	}

	for k := range expected {
		if got[k] != expected[k] {
			t.Errorf("Expected to find link %v in post-dispatched links with count %d, but found count %d",
				k, expected[k], got[k])
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

func TestDomainInfoStats(t *testing.T) {
	orig := walker.Config.Dispatcher.MinLinkRefreshTime
	func() {
		walker.Config.Dispatcher.MinLinkRefreshTime = orig
	}()
	walker.Config.Dispatcher.MinLinkRefreshTime = "12h"

	var now = time.Now()
	var tests = []DispatcherTest{
		DispatcherTest{
			Tag: "BasicTest",

			ExistingDomainInfos: []ExistingDomainInfo{
				{Dom: "test.com"},
			},

			ExistingLinks: []ExistingLink{
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
					LastCrawled: now.AddDate(0, 0, -1)}},
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
					LastCrawled: now.AddDate(0, 0, -2)}},
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
					LastCrawled: now.AddDate(0, 0, -3)}},
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page1.html").URL,
					LastCrawled: now.AddDate(0, 0, -4)}},
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page2.html").URL,
					LastCrawled: walker.NotYetCrawled}},
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page3.html").URL,
					LastCrawled: walker.NotYetCrawled}},
				{URL: walker.URL{URL: walker.MustParse("http://test.com/page4.html").URL,
					LastCrawled: time.Now()}},
			},
		},
	}

	var q *gocql.Query
	for _, dt := range tests {
		db := GetTestDB() // runs between tests to reset the db

		for _, edi := range dt.ExistingDomainInfos {
			priority := edi.Priority
			if priority == 0 {
				priority = MaxPriority
			}
			q = db.Query(`INSERT INTO domain_info (dom, claim_tok, priority, dispatched, excluded)
							VALUES (?, ?, ?, ?, ?)`,
				edi.Dom, edi.ClaimTok, priority, edi.Dispatched, edi.Excluded)
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test domain info: %v\nQuery: %v", err, q)
			}
		}

		for _, el := range dt.ExistingLinks {
			dom, subdom, _ := el.URL.TLDPlusOneAndSubdomain()
			q = db.Query(`INSERT INTO links (dom, subdom, path, proto, time, getnow)
								VALUES (?, ?, ?, ?, ?, ?)`,
				dom,
				subdom,
				el.URL.RequestURI(),
				el.URL.Scheme,
				el.URL.LastCrawled,
				el.GetNow)
			if err := q.Exec(); err != nil {
				t.Fatalf("Failed to insert test links: %v\nQuery: %v", err, q)
			}
		}

		runDispatcher(t)

		var linksCount, uncrawledLinksCount, queuedLinksCount int
		err := db.Query(`SELECT tot_links, uncrawled_links, queued_links 
						 FROM domain_info 
						 WHERE dom = 'test.com'`).Scan(&linksCount, &uncrawledLinksCount, &queuedLinksCount)
		if err != nil {
			t.Fatalf("Select direct error: %v", err)
		}
		if linksCount != 4 {
			t.Errorf("tot_links mismatch: got %d, expected %d", linksCount, 4)
		}
		if uncrawledLinksCount != 2 {
			t.Errorf("uncrawled_links mismatch: got %d, expected %d", uncrawledLinksCount, 2)
		}
		if queuedLinksCount != 3 {
			t.Errorf("queued_links mismatch: got %d, expected %d", queuedLinksCount, 3)
		}
	}

}

func TestDispatchPruning(t *testing.T) {
	orig := walker.Config.Dispatcher.EmptyDispatchRetryInterval
	func() {
		walker.Config.Dispatcher.EmptyDispatchRetryInterval = orig
	}()
	walker.Config.Dispatcher.EmptyDispatchRetryInterval = "15m"

	db := GetTestDB() // runs between tests to reset the db
	delT1, _ := time.ParseDuration("10m")
	delT2, _ := time.ParseDuration("20m")
	delT3, _ := time.ParseDuration("30m")
	time0 := time.Now()
	time1 := time0.Add(-delT1)
	time2 := time0.Add(-delT2)
	time3 := time0.Add(-delT3)

	tests := []struct {
		dom               string
		lastDispatch      time.Time
		lastEmptyDispatch time.Time
	}{
		// should dispatch last_dispatch > last_empty_dispatch
		{"a.com", time0, time1},

		// should NOT dispatch: last_dispatch < last_empty_dispatch
		// && |now()-last_empty_dispatch| < EmptyDispatchRetryInterval
		{"b.com", time2, time1},

		// should dispatch: last_dispatch < last_empty_dispatch
		// && |now()-last_empty_dispatch| > EmptyDispatchRetryInterval
		{"c.com", time3, time2},
	}

	insertDomain := `INSERT INTO domain_info (dom, last_dispatch, last_empty_dispatch, dispatched) VALUES (?, ?, ?, false)`
	insertLink := `INSERT INTO links (dom, subdom, path, proto, time) VALUES (?, ?, ?, ?, ?)`
	for _, tst := range tests {
		err := db.Query(insertDomain, tst.dom, tst.lastDispatch, tst.lastEmptyDispatch).Exec()
		if err != nil {
			t.Fatalf("Failed to insert domain: %v", err)
		}
		err = db.Query(insertLink, tst.dom, "", "/page1.html", "http", time0.AddDate(0, 0, -1)).Exec()
		if err != nil {
			t.Fatalf("Failed to insert link: %v", err)
		}
	}

	d, err := NewDispatcher()
	if err != nil {
		t.Fatalf("Dispatcher failed to initialize: %v", err)
	}
	go d.StartDispatcher()
	time.Sleep(time.Millisecond * 300)
	d.StopDispatcher()

	itr := db.Query("SELECT dom FROM segments").Iter()
	var domain string
	got := map[string]bool{}
	for itr.Scan(&domain) {
		got[domain] = true
	}

	expected := map[string]bool{
		"a.com": true,
		"c.com": true,
	}

	for dom := range got {
		if !expected[dom] {
			t.Errorf("Didn't expect domain %q in segments table", dom)
		}
		delete(expected, dom)
	}

	for dom := range expected {
		t.Errorf("Failed to find expected domain %q", dom)
	}

}
