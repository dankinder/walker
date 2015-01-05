package cassandra

//
// This test is specifically for elements of the Datastore that are used by the
// console.
//
// TODO: there is likely still room to further synthesize these test and
// 		 datastore_test.go (these tests were totally separate in console/test/
// 		 before they were brought in here). For example we could consider
// 		 building helpers.LoadTestData further into a robust fixtures
// 		 infrastructure and incorporate it into datastore_test.go

import (
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/helpers"
)

//
// Some global data
//
var fooTime = time.Now().AddDate(0, 0, -1)
var testTime = time.Now().AddDate(0, 0, -2)
var bazUUID, _ = gocql.RandomUUID()
var testComLinkOrder []walker.LinkInfo
var testComLinkHash = map[string]walker.LinkInfo{
	"http://test.com/page1.html": walker.LinkInfo{
		URL:            helpers.Parse("http://test.com/page1.html"),
		Status:         200,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"http://test.com/page2.html": walker.LinkInfo{
		URL:            helpers.Parse("http://test.com/page2.html"),
		Status:         200,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"http://test.com/page3.html": walker.LinkInfo{
		URL:            helpers.Parse("http://test.com/page3.html"),
		Status:         404,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"http://test.com/page4.html": walker.LinkInfo{
		URL:            helpers.Parse("http://test.com/page4.html"),
		Status:         200,
		Error:          "An Error",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"http://test.com/page5.html": walker.LinkInfo{
		URL:            helpers.Parse("http://test.com/page5.html"),
		Status:         200,
		Error:          "",
		RobotsExcluded: true,
		CrawlTime:      walker.NotYetCrawled,
	},

	"http://sub.test.com/page6.html": walker.LinkInfo{
		URL:            helpers.Parse("http://sub.test.com/page6.html"),
		Status:         200,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"https://sub.test.com/page7.html": walker.LinkInfo{
		URL:            helpers.Parse("https://sub.test.com/page7.html"),
		Status:         200,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"https://sub.test.com/page8.html": walker.LinkInfo{
		URL:            helpers.Parse("https://sub.test.com/page8.html"),
		Status:         200,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},
}

var bazLinkHistoryOrder []walker.LinkInfo

var bazLinkHistoryInit = []walker.LinkInfo{
	walker.LinkInfo{
		URL:       helpers.Parse("http://sub.baz.com/page1.html"),
		Status:    200,
		CrawlTime: walker.NotYetCrawled,
	},
	walker.LinkInfo{
		URL:       helpers.Parse("http://sub.baz.com/page1.html"),
		Status:    200,
		CrawlTime: time.Now().AddDate(0, 0, -1),
	},
	walker.LinkInfo{
		URL:       helpers.Parse("http://sub.baz.com/page1.html"),
		Status:    200,
		CrawlTime: time.Now().AddDate(0, 0, -2),
	},
	walker.LinkInfo{
		URL:       helpers.Parse("http://sub.baz.com/page1.html"),
		Status:    200,
		CrawlTime: time.Now().AddDate(0, 0, -3),
	},
	walker.LinkInfo{
		URL:       helpers.Parse("http://sub.baz.com/page1.html"),
		Status:    200,
		CrawlTime: time.Now().AddDate(0, 0, -4),
	},
	walker.LinkInfo{
		URL:       helpers.Parse("http://sub.baz.com/page1.html"),
		Status:    200,
		CrawlTime: time.Now().AddDate(0, 0, -5),
	},
}

var bazSeed string

type findTest struct {
	omittest bool
	tag      string
	domain   string
	expected *DomainInfo
}

type domainTest struct {
	omittest bool
	tag      string
	seed     string
	limit    int
	expected []DomainInfo
}

type linkTest struct {
	omittest    bool
	tag         string
	domain      string
	histURL     *walker.URL
	seed        int
	seedURL     *walker.URL
	filterRegex string
	limit       int
	expected    []walker.LinkInfo
}

const LIM = 50

const EpsilonSeconds = 1

func timeClose(l time.Time, r time.Time) bool {
	delta := l.Unix() - r.Unix()
	if delta < 0 {
		delta = -delta
	}
	return delta <= EpsilonSeconds
}

//Shared Domain Information
var bazDomain = DomainInfo{
	Domain:               "baz.com",
	NumberLinksTotal:     1,
	NumberLinksQueued:    1,
	NumberLinksUncrawled: 0,
	ClaimTime:            testTime,
	ClaimToken:           bazUUID,
}

var fooDomain = DomainInfo{
	Domain:               "foo.com",
	NumberLinksTotal:     2,
	NumberLinksQueued:    0,
	NumberLinksUncrawled: 0,
	ClaimTime:            walker.NotYetCrawled,
}

var barDomain = DomainInfo{
	Domain:               "bar.com",
	NumberLinksTotal:     0,
	NumberLinksQueued:    0,
	NumberLinksUncrawled: 0,
	ClaimTime:            walker.NotYetCrawled,
}

var testDomain = DomainInfo{
	Domain:               "test.com",
	NumberLinksTotal:     8,
	NumberLinksQueued:    2,
	NumberLinksUncrawled: 8,
	ClaimTime:            testTime,
	ClaimToken:           gocql.UUID{},
}

var filterDomain = DomainInfo{
	Domain:               "filter.com",
	NumberLinksTotal:     7,
	NumberLinksQueued:    0,
	NumberLinksUncrawled: 7,
	ClaimTime:            testTime,
}

var excludedDomain = DomainInfo{
	Domain:               "excluded.com",
	NumberLinksTotal:     0,
	NumberLinksQueued:    0,
	NumberLinksUncrawled: 0,
	ExcludeReason:        "Reason for exclusion",
	ClaimTime:            walker.NotYetCrawled,
	ClaimToken:           gocql.UUID{},
}

type updatedInDb struct {
	link                string
	domain              string
	path                string
	excludeDomainReason string
}

type insertTest struct {
	omittest bool
	tag      string
	updated  []updatedInDb
}

//
// Fixture generation
//
func getModelTestDatastore(t *testing.T) *Datastore {
	db := GetTestDB()

	insertDomainInfo := `INSERT INTO domain_info (dom, claim_time, priority) VALUES (?, ?, 0)`
	insertDomainToCrawl := `INSERT INTO domain_info (dom, claim_tok, claim_time, dispatched, priority) VALUES (?, ?, ?, true, 0)`
	insertSegment := `INSERT INTO segments (dom, subdom, path, proto) VALUES (?, ?, ?, ?)`
	insertLink := `INSERT INTO links (dom, subdom, path, proto, time, stat, err, robot_ex) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	queries := []*gocql.Query{
		db.Query(insertDomainToCrawl, "test.com", gocql.UUID{}, testTime),
		db.Query(insertLink, "test.com", "", "/page1.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "", "/page2.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "", "/page3.html", "http", walker.NotYetCrawled, 404, "", false),
		db.Query(insertLink, "test.com", "", "/page4.html", "http", walker.NotYetCrawled, 200, "An Error", false),
		db.Query(insertLink, "test.com", "", "/page5.html", "http", walker.NotYetCrawled, 200, "", true),

		db.Query(insertLink, "test.com", "sub", "/page6.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "sub", "/page7.html", "https", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "test.com", "sub", "/page8.html", "https", walker.NotYetCrawled, 200, "", false),

		db.Query(insertSegment, "test.com", "", "/page1.html", "http"),
		db.Query(insertSegment, "test.com", "", "/page2.html", "http"),

		db.Query(insertDomainInfo, "foo.com", walker.NotYetCrawled),
		db.Query(insertLink, "foo.com", "sub", "/page1.html", "http", fooTime, 200, "", false),
		db.Query(insertLink, "foo.com", "sub", "/page2.html", "http", fooTime, 200, "", false),

		db.Query(insertDomainInfo, "bar.com", walker.NotYetCrawled),

		db.Query(insertDomainToCrawl, "baz.com", bazUUID, testTime),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[0].CrawlTime, 200, "", false),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[1].CrawlTime, 200, "", false),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[2].CrawlTime, 200, "", false),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[3].CrawlTime, 200, "", false),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[4].CrawlTime, 200, "", false),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[5].CrawlTime, 200, "", false),

		db.Query(insertSegment, "baz.com", "sub", "page1.html", "http"),

		db.Query(insertDomainToCrawl, "filter.com", gocql.UUID{}, testTime),
		db.Query(insertLink, "filter.com", "", "/aaa.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "filter.com", "", "/bbb.html", "https", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "filter.com", "", "/ccc/ddd.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "filter.com", "subd", "/aaa.html", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "filter.com", "", "/1/2/A", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "filter.com", "", "/1111/2222/A", "http", walker.NotYetCrawled, 200, "", false),
		db.Query(insertLink, "filter.com", "", "/1111/2222/B", "http", walker.NotYetCrawled, 200, "", false),
	}

	for _, q := range queries {
		err := q.Exec()
		if err != nil {
			t.Fatalf("Failed to insert test data: %v\nQuery: %v", err, q)
		}
	}

	{
		insertDomainInfoExcluded := `INSERT INTO domain_info (dom, claim_time, priority, excluded, exclude_reason) 
									 VALUES (?, ?, 0, true, ?)`
		dom := fmt.Sprintf("excluded.com")
		reason := "Reason for exclusion"
		err := db.Query(insertDomainInfoExcluded, dom, walker.NotYetCrawled, reason).Exec()
		if err != nil {
			panic(err)
		}
	}

	//
	// Need to record the order that the test.com urls come off on
	//
	itr := db.Query("SELECT dom, subdom, path, proto FROM links WHERE dom = 'test.com'").Iter()
	var domain, subdomain, path, protocol string
	testComLinkOrder = nil
	for itr.Scan(&domain, &subdomain, &path, &protocol) {
		u, _ := walker.CreateURL(domain, subdomain, path, protocol, walker.NotYetCrawled)
		urlString := u.String()
		linfo, gotLinfo := testComLinkHash[urlString]
		if !gotLinfo {
			panic(fmt.Errorf("testComLinkOrder can't find url: %v", urlString))
		}
		testComLinkOrder = append(testComLinkOrder, linfo)
	}
	err := itr.Close()
	if err != nil {
		panic(fmt.Errorf("testComLinkOrder iterator error: %v", err))
	}

	//
	// Need to record order for baz
	//
	itr = db.Query("SELECT time FROM links WHERE dom = 'baz.com'").Iter()
	var crawlTime time.Time
	bazLinkHistoryOrder = nil
	for itr.Scan(&crawlTime) {
		bestIndex := -1
		var bestDiff int64 = 99999999
		for i := range bazLinkHistoryInit {
			e := &bazLinkHistoryInit[i]
			delta := crawlTime.Unix() - e.CrawlTime.Unix()
			if delta < 0 {
				delta = -delta
			}
			if delta < bestDiff {
				bestIndex = i
				bestDiff = delta
			}
		}
		if bestIndex < 0 {
			panic("UNEXPECTED ERROR")
		}
		bazLinkHistoryOrder = append(bazLinkHistoryOrder, bazLinkHistoryInit[bestIndex])
	}
	err = itr.Close()
	if err != nil {
		panic(fmt.Errorf("bazLinkHistoryOrder iterator error: %v", err))
	}

	itr = db.Query("SELECT dom, subdom, path, proto FROM links").Iter()
	var foundBaz = false
	var beforeBazComLink *walker.URL
	for itr.Scan(&domain, &subdomain, &path, &protocol) {
		url, err := walker.CreateURL(domain, subdomain, path, protocol, walker.NotYetCrawled)
		if err != nil {
			panic(err)
		}
		if domain == "baz.com" {
			foundBaz = true
			break
		}

		beforeBazComLink = url
	}
	err = itr.Close()
	if err != nil {
		panic(fmt.Errorf("beforeBazCom link iterator error: %v", err))
	}
	if !foundBaz {
		panic("Unable to find domain before baz.com")
	}
	if beforeBazComLink == nil {
		bazSeed = ""
	} else {
		bazSeed = beforeBazComLink.String()
	}

	// Update the domain_info stats
	for _, d := range []DomainInfo{bazDomain, fooDomain, barDomain, testDomain, filterDomain, excludedDomain} {
		err := db.Query(`UPDATE domain_info 
					   SET 					   	
					   		tot_links = ?,
					   		uncrawled_links = ?,
					   		queued_links = ?
					   WHERE dom = ?`, d.NumberLinksTotal, d.NumberLinksUncrawled, d.NumberLinksQueued, d.Domain).Exec()
		if err != nil {
			panic(err)
		}
	}

	return getDS(t)
}

//
// THE TESTS
//
func TestListDomains(t *testing.T) {
	store := getModelTestDatastore(t)

	tests := []domainTest{
		domainTest{
			tag:   "Basic Pull",
			limit: LIM,
			expected: []DomainInfo{
				bazDomain,
				excludedDomain,
				filterDomain,
				fooDomain,
				barDomain,
				testDomain,
			},
		},

		domainTest{
			tag:   "Limit Pull",
			limit: 1,
			expected: []DomainInfo{
				bazDomain,
			},
		},

		domainTest{
			tag:   "Seeded Pull",
			seed:  "foo.com",
			limit: LIM,
			expected: []DomainInfo{
				barDomain,
				testDomain,
			},
		},

		domainTest{
			tag:   "Seeded & Limited Pull",
			seed:  "foo.com",
			limit: 1,
			expected: []DomainInfo{
				barDomain,
			},
		},
	}

	for _, test := range tests {
		if test.omittest {
			continue
		}
		dinfos, err := store.ListDomains(DQ{
			Seed:  test.seed,
			Limit: test.limit,
		})

		if err != nil {
			t.Errorf("ListDomains direct error %v", err)
			continue
		}

		if len(dinfos) != len(test.expected) {
			t.Errorf("ListDomains for tag '%s' length mismatch: got %d, expected %d",
				test.tag, len(dinfos), len(test.expected))
			continue
		}

		for i := range dinfos {
			got := dinfos[i]
			exp := test.expected[i]
			if got.Domain != exp.Domain {
				t.Errorf("ListDomains for tag %q domain mismatch: got %v, expected %v", test.tag, got.Domain, exp.Domain)
			}
			if got.NumberLinksTotal != exp.NumberLinksTotal {
				t.Errorf("ListDomains with domain '%s' for tag '%s' NumberLinksTotal mismatch got %v, expected %v",
					got.Domain, test.tag, got.NumberLinksTotal, exp.NumberLinksTotal)
			}
			if got.NumberLinksQueued != exp.NumberLinksQueued {
				t.Errorf("ListDomains with domain '%s' for tag '%s' NumberLinksQueued mismatch got %v, expected %v",
					got.Domain, test.tag, got.NumberLinksQueued, exp.NumberLinksQueued)
			}
			if got.NumberLinksUncrawled != exp.NumberLinksUncrawled {
				t.Errorf("ListDomains with domain '%s' for tag '%s' NumberLinksUncrawled mismatch got %v, expected %v",
					got.Domain, test.tag, got.NumberLinksUncrawled, exp.NumberLinksUncrawled)
			}
			if !timeClose(got.ClaimTime, exp.ClaimTime) {
				t.Errorf("ListDomains with domain '%s' for tag '%s' ClaimTime mismatch got %v, expected %v", got.Domain,
					test.tag, got.ClaimTime, exp.ClaimTime)
			}
			if got.ClaimToken != exp.ClaimToken {
				t.Errorf("ListDomains with domain '%s' for tag '%s' ClaimToken mismatch got %v, expected %v", got.Domain,
					test.tag, got.ClaimToken, exp.ClaimToken)
			}
			if got.ExcludeReason != exp.ExcludeReason {
				t.Errorf("ListDomains with domain '%s' for tag '%s' ExcludeReason mismatch got %v, expected %v",
					got.Domain, test.tag, got.ExcludeReason, exp.ExcludeReason)
			}
		}
	}
	store.Close()
}

func TestFindDomain(t *testing.T) {
	store := getModelTestDatastore(t)

	tests := []findTest{
		findTest{
			tag:      "Basic",
			domain:   "test.com",
			expected: &testDomain,
		},

		findTest{
			tag:      "Basic 2",
			domain:   "foo.com",
			expected: &fooDomain,
		},

		findTest{
			tag:      "Nil return",
			domain:   "notgoingtobethere.com",
			expected: nil,
		},
	}

	for _, test := range tests {
		dinfoPtr, err := store.FindDomain(test.domain)
		if err != nil {
			t.Errorf("FindDomain for tag %s direct error %v", test.tag, err)
			continue
		}
		expPtr := test.expected

		if dinfoPtr == nil && expPtr != nil {
			t.Errorf("FindDomain %s got nil return, expected non-nil return", test.tag)
			continue
		} else if dinfoPtr != nil && expPtr == nil {
			t.Errorf("FindDomain %s got non-nil return, expected nil return", test.tag)
		} else if dinfoPtr == nil && expPtr == nil {
			// everything is cool. Expected nil pointers and got em
			continue
		}

		got := *dinfoPtr
		exp := *expPtr
		if got.Domain != exp.Domain {
			t.Errorf("FindDomain %s Domain mismatch got %v, expected %v", test.tag, got.Domain, exp.Domain)
		}
		if got.NumberLinksTotal != exp.NumberLinksTotal {
			t.Errorf("FindDomain %s NumberLinksTotal mismatch got %v, expected %v", test.tag, got.NumberLinksTotal, exp.NumberLinksTotal)
		}
		if got.NumberLinksQueued != exp.NumberLinksQueued {
			t.Errorf("FindDomain %s NumberLinksQueued mismatch got %v, expected %v", test.tag, got.NumberLinksQueued, exp.NumberLinksQueued)
		}
		if got.NumberLinksUncrawled != exp.NumberLinksUncrawled {
			t.Errorf("FindDomain with domain '%s' for tag '%s' NumberLinksUncrawled mismatch got %v, expected %v", got.Domain, test.tag, got.NumberLinksUncrawled, exp.NumberLinksUncrawled)
		}
		if !timeClose(got.ClaimTime, exp.ClaimTime) {
			t.Errorf("FindDomain %s ClaimTime mismatch got %v, expected %v", test.tag, got.ClaimTime, exp.ClaimTime)
		}
		if got.ClaimToken != exp.ClaimToken {
			t.Errorf("FindDomain %s ClaimToken mismatch got %v, expected %v", test.tag, got.ClaimToken, exp.ClaimToken)
		}
		if got.ExcludeReason != exp.ExcludeReason {
			t.Errorf("FindDomain %s ExcludeReason mismatch got %v, expected %v", test.tag, got.ExcludeReason, exp.ExcludeReason)
		}
	}

	store.Close()
}

func TestListWorkingDomains(t *testing.T) {
	store := getModelTestDatastore(t)

	tests := []domainTest{
		domainTest{
			tag:   "Basic Pull",
			limit: LIM,
			expected: []DomainInfo{
				bazDomain,
				filterDomain,
				testDomain,
			},
		},

		domainTest{
			tag:   "Limit Pull",
			limit: 1,
			expected: []DomainInfo{
				bazDomain,
			},
		},

		domainTest{
			tag:   "Seeded Pull",
			seed:  "baz.com",
			limit: LIM,
			expected: []DomainInfo{
				filterDomain,
				testDomain,
			},
		},
	}

	for _, test := range tests {
		dinfos, err := store.ListDomains(DQ{
			Seed:    test.seed,
			Limit:   test.limit,
			Working: true,
		})
		if err != nil {
			t.Errorf("ListWorkingDomains for tag %s direct error %v", test.tag, err)
			continue
		}
		if len(dinfos) != len(test.expected) {
			t.Errorf("ListWorkingDomains length mismatch for tag %s: got %d, expected %d", test.tag, len(dinfos), len(test.expected))
			continue
		}
		for i := range dinfos {
			got := dinfos[i]
			exp := test.expected[i]
			if got.Domain != exp.Domain {
				t.Errorf("ListWorkingDomains %s Domain mismatch got %v, expected %v", test.tag, got.Domain, exp.Domain)
			}
			if got.NumberLinksTotal != exp.NumberLinksTotal {
				t.Errorf("ListWorkingDomains %s NumberLinksTotal mismatch got %v, expected %v", test.tag, got.NumberLinksTotal, exp.NumberLinksTotal)
			}
			if got.NumberLinksQueued != exp.NumberLinksQueued {
				t.Errorf("ListWorkingDomains %s NumberLinksQueued mismatch got %v, expected %v", test.tag, got.NumberLinksQueued, exp.NumberLinksQueued)
			}
			if got.NumberLinksUncrawled != exp.NumberLinksUncrawled {
				t.Errorf("ListWorkingDomains with domain '%s' for tag '%s' NumberLinksUncrawled mismatch got %v, expected %v", got.Domain, test.tag, got.NumberLinksUncrawled, exp.NumberLinksUncrawled)
			}
			if !timeClose(got.ClaimTime, exp.ClaimTime) {
				t.Errorf("ListWorkingDomains %s ClaimTime mismatch got %v, expected %v", test.tag, got.ClaimTime, exp.ClaimTime)
			}
			if got.ClaimToken != exp.ClaimToken {
				t.Errorf("ListWorkingDomains %s ClaimToken mismatch got %v, expected %v", test.tag, got.ClaimToken, exp.ClaimToken)
			}
			if got.ExcludeReason != exp.ExcludeReason {
				t.Errorf("ListWorkingDomains %s ExcludeReason mismatch got %v, expected %v", test.tag, got.ExcludeReason, exp.ExcludeReason)
			}
		}
	}
	store.Close()
}

func TestListLinks(t *testing.T) {
	store := getModelTestDatastore(t)

	tests := []linkTest{
		linkTest{
			tag:      "Basic Pull",
			domain:   "test.com",
			limit:    LIM,
			expected: testComLinkOrder,
		},

		linkTest{
			tag:    "foo pull",
			domain: "foo.com",
			limit:  LIM,
			expected: []walker.LinkInfo{
				walker.LinkInfo{
					URL:            helpers.Parse("http://sub.foo.com/page1.html"),
					Status:         200,
					Error:          "",
					RobotsExcluded: false,
					CrawlTime:      fooTime,
				},

				walker.LinkInfo{
					URL:            helpers.Parse("http://sub.foo.com/page2.html"),
					Status:         200,
					Error:          "",
					RobotsExcluded: false,
					CrawlTime:      fooTime,
				},
			},
		},

		linkTest{
			tag:      "bar pull",
			domain:   "bar.com",
			limit:    LIM,
			expected: []walker.LinkInfo{},
		},

		linkTest{
			tag:      "seeded pull",
			domain:   "test.com",
			seedURL:  testComLinkOrder[len(testComLinkOrder)/2-1].URL,
			limit:    LIM,
			expected: testComLinkOrder[len(testComLinkOrder)/2:],
		},

		linkTest{
			tag:      "seeded pull with limit",
			domain:   "test.com",
			seedURL:  testComLinkOrder[len(testComLinkOrder)/2-1].URL,
			limit:    1,
			expected: testComLinkOrder[len(testComLinkOrder)/2 : len(testComLinkOrder)/2+1],
		},
	}

	// run the tests
	for _, test := range tests {
		if test.omittest {
			continue
		}
		linfos, err := store.ListLinks(test.domain, LQ{Seed: test.seedURL, Limit: test.limit})
		if err != nil {
			t.Errorf("ListLinks for tag %s direct error %v", test.tag, err)
			continue
		}
		if len(linfos) != len(test.expected) {
			t.Errorf("ListLinks for tag %s length mismatch got %d, expected %d", test.tag, len(linfos), len(test.expected))
			continue
		}
		for i := range linfos {
			got := linfos[i]
			exp := test.expected[i]
			if got.URL.String() != exp.URL.String() {
				t.Errorf("ListLinks %s URL mismatch got %v, expected %v", test.tag, got.URL, exp.URL)
			}
			if got.Status != exp.Status {
				t.Errorf("ListLinks %s Status mismatch got %v, expected %v", test.tag, got.Status, exp.Status)
			}
			if got.Error != exp.Error {
				t.Errorf("ListLinks %s Error mismatch got %v, expected %v", test.tag, got.Error, exp.Error)
			}
			if got.RobotsExcluded != exp.RobotsExcluded {
				t.Errorf("ListLinks %s RobotsExcluded mismatch got %v, expected %v", test.tag, got.RobotsExcluded, exp.RobotsExcluded)
			}
			if !timeClose(got.CrawlTime, exp.CrawlTime) {
				t.Errorf("ListLinks %s CrawlTime mismatch got %v, expected %v", test.tag, got.CrawlTime, exp.CrawlTime)
			}
		}
	}

	store.Close()
}

func TestListLinkHistorical(t *testing.T) {
	store := getModelTestDatastore(t)

	// If we add pagination for ListLinkHistorical back in, we add these tests as well
	tests := []linkTest{
		linkTest{
			tag:      "full read",
			histURL:  helpers.Parse("http://sub.baz.com/page1.html"),
			limit:    LIM,
			expected: bazLinkHistoryOrder,
		},

		//linkTest{
		//	tag:      "limit",
		//	histURL:  "http://sub.baz.com/page1.html",
		//	limit:    4,
		//	expected: bazLinkHistoryOrder[:4],
		//},

		//linkTest{
		//	tag:      "seed",
		//	histURL:  "http://sub.baz.com/page1.html",
		//	seed:     4,
		//	limit:    LIM,
		//	expected: bazLinkHistoryOrder[4:],
		//},

		//linkTest{
		//	tag:      "seed & limit",
		//	histURL:  "http://sub.baz.com/page1.html",
		//	seed:     1,
		//	limit:    2,
		//	expected: bazLinkHistoryOrder[1:3],
		//},
	}

	// run the tests
	for _, test := range tests {
		if test.omittest {
			continue
		}
		linfos, err := store.ListLinkHistorical(test.histURL)
		if err != nil {
			t.Errorf("ListLinkHistorical for tag %s direct error %v", test.tag, err)
			continue
		}
		//if nextSeed != test.seed+len(linfos) {
		//	t.Errorf("ListLinkHistorical for tag %s bad nextSeed got %d, expected %d", test.tag, nextSeed, test.seed+len(linfos))
		//	continue
		//}
		if len(linfos) != len(test.expected) {
			t.Errorf("ListLinkHistorical for tag %s length mismatch got %d, expected %d", test.tag, len(linfos), len(test.expected))
			continue
		}
		for i := range linfos {
			got := linfos[i]
			exp := test.expected[i]
			if got.URL.String() != exp.URL.String() {
				t.Errorf("ListLinkHistorical %s URL mismatch got %v, expected %v", test.tag, got.URL, exp.URL)
			}
			if got.Status != exp.Status {
				t.Errorf("ListLinkHistorical %s Status mismatch got %v, expected %v", test.tag, got.Status, exp.Status)
			}
			if got.Error != exp.Error {
				t.Errorf("ListLinkHistorical %s Error mismatch got %v, expected %v", test.tag, got.Error, exp.Error)
			}
			if got.RobotsExcluded != exp.RobotsExcluded {
				t.Errorf("ListLinkHistorical %s RobotsExcluded mismatch got %v, expected %v", test.tag, got.RobotsExcluded, exp.RobotsExcluded)
			}
			if !timeClose(got.CrawlTime, exp.CrawlTime) {
				t.Errorf("ListLinkHistorical %s CrawlTime mismatch got %v, expected %v", test.tag, got.CrawlTime, exp.CrawlTime)
			}
		}
	}
}

func TestInsertLinks(t *testing.T) {
	store := getModelTestDatastore(t)

	tests := []insertTest{
		insertTest{
			updated: []updatedInDb{
				updatedInDb{
					link:   "http://sub.niffler1.com/page1.html",
					domain: "niffler1.com",
				},
			},
		},

		insertTest{
			updated: []updatedInDb{
				updatedInDb{
					link:   "http://sub.niffler2.com/page1.html",
					domain: "niffler2.com",
				},

				updatedInDb{
					link:   "http://sub.niffler2.com/page2.html",
					domain: "niffler2.com",
				},

				updatedInDb{
					link:   "http://sub.niffler2.com/page3.html",
					domain: "niffler2.com",
				},
			},
		},

		insertTest{
			updated: []updatedInDb{
				updatedInDb{
					link:   "http://sub.niffler3.com/page1.html",
					domain: "niffler3.com",
				},

				updatedInDb{
					link:   "http://sub.niffler4.com/page1.html",
					domain: "niffler4.com",
				},

				updatedInDb{
					link:   "http://sub.niffler5.com/page1.html",
					domain: "niffler5.com",
				},
			},
		},
	}

	// run the tests
	for _, test := range tests {
		if test.omittest {
			continue
		}

		expect := map[string][]string{}
		toadd := []string{}
		for _, u := range test.updated {
			toadd = append(toadd, u.link)
			expect[u.domain] = append(expect[u.domain], u.link)
		}

		errList := store.InsertLinks(toadd, "")
		if len(errList) != 0 {
			t.Errorf("InsertLinks for tag %s direct error %v", test.tag, errList)
			continue
		}

		allDomains := []string{}
		for domain, exp := range expect {
			linfos, err := store.ListLinks(domain, LQ{Limit: LIM})
			if err != nil {
				t.Errorf("InsertLinks:ListLinks for tag %s direct error %v", test.tag, err)
			}
			gotHash := map[string]bool{}
			for _, linfo := range linfos {
				gotHash[linfo.URL.String()] = true
			}

			for _, e := range exp {
				if !gotHash[e] {
					t.Errorf("InsertLinks:ListLinks for tag %s failed to find link %v", test.tag, e)
				}
			}

			allDomains = append(allDomains, domain)
		}

		dinfos, err := store.ListDomains(DQ{Limit: LIM})
		if err != nil {
			t.Errorf("InsertLinks:ListDomains for tag %s direct error %v", test.tag, err)
		}

		gotHash := map[string]bool{}
		for _, d := range dinfos {
			gotHash[d.Domain] = true
		}

		for _, d := range allDomains {
			if !gotHash[d] {
				t.Errorf("InsertLinks:ListDomains for tag %s failed to find domain %v", test.tag, d)
			}
		}
	}

}

func TestInsertExcludedLinks(t *testing.T) {
	store := getModelTestDatastore(t)

	tests := []insertTest{
		insertTest{
			updated: []updatedInDb{
				updatedInDb{
					link:                "http://excluded.com/page1.html",
					domain:              "excluded.com",
					excludeDomainReason: "Because I said so",
				},
			},
		},
	}

	// FYI: by iterating twice, we follow two different code paths through
	// addDomainIfNew in model.go
	for iterate := 0; iterate < 2; iterate++ {
		for _, test := range tests {
			if test.omittest {
				continue
			}

			added := test.updated[0]
			toadd := []string{added.link}
			errList := store.InsertLinks(toadd, added.excludeDomainReason)
			if len(errList) != 0 {
				t.Errorf("InsertExcludedLinks for tag %s direct error %v", test.tag, errList)
				continue
			}

			dinfo, err := store.FindDomain(added.domain)
			if err != nil {
				t.Errorf("InsertExcludedLinks:FindDomain for tag %s direct error %v", test.tag, err)
			} else if dinfo == nil {
				t.Errorf("InsertExcludedLinks:FindDomain for tag %s didn't find domain %s", test.tag, added.domain)
			} else if dinfo.ExcludeReason != added.excludeDomainReason {
				t.Errorf("InsertExcludedLinks:FindDomain for tag %s ExcludeReason mismatch: got %q, expected %q",
					test.tag, dinfo.ExcludeReason, added.excludeDomainReason)
			}
		}
	}
}

func TestCloseToLimitBug(t *testing.T) {
	store := getModelTestDatastore(t)
	tests := []linkTest{
		linkTest{
			domain:   "baz.com",
			tag:      "bug exposed with limit 1",
			limit:    1,
			expected: []walker.LinkInfo{bazLinkHistoryOrder[len(bazLinkHistoryOrder)-1]},
		},
	}

	// run the tests
	for _, test := range tests {
		if test.omittest {
			continue
		}
		linfos, err := store.ListLinks(test.domain, LQ{Seed: test.seedURL, Limit: test.limit})
		if err != nil {
			t.Errorf("ListLinks for tag %s direct error %v", test.tag, err)
			continue
		}
		if len(linfos) != len(test.expected) {
			t.Errorf("ListLinks for tag %s length mismatch got %d, expected %d", test.tag, len(linfos), len(test.expected))
			continue
		}
		for i := range linfos {
			got := linfos[i]
			exp := test.expected[i]
			if got.URL.String() != exp.URL.String() {
				t.Errorf("TestCloseToLimitBug %s URL mismatch got %v, expected %v", test.tag, got.URL, exp.URL)
			}
			if got.Status != exp.Status {
				t.Errorf("TestCloseToLimitBug %s Status mismatch got %v, expected %v", test.tag, got.Status, exp.Status)
			}
			if got.Error != exp.Error {
				t.Errorf("TestCloseToLimitBug %s Error mismatch got %v, expected %v", test.tag, got.Error, exp.Error)
			}
			if got.RobotsExcluded != exp.RobotsExcluded {
				t.Errorf("TestCloseToLimitBug %s RobotsExcluded mismatch got %v, expected %v", test.tag, got.RobotsExcluded, exp.RobotsExcluded)
			}
			if !timeClose(got.CrawlTime, exp.CrawlTime) {
				t.Errorf("TestCloseToLimitBug %s CrawlTime mismatch got %v, expected %v", test.tag, got.CrawlTime, exp.CrawlTime)
			}
		}
	}
}

func TestFilterRegex(t *testing.T) {
	filterURLs := []string{
		"http://filter.com/1/2/A",
		"http://filter.com/1111/2222/A",
		"http://filter.com/1111/2222/B",

		"http://filter.com/aaa.html",
		"https://filter.com/bbb.html",
		"http://filter.com/ccc/ddd.html",
		"http://subd.filter.com/aaa.html",
	}

	// This function composes LinkInfo array from the urls in filterURLs. The
	// index argument corresponds to  which element of filterURLs to include
	// in the LinkInfo list.
	pickFiltered := func(index ...int) []walker.LinkInfo {
		var r []walker.LinkInfo
		for _, i := range index {
			if i >= len(filterURLs) || i < 0 {
				panic("INTERNAL ERROR")
			}
			r = append(r, walker.LinkInfo{
				URL: helpers.Parse(filterURLs[i]),
			})
		}
		return r
	}

	store := getModelTestDatastore(t)
	tests := []linkTest{
		linkTest{
			domain:      "filter.com",
			tag:         "NoFilter",
			limit:       LIM,
			filterRegex: "",
			expected:    pickFiltered(0, 1, 2, 3, 4, 5, 6),
		},

		linkTest{
			domain:      "filter.com",
			tag:         "PathFilter",
			limit:       LIM,
			filterRegex: `/ccc/.*\.html$`,
			expected:    pickFiltered(5),
		},

		linkTest{
			domain:      "filter.com",
			tag:         "PathFilter2",
			limit:       LIM,
			filterRegex: `aaa\.html$`,
			expected:    pickFiltered(3, 6),
		},

		linkTest{
			domain:      "filter.com",
			tag:         "SubdomainFilter",
			limit:       LIM,
			filterRegex: `subd.filter.com`,
			expected:    pickFiltered(6),
		},

		linkTest{
			domain:      "filter.com",
			tag:         "ProtocolFilter",
			limit:       LIM,
			filterRegex: `^https`,
			expected:    pickFiltered(4),
		},

		linkTest{
			domain:      "filter.com",
			tag:         "HonorLimit",
			limit:       1,
			filterRegex: `aaa\.html$`,
			expected:    pickFiltered(3),
		},

		linkTest{
			domain:      "filter.com",
			tag:         "DeepPathFilter",
			limit:       LIM,
			filterRegex: `/[^/]+/[^/]+/A$`,
			expected:    pickFiltered(0, 1),
		},

		linkTest{
			domain:      "filter.com",
			tag:         "DeepPathFilter2",
			limit:       LIM,
			filterRegex: `/[^/]{4}/[^/]{4}/.$`,
			expected:    pickFiltered(1, 2),
		},
	}

	// run the tests
	for _, test := range tests {
		if test.omittest {
			continue
		}
		linfos, err := store.ListLinks(test.domain, LQ{
			Limit:       test.limit,
			FilterRegex: test.filterRegex,
		})
		if err != nil {
			t.Errorf("ListLinks for tag %s direct error %v", test.tag, err)
			continue
		}

		if len(linfos) != len(test.expected) {
			t.Errorf("ListLinks for tag %s length mismatch got %d, expected %d", test.tag, len(linfos), len(test.expected))
			continue
		}
		for i := range linfos {
			got := linfos[i]
			exp := test.expected[i]
			if got.URL.String() != exp.URL.String() {
				t.Errorf("ListLinks %s URL mismatch got %v, expected %v", test.tag, got.URL, exp.URL)
			}
		}
	}

	store.Close()

}
