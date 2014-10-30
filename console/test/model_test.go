package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/cassandra"
	"github.com/iParadigms/walker/console"
)

//
// Config alteration right up front
//
func modifyConfigDataSource() {
	source := "test-walker.yaml"
	err := walker.ReadConfigFile(source)
	if err != nil {
		panic(err)
	}
}

//
// Some global data
//
var fooTime = time.Now().AddDate(0, 0, -1)
var testTime = time.Now().AddDate(0, 0, -2)
var bazUuid, _ = gocql.RandomUUID()
var testComLinkOrder []console.LinkInfo
var testComLinkHash = map[string]console.LinkInfo{
	"http://test.com/page1.html": console.LinkInfo{
		Url:            "http://test.com/page1.html",
		Status:         200,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"http://test.com/page2.html": console.LinkInfo{
		Url:            "http://test.com/page2.html",
		Status:         200,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"http://test.com/page3.html": console.LinkInfo{
		Url:            "http://test.com/page3.html",
		Status:         404,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"http://test.com/page4.html": console.LinkInfo{
		Url:            "http://test.com/page4.html",
		Status:         200,
		Error:          "An Error",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"http://test.com/page5.html": console.LinkInfo{
		Url:            "http://test.com/page5.html",
		Status:         200,
		Error:          "",
		RobotsExcluded: true,
		CrawlTime:      walker.NotYetCrawled,
	},

	"http://sub.test.com/page6.html": console.LinkInfo{
		Url:            "http://sub.test.com/page6.html",
		Status:         200,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"https://sub.test.com/page7.html": console.LinkInfo{
		Url:            "https://sub.test.com/page7.html",
		Status:         200,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},

	"https://sub.test.com/page8.html": console.LinkInfo{
		Url:            "https://sub.test.com/page8.html",
		Status:         200,
		Error:          "",
		RobotsExcluded: false,
		CrawlTime:      walker.NotYetCrawled,
	},
}

var bazLinkHistoryOrder []console.LinkInfo

var bazLinkHistoryInit = []console.LinkInfo{
	console.LinkInfo{
		Url:       "http://sub.baz.com/page1.html",
		Status:    200,
		CrawlTime: walker.NotYetCrawled,
	},
	console.LinkInfo{
		Url:       "http://sub.baz.com/page1.html",
		Status:    200,
		CrawlTime: time.Now().AddDate(0, 0, -1),
	},
	console.LinkInfo{
		Url:       "http://sub.baz.com/page1.html",
		Status:    200,
		CrawlTime: time.Now().AddDate(0, 0, -2),
	},
	console.LinkInfo{
		Url:       "http://sub.baz.com/page1.html",
		Status:    200,
		CrawlTime: time.Now().AddDate(0, 0, -3),
	},
	console.LinkInfo{
		Url:       "http://sub.baz.com/page1.html",
		Status:    200,
		CrawlTime: time.Now().AddDate(0, 0, -4),
	},
	console.LinkInfo{
		Url:       "http://sub.baz.com/page1.html",
		Status:    200,
		CrawlTime: time.Now().AddDate(0, 0, -5),
	},
}

var bazSeed string

type findTest struct {
	omittest bool
	tag      string
	domain   string
	expected *console.DomainInfo
}

type domainTest struct {
	omittest bool
	tag      string
	seed     string
	limit    int
	expected []console.DomainInfo
}

type linkTest struct {
	omittest bool
	tag      string
	domain   string
	histUrl  string
	seed     int
	seedUrl  string
	limit    int
	expected []console.LinkInfo
}

const LIM = 50

const EPSILON_SECONDS = 1

func timeClose(l time.Time, r time.Time) bool {
	delta := l.Unix() - r.Unix()
	if delta < 0 {
		delta = -delta
	}
	return delta <= EPSILON_SECONDS
}

//Shared Domain Information
var bazDomain = console.DomainInfo{
	Domain:            "baz.com",
	NumberLinksTotal:  1,
	NumberLinksQueued: 1,
	TimeQueued:        testTime,
	UuidOfQueued:      bazUuid,
}

var fooDomain = console.DomainInfo{
	Domain:            "foo.com",
	NumberLinksTotal:  2,
	NumberLinksQueued: 0,
	TimeQueued:        walker.NotYetCrawled,
}

var barDomain = console.DomainInfo{
	Domain:            "bar.com",
	NumberLinksTotal:  0,
	NumberLinksQueued: 0,
	TimeQueued:        walker.NotYetCrawled,
}

var testDomain = console.DomainInfo{
	Domain:            "test.com",
	NumberLinksTotal:  8,
	NumberLinksQueued: 2,
	TimeQueued:        testTime,
	UuidOfQueued:      gocql.UUID{},
}

type updatedInDb struct {
	link   string
	domain string
	path   string
}

type insertTest struct {
	omittest bool
	tag      string
	updated  []updatedInDb
}

//
// Fixture generation
//
var initdb sync.Once

func getDs(t *testing.T) *console.CqlModel {
	modifyConfigDataSource()

	initdb.Do(func() {
		cluster := gocql.NewCluster(walker.Config.Cassandra.Hosts...)
		db, err := cluster.CreateSession()
		if err != nil {
			panic(err)
		}

		// Just want to make sure no one makes a mistake with this code
		if walker.Config.Cassandra.Keyspace == "walker" {
			panic("Not allowed to spoof the walker keyspace")
		}
		err = db.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", walker.Config.Cassandra.Keyspace)).Exec()
		if err != nil {
			panic(fmt.Errorf("Failed to drop %s keyspace: %v", walker.Config.Cassandra.Keyspace, err))
		}
		err = cassandra.CreateSchema()
		if err != nil {
			t.Fatalf(err.Error())
		}

		db.Close()
	})

	ds, err := console.NewCqlModel()
	if err != nil {
		panic(err)
	}
	db := ds.Db

	//
	// Clear out the tables first
	//
	tables := []string{"links", "segments", "domain_info"}
	for _, table := range tables {
		err := db.Query(fmt.Sprintf(`TRUNCATE %v`, table)).Exec()
		if err != nil {
			t.Fatalf("Failed to truncate table %v: %v", table, err)
		}
	}

	//
	// Insert some data
	//
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

		db.Query(insertDomainToCrawl, "baz.com", bazUuid, testTime),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[0].CrawlTime, 200, "", false),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[1].CrawlTime, 200, "", false),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[2].CrawlTime, 200, "", false),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[3].CrawlTime, 200, "", false),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[4].CrawlTime, 200, "", false),
		db.Query(insertLink, "baz.com", "sub", "/page1.html", "http", bazLinkHistoryInit[5].CrawlTime, 200, "", false),

		db.Query(insertSegment, "baz.com", "sub", "page1.html", "http"),
	}
	for _, q := range queries {
		err := q.Exec()
		if err != nil {
			t.Fatalf("Failed to insert test data: %v\nQuery: %v", err, q)
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
	err = itr.Close()
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
	var beforeBazComLink *walker.URL = nil
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

	return ds
}

//
// THE TESTS
//
func TestListDomains(t *testing.T) {
	store := getDs(t)

	tests := []domainTest{
		domainTest{
			tag:   "Basic Pull",
			seed:  console.DontSeedDomain,
			limit: LIM,
			expected: []console.DomainInfo{
				bazDomain,
				fooDomain,
				barDomain,
				testDomain,
			},
		},

		domainTest{
			tag:   "Limit Pull",
			seed:  console.DontSeedDomain,
			limit: 1,
			expected: []console.DomainInfo{
				bazDomain,
			},
		},

		domainTest{
			tag:   "Seeded Pull",
			seed:  "foo.com",
			limit: LIM,
			expected: []console.DomainInfo{
				barDomain,
				testDomain,
			},
		},

		domainTest{
			tag:   "Seeded & Limited Pull",
			seed:  "foo.com",
			limit: 1,
			expected: []console.DomainInfo{
				barDomain,
			},
		},
	}

	for _, test := range tests {
		if test.omittest {
			continue
		}
		dinfos, err := store.ListDomains(test.seed, test.limit)
		if err != nil {
			t.Errorf("ListDomains direct error %v", err)
			continue
		}

		if len(dinfos) != len(test.expected) {
			t.Errorf("ListDomains length mismatch %v: got %d, expected %d", test.tag, len(dinfos), len(test.expected))
			continue
		}

		for i := range dinfos {
			got := dinfos[i]
			exp := test.expected[i]
			if got.NumberLinksTotal != exp.NumberLinksTotal {
				t.Errorf("ListDomains with domain '%s' for tag '%s' NumberLinksTotal mismatch got %v, expected %v", got.Domain, test.tag, got.NumberLinksTotal, exp.NumberLinksTotal)
			}
			if got.NumberLinksQueued != exp.NumberLinksQueued {
				t.Errorf("ListDomains with domain '%s' for tag '%s' NumberLinksQueued mismatch got %v, expected %v", got.Domain, test.tag, got.NumberLinksQueued, exp.NumberLinksQueued)
			}
			if !timeClose(got.TimeQueued, exp.TimeQueued) {
				t.Errorf("ListDomains with domain '%s' for tag '%s' TimeQueued mismatch got %v, expected %v", got.Domain, test.tag, got.TimeQueued, exp.TimeQueued)
			}
			if got.UuidOfQueued != exp.UuidOfQueued {
				t.Errorf("ListDomains with domain '%s' for tag '%s' UuidOfQueued mismatch got %v, expected %v", got.Domain, test.tag, got.UuidOfQueued, exp.UuidOfQueued)
			}
			if got.ExcludeReason != exp.ExcludeReason {
				t.Errorf("ListDomains with domain '%s' for tag '%s' ExcludeReason mismatch got %v, expected %v", got.Domain, test.tag, got.ExcludeReason, exp.ExcludeReason)
			}
		}
	}
	store.Close()
}

func TestFindDomain(t *testing.T) {
	store := getDs(t)

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
		if !timeClose(got.TimeQueued, exp.TimeQueued) {
			t.Errorf("FindDomain %s TimeQueued mismatch got %v, expected %v", test.tag, got.TimeQueued, exp.TimeQueued)
		}
		if got.UuidOfQueued != exp.UuidOfQueued {
			t.Errorf("FindDomain %s UuidOfQueued mismatch got %v, expected %v", test.tag, got.UuidOfQueued, exp.UuidOfQueued)
		}
		if got.ExcludeReason != exp.ExcludeReason {
			t.Errorf("FindDomain %s ExcludeReason mismatch got %v, expected %v", test.tag, got.ExcludeReason, exp.ExcludeReason)
		}
	}

	store.Close()
}

func TestListWorkingDomains(t *testing.T) {
	store := getDs(t)

	tests := []domainTest{
		domainTest{
			tag:   "Basic Pull",
			seed:  console.DontSeedDomain,
			limit: LIM,
			expected: []console.DomainInfo{
				bazDomain,
				testDomain,
			},
		},

		domainTest{
			tag:   "Limit Pull",
			seed:  console.DontSeedDomain,
			limit: 1,
			expected: []console.DomainInfo{
				bazDomain,
			},
		},

		domainTest{
			tag:   "Seeded Pull",
			seed:  "baz.com",
			limit: LIM,
			expected: []console.DomainInfo{
				testDomain,
			},
		},
	}

	for _, test := range tests {
		dinfos, err := store.ListWorkingDomains(test.seed, test.limit)
		if err != nil {
			t.Errorf("ListWorkingDomains for tag %s direct error %v", test.tag, err)
			continue
		}
		if len(dinfos) != len(test.expected) {
			t.Errorf("ListWorkingDomains length mismatch: got %d, expected %d", len(dinfos), len(test.expected))
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
			if !timeClose(got.TimeQueued, exp.TimeQueued) {
				t.Errorf("ListWorkingDomains %s TimeQueued mismatch got %v, expected %v", test.tag, got.TimeQueued, exp.TimeQueued)
			}
			if got.UuidOfQueued != exp.UuidOfQueued {
				t.Errorf("ListWorkingDomains %s UuidOfQueued mismatch got %v, expected %v", test.tag, got.UuidOfQueued, exp.UuidOfQueued)
			}
			if got.ExcludeReason != exp.ExcludeReason {
				t.Errorf("ListWorkingDomains %s ExcludeReason mismatch got %v, expected %v", test.tag, got.ExcludeReason, exp.ExcludeReason)
			}
		}
	}
	store.Close()
}

func TestListLinks(t *testing.T) {
	store := getDs(t)

	tests := []linkTest{
		linkTest{
			tag:      "Basic Pull",
			domain:   "test.com",
			seedUrl:  console.DontSeedUrl,
			limit:    LIM,
			expected: testComLinkOrder,
		},

		linkTest{
			tag:     "foo pull",
			domain:  "foo.com",
			seedUrl: console.DontSeedUrl,
			limit:   LIM,
			expected: []console.LinkInfo{
				console.LinkInfo{
					Url:            "http://sub.foo.com/page1.html",
					Status:         200,
					Error:          "",
					RobotsExcluded: false,
					CrawlTime:      fooTime,
				},

				console.LinkInfo{
					Url:            "http://sub.foo.com/page2.html",
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
			seedUrl:  console.DontSeedUrl,
			seed:     console.DontSeedIndex,
			limit:    LIM,
			expected: []console.LinkInfo{},
		},

		linkTest{
			tag:      "seeded pull",
			domain:   "test.com",
			seedUrl:  testComLinkOrder[len(testComLinkOrder)/2-1].Url,
			limit:    LIM,
			expected: testComLinkOrder[len(testComLinkOrder)/2:],
		},

		linkTest{
			tag:      "seeded pull with limit",
			domain:   "test.com",
			seedUrl:  testComLinkOrder[len(testComLinkOrder)/2-1].Url,
			limit:    1,
			expected: testComLinkOrder[len(testComLinkOrder)/2 : len(testComLinkOrder)/2+1],
		},
	}

	// run the tests
	for _, test := range tests {
		if test.omittest {
			continue
		}
		linfos, err := store.ListLinks(test.domain, test.seedUrl, test.limit)
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
			if got.Url != exp.Url {
				t.Errorf("ListLinks %s Url mismatch got %v, expected %v", test.tag, got.Url, exp.Url)
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
	store := getDs(t)

	tests := []linkTest{
		linkTest{
			tag:      "full read",
			histUrl:  "http://sub.baz.com/page1.html",
			seed:     console.DontSeedIndex,
			limit:    LIM,
			expected: bazLinkHistoryOrder,
		},

		linkTest{
			tag:      "limit",
			histUrl:  "http://sub.baz.com/page1.html",
			seed:     console.DontSeedIndex,
			limit:    4,
			expected: bazLinkHistoryOrder[:4],
		},

		linkTest{
			tag:      "seed",
			histUrl:  "http://sub.baz.com/page1.html",
			seed:     4,
			limit:    LIM,
			expected: bazLinkHistoryOrder[4:],
		},

		linkTest{
			tag:      "seed & limit",
			histUrl:  "http://sub.baz.com/page1.html",
			seed:     1,
			limit:    2,
			expected: bazLinkHistoryOrder[1:3],
		},
	}

	// run the tests
	for _, test := range tests {
		if test.omittest {
			continue
		}
		linfos, nextSeed, err := store.ListLinkHistorical(test.histUrl, test.seed, test.limit)
		if err != nil {
			t.Errorf("ListLinkHistorical for tag %s direct error %v", test.tag, err)
			continue
		}
		if nextSeed != test.seed+len(linfos) {
			t.Errorf("ListLinkHistorical for tag %s bad nextSeed got %d, expected %d", test.tag, nextSeed, test.seed+len(linfos))
			continue
		}
		if len(linfos) != len(test.expected) {
			t.Errorf("ListLinkHistorical for tag %s length mismatch got %d, expected %d", test.tag, len(linfos), len(test.expected))
			continue
		}
		for i := range linfos {
			got := linfos[i]
			exp := test.expected[i]
			if got.Url != exp.Url {
				t.Errorf("ListLinkHistorical %s Url mismatch got %v, expected %v", test.tag, got.Url, exp.Url)
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
	store := getDs(t)

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

		errList := store.InsertLinks(toadd)
		if len(errList) != 0 {
			t.Errorf("InsertLinks for tag %s direct error %v", test.tag, errList)
			continue
		}

		allDomains := []string{}
		for domain, exp := range expect {
			linfos, err := store.ListLinks(domain, console.DontSeedUrl, LIM)
			if err != nil {
				t.Errorf("InsertLinks:ListLinks for tag %s direct error %v", test.tag, err)
			}
			gotHash := map[string]bool{}
			for _, linfo := range linfos {
				gotHash[linfo.Url] = true
			}

			for _, e := range exp {
				if !gotHash[e] {
					t.Errorf("InsertLinks:ListLinks for tag %s failed to find link %v", test.tag, e)
				}
			}

			allDomains = append(allDomains, domain)
		}

		dinfos, err := store.ListDomains(console.DontSeedDomain, LIM)
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

func TestCloseToLimitBug(t *testing.T) {
	store := getDs(t)
	tests := []linkTest{
		linkTest{
			domain:   "baz.com",
			tag:      "bug exposed with limit 1",
			seedUrl:  bazSeed,
			limit:    1,
			expected: []console.LinkInfo{bazLinkHistoryOrder[len(bazLinkHistoryOrder)-1]},
		},
	}

	// run the tests
	for _, test := range tests {
		if test.omittest {
			continue
		}
		linfos, err := store.ListLinks(test.domain, test.seedUrl, test.limit)
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
			if got.Url != exp.Url {
				t.Errorf("TestCloseToLimitBug %s Url mismatch got %v, expected %v", test.tag, got.Url, exp.Url)
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
