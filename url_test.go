package walker

import (
	"testing"
	"time"
)

func TestURLParsing(t *testing.T) {
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

func TestURLEqual(t *testing.T) {
	tests := []struct {
		tag    string
		expect bool
		link1  *URL
		link2  *URL
	}{
		{"basic equal", true,
			MustParse("http://www.test.com/"), MustParse("http://www.test.com/")},
		{"basic not equal", false,
			MustParse("http://www.test.com/stuff"), MustParse("http://www.test.com/")},
		{"query param equal", true,
			MustParse("http://www.test.com/?a=b"), MustParse("http://www.test.com/?a=b")},
		{"query param not equal", false,
			MustParse("http://www.test.com/?a=1"), MustParse("http://www.test.com/?a=2")},
		{"protocol not equal", false,
			MustParse("http://www.test.com"), MustParse("https://www.test.com")},
		{"time equal", true,
			&URL{
				URL:         MustParse("http://www.test.com").URL,
				LastCrawled: NotYetCrawled,
			},
			&URL{
				URL:         MustParse("http://www.test.com").URL,
				LastCrawled: NotYetCrawled,
			},
		},
		{"time not equal", false,
			&URL{
				URL:         MustParse("http://www.test.com").URL,
				LastCrawled: NotYetCrawled,
			},
			&URL{
				URL:         MustParse("http://www.test.com").URL,
				LastCrawled: time.Now(),
			},
		},
	}

	for _, tst := range tests {
		result := tst.link1.Equal(tst.link2)
		if result != tst.expect {
			t.Errorf("Tag: %v\nExpected Equal() to be %v but was %v for %v and %v",
				tst.tag, tst.expect, result, tst.link1, tst.link2)
		}
	}
}

func TestURLEqualIgnoreLastCrawled(t *testing.T) {
	tests := []struct {
		tag    string
		expect bool
		link1  *URL
		link2  *URL
	}{
		{"time equal", true,
			&URL{
				URL:         MustParse("http://www.test.com").URL,
				LastCrawled: NotYetCrawled,
			},
			&URL{
				URL:         MustParse("http://www.test.com").URL,
				LastCrawled: NotYetCrawled,
			},
		},
		{"time not equal", true,
			&URL{
				URL:         MustParse("http://www.test.com").URL,
				LastCrawled: NotYetCrawled,
			},
			&URL{
				URL:         MustParse("http://www.test.com").URL,
				LastCrawled: time.Now(),
			},
		},
	}

	for _, tst := range tests {
		result := tst.link1.EqualIgnoreLastCrawled(tst.link2)
		if result != tst.expect {
			t.Errorf("Tag: %v\nExpected Equal() to be %v but was %v for %v and %v",
				tst.tag, tst.expect, result, tst.link1, tst.link2)
		}
	}
}
