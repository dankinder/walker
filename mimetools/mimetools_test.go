package mimetools

import (
	"testing"
)

func testMatch(t *testing.T, target string, matchInputs ...string) {
	mm, err := NewMatcher(matchInputs)
	if err != nil {
		t.Error(err)
	}

	q, err := mm.Match(target)
	if err != nil {
		t.Error(err)
	}
	if !q {
		t.Errorf("Failed match against target: %v, inputs: %v", target, matchInputs)
	}
}

func testMatchFail(t *testing.T, target string, matchInputs ...string) {
	mm, err := NewMatcher(matchInputs)
	if err != nil {
		t.Error(err)
	}

	q, err := mm.Match(target)
	if q {
		t.Errorf("Errantly matched against target: %v, inputs: %v", target, matchInputs)
	}
}

func TestGoodMatch(t *testing.T) {
	testMatch(t, "text/html", "text/html")
	testMatch(t, "text/html", "text/*")
	testMatch(t, "text/html", "*/html")

	testMatch(t, "text/html", "foo/bar", "baz/bing", "text/html")
	testMatch(t, "foo/bar", "foo/bar", "baz/bing", "text/html")
	testMatch(t, "baz/bing", "foo/bar", "baz/bing", "text/html")

	testMatch(t, "text/html", "foo/bar", "baz/bing", "text/*")
	testMatch(t, "foo/bar", "*/bar", "baz/bing", "text/html")
	testMatch(t, "baz/bing", "foo/bar", "baz/*", "text/html")

	testMatch(t, "text/html", "*/*")
	testMatch(t, "foo/bar", "*/*")
	testMatch(t, "baz/bing", "*/*")

	testMatch(t, "text/html; q=1; level=5", "foo/bar; niffler=7", "text/html; q=0.4; level=7")
}

func TestFailMatch(t *testing.T) {
	testMatchFail(t, "foo/bar", "text/html")
	testMatchFail(t, "foo/bar", "text/*")
	testMatchFail(t, "foo/bar", "*/html")
	testMatchFail(t, "", "text/html")
	testMatchFail(t, "", "text/*")
	testMatchFail(t, "", "*/html")
}
