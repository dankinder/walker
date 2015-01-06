package test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	"github.com/gorilla/mux"
	"github.com/iParadigms/walker/cassandra"
	"github.com/iParadigms/walker/console"
	"github.com/iParadigms/walker/helpers"
)

//
// Fixtures
//
func spoofData() {
	if console.DS != nil {
		console.DS.Close()
		console.DS = nil
	}

	helpers.LoadTestConfig("test-walker.yaml")

	console.SpoofData()
	ds, err := cassandra.NewDatastore()
	if err != nil {
		panic(fmt.Errorf("Failed to start data source: %v", err))
	}
	console.DS = ds

	console.BuildRender()
}

//
// Call a controller and return a Document
//
func callController(url string, body string, urlPattern string, controller func(w http.ResponseWriter, req *http.Request)) (*goquery.Document, string, int) {
	a, b, c, _ := callControllerFull(url, body, urlPattern, controller)
	return a, b, c
}

func callControllerFull(url string, body string, urlPattern string, controller func(w http.ResponseWriter, req *http.Request)) (*goquery.Document, string, int,
	map[string]interface{}) {
	//
	// Set your method based on the body input
	//
	var bodyBuff io.Reader
	method := "GET"
	ct := ""
	if body != "" {
		bodyBuff = bytes.NewBufferString(body)
		method = "POST"
		ct = "application/x-www-form-urlencoded; param=value"
	}
	req, err := http.NewRequest(method, url, bodyBuff)
	if err != nil {
		panic(err)
	}
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}

	//
	// Need to build a router to get the mux.Vars to work.
	//
	router := mux.NewRouter()
	router.HandleFunc(urlPattern, controller)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	status := w.Code
	output := w.Body.String()
	mp := map[string]interface{}{
		"headers": w.Header(),
	}

	if strings.TrimSpace(output) == "" {
		return nil, output, status, mp
	}

	outputReader := strings.NewReader(output)
	doc, err := goquery.NewDocumentFromReader(outputReader)
	if err != nil {
		panic(err)
	}
	return doc, output, status, mp

}

//
// The tests
//
func TestLayout(t *testing.T) {
	spoofData()
	doc, body, status := callController("http://localhost:3000/", "", "/", console.HomeController)
	if status != http.StatusOK {
		t.Errorf("TestHome bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	// Make sure the main menu is there
	mainLinks := map[string]string{
		"/list":        "List",
		"/find":        "Find Domains",
		"/findLinks":   "Find Links",
		"/add":         "Add",
		"/filterLinks": "Filter Links",
	}
	sub := doc.Find("nav ul li a")
	if sub.Size() != len(mainLinks) {
		t.Fatalf("[nav ul li a] Bad size: got %d, expected %d", sub.Size(), len(mainLinks))
		return
	}
	sub.Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Errorf("[nav ul li a] Failed to find href attribute in main menu list")
			return
		}
		text := strings.TrimSpace(sel.Text())
		found, foundOk := mainLinks[link]

		if !foundOk {
			t.Errorf("[nav ul li a] Failed to find link '%s' in menu list", link)
			return
		}

		if found != text {
			t.Errorf("[nav ul li a] Text mismatch for %s: got '%s', expected '%s'", link, text, found)
			return
		}

		delete(mainLinks, link)
	})
	for k, v := range mainLinks {
		t.Errorf("[nav ul li a] Unfound link %v (%v)", k, v)
	}

	cssLinks := map[string]bool{
		"/css/bootstrap.css": true,
		"/css/custom.css":    true,
	}
	sub = doc.Find("head link")
	if sub.Size() <= 0 {
		t.Errorf("[head link] Failed to find any links")
	}
	sub.Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Errorf("[head link] Failed to find href")
			return
		}
		if !cssLinks[link] {
			t.Errorf("[head link] Failed to find link %s", link)
			return
		}

		delete(cssLinks, link)
	})
	for k, v := range mainLinks {
		t.Errorf("[head link] Unfound link %v (%v)", k, v)
	}

	jsLinks := map[string]bool{
		"/js/jquery-2.1.1.js": true,
		"/js/bootstrap.js":    true,
	}
	sub = doc.Find("head script")
	if sub.Size() != len(jsLinks) {
		t.Fatalf("[head script] Size mismatch: got %d, expected %d", sub.Size(), len(jsLinks))
	}
	sub.Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("src")
		if !linkOk {
			t.Errorf("[head script] Failed to find src")
			return
		}
		if !jsLinks[link] {
			t.Errorf("[head script] Failed to find link %s", link)
			return
		}

		delete(jsLinks, link)
	})
	for k, v := range mainLinks {
		t.Errorf("[head script] Unfound link %v (%v)", k, v)
	}
}

func TestHome(t *testing.T) {
	spoofData()
	doc, body, status := callController("http://localhost:3000/", "", "/", console.HomeController)
	if status != http.StatusOK {
		t.Errorf("TestHome bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	sub := doc.Find(".container p")
	if sub.Size() != 1 {
		t.Errorf("[.container p] Expected 1 paragraph, found %d", sub.Size())
	}
	sub.Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		exp := "Walker Console"
		if !strings.Contains(text, exp) {
			t.Errorf("[.container p] <p> mismatch: got '%s', expected to contain '%s'", text, exp)
		}
	})
}

func TestListDomainsWeb(t *testing.T) {
	spoofData()
	doc, body, status := callController("http://localhost:3000/list", "", "/list", console.ListDomainsController)
	if status != http.StatusOK {
		t.Errorf("TestListDomains bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}
	header := []string{
		"Domain",
		"Total Links",
		"Links Dispatched",
		"Excluded",
		"Last Claimed By Fetcher",
	}
	sub := doc.Find(".container table thead td")
	if sub.Size() != len(header) {
		t.Fatalf("[.container table thead td] Size mismatch got %d, expected %d", sub.Size(), len(header))
	}
	count := 0
	sub.Each(func(index int, sel *goquery.Selection) {

		text := strings.TrimSpace(sel.Text())
		if text != header[count] {
			t.Fatalf("[.container table thead td] Bad order got '%v' expected '%v'", text, header[count])
			return
		}

		count++
	})

	sub = doc.Find(".container table tbody tr td a")
	count = 0
	sub.Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Fatalf("[.container table tbody tr td a] Failed to find href")
			return
		}
		text := strings.TrimSpace(sel.Text())
		elink := "/links/" + text
		if elink != link {
			t.Fatalf("[.container table tbody tr td a] link mismatch got '%v' expected '%v'", link, elink)
			return
		}

		count++
	})

	minCount := 10
	if count < minCount {
		t.Fatalf("[.container table tbody tr td a] Count mismatch got %d, expected greater than %d ", count, minCount)
	}
}
func TestListDomainsSeeded(t *testing.T) {
	spoofData()
	doc, body, status := callController("http://localhost:3000/list/h5.com", "", "/list/{seed}", console.ListDomainsController)
	if status != http.StatusOK {
		t.Errorf("TestListDomains bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}
	sub := doc.Find(".container table tbody tr td a")
	if sub.Size() < 1 {
		t.Fatalf("[.container table tbody tr td a] Bad size expected > 0")
	}
	sub = sub.First()
	link, linkOk := sub.Attr("href")
	if !linkOk {
		t.Fatalf("[.container table tbody tr td a] Failed to find href")
		return
	}
	elink := "/links/x74.com"
	if elink != link {
		t.Fatalf("[.container table tbody tr td a] Link mismatch: got '%v' expected '%v'", link, elink)
		return
	}
}

func TestListLinksWeb(t *testing.T) {
	spoofData()
	doc, body, status := callController("http://localhost:3000/links/t1.com", "", "/links/{domain}", console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestListLinks bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	// Sanity check headers
	h2 := []string{
		"Domain information for t1.com",
		"Links for domain t1.com",
	}

	sub := doc.Find(".container h2")
	if sub.Size() != len(h2) {
		t.Fatalf("[.container h2] Size mismatch got %d, expected %d", sub.Size(), len(h2))
	}

	count := 0
	sub.Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != h2[count] {
			t.Fatalf("[.container h2] Text mismatch got '%s', expected '%s'", text, h2[count])
		}

		count++
	})

	// Nab the tables
	tables := doc.Find(".container table")
	if tables.Size() != 2 {
		t.Fatalf("[.container table] Bad size got %d, expected %d", tables.Size(), 2)
	}
	domainTable := tables.First()
	linksTable := tables.Last()

	//
	// Domain section
	//
	domainKeys := []string{
		"Domain",
		"Exclude Reason (if excluded)",
		"Last Claimed By Fetcher",
		"Current Fetcher Claim ID",
		"Total Unique Links",
		"Links Dispatched",
		"Unique Links Crawled",
		"Unique Links Not Yet Crawled",
		"Priority",
	}

	sub = domainTable.Find("tr > td:nth-child(1)")
	if sub.Size() != len(domainKeys) {
		t.Fatalf("[.container table tr > td:nth-child(1)] Size mismatch got %d, expected %d", sub.Size(), len(domainKeys))
	}
	count = 0
	sub.Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != domainKeys[count] {
			t.Fatalf("[.container table tr > td:nth-child(1)] Column key mismatch '%s', expected '%s'", text, domainKeys[count])
		}

		count++
	})

	secondColSize := domainTable.Find("tr > td:nth-child(2)").Size()
	if secondColSize != len(domainKeys) {
		t.Fatalf("[.container table tr > td:nth-child(2)] Second column mismatch got %d, expected %d", secondColSize, len(domainKeys))
	}

	thirdColSize := domainTable.Find("tr > td:nth-child(3)").Size()
	if thirdColSize != len(domainKeys) {
		t.Fatalf("[.container table tr > td:nth-child(3)] Wrong size got %d, expected %d", thirdColSize, 0)
	}

	//
	// Links
	//
	linksColHeaders := []string{
		"Link",
		"Status",
		"Error?",
		"Excluded by robots.txt?",
		"Last Fetch",
	}

	sub = linksTable.Find("thead th")
	if sub.Size() != len(linksColHeaders) {
		t.Fatalf("[.container table thead th] Size mismatch got %d, expected %d", sub.Size(), len(linksColHeaders))
	}
	count = 0
	sub.Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != linksColHeaders[count] {
			t.Fatalf("[.container table thead th] Col name mismatch got '%s', expected '%s'", text, linksColHeaders[count])
		}

		count++
	})

	linkRows := linksTable.Find("tbody tr td a")
	if linkRows.Size() < 5 {
		t.Fatalf("[.container table tbody tr td a] Row count mismatch got %d, expected %d", linkRows.Size(), 5)
	}
	linkRows.Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Fatalf("[.container table tbody tr td a] Failed to find href")
			return
		}
		if !strings.HasPrefix(link, "/historical") {
			t.Fatalf("[.container table tbody tr td a] Failed to find prefix /historical in href (%s)", link)
			return
		}
	})

	//
	// Buttons
	//
	buttons := []string{
		"Previous",
		"Next",
	}
	sub = doc.Find(".container .row > a").FilterFunction(func(index int, sel *goquery.Selection) bool {
		return sel.HasClass("btn")
	})
	if sub.Size() != len(buttons) {
		t.Fatalf("[.container .row > a <buttons>] Size mismatch got %d, expected %d", sub.Size(), len(buttons))
	}
	count = 0
	sub.Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != buttons[count] {
			t.Fatalf("[.container .row > a <buttons>] Button text mismatch got '%s', expected '%s'", text, buttons[count])
		}

		if text == "Previous" {
			if !sel.HasClass("disabled") {
				t.Fatalf("[.container .row > a <buttons>] Failed button disable for %s", text)
			}
		} else {
			if sel.HasClass("disabled") {
				t.Fatalf("[.container .row > a <buttons>] Failed button undisabled for %s", text)
			}
		}

		count++
	})

	sub = doc.Find(".container .console-table a").FilterFunction(func(index int, sel *goquery.Selection) bool {
		return sel.HasClass("btn")
	})

	if sub.Size() != 1 {
		t.Fatalf("[.container .console-table a] Size mismatch got %d, expected %d", sub.Size(), 1)
	}

	if strings.TrimSpace(sub.Text()) != "Exclude" {
		t.Errorf("[.container .console-table a] Text mismatch got %q, expected %q",
			strings.TrimSpace(sub.Text()), "Exclude")
	}

	// Priority button
	sub = doc.Find("button").FilterFunction(func(index int, sel *goquery.Selection) bool {
		return strings.Contains(sel.Text(), "Set Priority")
	})

	if sub.Size() != 1 {
		t.Fatalf("[button with text 'Set Priority'] Failed to find Priority button")
	}

	// Page length button
	sub = doc.Find("button").FilterFunction(func(index int, sel *goquery.Selection) bool {
		return strings.Contains(sel.Text(), "Set Page Length")
	})

	if sub.Size() != 1 {
		t.Fatalf("[button with text 'Set Page Length'] Failed to find Priority button")
	}
}

func TestListLinksSecondPage(t *testing.T) {
	spoofData()

	//
	// First find the second page link
	//
	doc, body, status := callController("http://localhost:3000/links/t1.com", "", "/links/{domain}", console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestListLinksSecondPage bad status code got %d, expected %d", status, http.StatusOK)
		//body = ""
		t.Log(body)
		t.FailNow()
	}

	nextButton := doc.Find(".container a").FilterFunction(func(index int, sel *goquery.Selection) bool {
		return sel.HasClass("btn") && strings.Contains(sel.Text(), "Next")
	})
	if nextButton.Size() != 1 {
		t.Fatalf("[.container a <buttons>] Failed to find next button")
		return
	}
	nextPagePath, nextPageOk := nextButton.Attr("href")
	if !nextPageOk {
		t.Fatalf("[.container a <buttons>] Failed to find next button href")
		return
	}

	//
	// OK now click on the next button
	//
	nextPage := "http://localhost:3000" + nextPagePath
	doc, body, status = callController(nextPage, "", "/links/{domain}/{seedURL}", console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestListLinks bad status code got %d, expected %d, link %v",
			status, http.StatusOK, nextPage)
		t.Log(body)
		t.FailNow()
	}

	// Nab the tables
	tables := doc.Find(".container table")
	if tables.Size() != 1 {
		t.Fatalf("[.container table] Bad size got %d, expected %d", tables.Size(), 1)
	}
	linksTable := tables.Last()

	//
	// Links Table
	//
	linksColHeaders := []string{
		"Link",
		"Status",
		"Error?",
		"Excluded by robots.txt?",
		"Last Fetch",
	}
	sub := linksTable.Find("thead th")
	count := 0
	sub.Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != linksColHeaders[count] {
			t.Fatalf("[.container table thead th] Col name mismatch got '%s', expected '%s'", text, linksColHeaders[count])
		}

		count++
	})

	linkRows := linksTable.Find("tbody tr td a")
	if linkRows.Size() < 5 {
		t.Fatalf("[.container table tbody tr td a] not enough rows")
	}
	linkRows.Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Fatalf("[.container table tbody tr td a] Failed to find href")
			return
		}
		if !strings.HasPrefix(link, "/historical") {
			t.Fatalf("[.container table tbody tr td a] Failed to find prefix /historical in href (%s)", link)
			return
		}
	})

	//
	// Buttons
	//
	buttons := []string{
		"Previous",
		"Next",
	}
	sub = doc.Find(".container a").FilterFunction(func(index int, sel *goquery.Selection) bool {
		return sel.HasClass("btn")
	})
	if sub.Size() != len(buttons) {
		t.Fatalf("[.container a <buttons>] Size mismatch got %d, expected %d", sub.Size(), len(buttons))
	}

	count = 0
	sub.Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != buttons[count] {
			t.Fatalf("[.container a <buttons>] Failed text got '%s', expected '%s'", text, buttons[count])
		}

		if text == "Previous" {
			if sel.HasClass("disabled") {
				t.Fatalf("[.container a <buttons>] Failed disabled for %s", text)
			}
		} else {
			if !sel.HasClass("disabled") {
				t.Fatalf("[.container a <buttons>] Failed disabled for %s", text)
			}
		}

		count++
	})

	// Page length button
	sub = doc.Find("button").FilterFunction(func(index int, sel *goquery.Selection) bool {
		return strings.Contains(sel.Text(), "Set Page Length")
	})

	if sub.Size() != 1 {
		t.Fatalf("[button with text 'Set Page Length'] Failed to find Priority button")
	}
}

func TestListHistorical(t *testing.T) {
	spoofData()

	//
	// First get the domain page, and find the historical link
	//
	doc, body, status := callController("http://localhost:3000/links/h1.com", "", "/links/{domain}", console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestListHistorical bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	// Nab the tables
	tables := doc.Find(".container table")
	if tables.Size() != 2 {
		t.Fatalf("[.container table] Bad size got %d, expected %d", tables.Size(), 2)
	}
	linksTable := tables.Last()

	linkRows := linksTable.Find("tbody tr td a")
	if linkRows.Size() < 1 {
		t.Fatalf("[.container table tbody tr td a] Not enough rows")
	}
	historicalLink := ""
	linkRows.First().Each(func(index int, sel *goquery.Selection) {
		link, linkOk := sel.Attr("href")
		if !linkOk {
			t.Fatalf("[.container table tbody tr td a] Failed to find href")
			return
		}
		if !strings.HasPrefix(link, "/historical") {
			t.Fatalf("[.container table tbody tr td a] Failed to find prefix /historical in href (%s)", link)
			return
		}

		historicalLink = link
	})

	//
	// Now lets fetch the historical page
	//
	nextPage := "http://localhost:3000" + historicalLink
	doc, body, status = callController(nextPage, "", "/historical/{url}", console.LinksHistoricalController)
	if status != http.StatusOK {
		t.Fatalf("TestListHistorical bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	doc.Find(".container h2").First().Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if !strings.HasPrefix(text, "History for Link") {
			t.Fatalf("[.container h2] Bad prefix got %s, expected 'History for Link'", text)
		}
	})

	tables = doc.Find(".container table")
	if tables.Size() != 1 {
		t.Fatalf("[.container table] Bad size got %d, expected %d", tables.Size(), 1)
		return
	}

	colHeaders := []string{
		"Fetched On",
		"Robots Excluded",
		"Status",
		"Error",
	}
	count := 0
	tables.Find("thead th").Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != colHeaders[count] {
			t.Fatalf("[.container table thead th] Column header got '%s', expected '%s'", text, colHeaders[count])
		}

		count++
	})

	nrows := tables.Find("tbody tr").Size()
	if nrows < 5 {
		t.Fatalf("[.container table tbody tr] Size mismatch got %d, expected > %d", nrows, 5)
	}

	tables.Find("tbody tr").Each(func(index int, sel *goquery.Selection) {
		ncol := sel.Children().Size()
		if ncol != 4 {
			t.Fatalf("[.container table tbody tr] Wrong column count got %d, expected %d", ncol, 4)
		}
	})
}

func TestFindDomains(t *testing.T) {
	spoofData()

	//
	// First get the domain page, and find the historical link
	//
	doc, body, status := callController("http://localhost:3000/find", "", "/find", console.FindDomainController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	doc.Find(".container h2").Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != "Find domains" {
			t.Errorf("[.container h2] H2 mismatch got '%s', expected '%s'", text, "Find domains")
		}
	})

	form := doc.Find(".container form")
	textarea := form.Find("textarea")
	input := form.Find("input")
	if textarea.Size() != 1 {
		t.Fatalf("[.container form textarea] Count mismatch got %d, expected 1", textarea.Size())
	}
	placeholder, placeholderOk := textarea.Attr("placeholder")
	if !placeholderOk {
		t.Fatalf("[.container form textarea] Failed to find placeholder attribute")
	} else {
		e := "Enter domains: one per line"
		if placeholder != e {
			t.Fatalf("[.container form textarea] Bad placeholder attribute got %s, expected %s", placeholder, e)
		}
	}

	if input.Size() != 1 {
		t.Fatalf("[.container form input] Count mismatch got %d, expected 1", input.Size())
	}

	typ, typOk := input.Attr("type")
	if !typOk {
		t.Fatalf("[.container form input] Failed to find type attribute")
	} else if typ != "submit" {
		t.Fatalf("[.container form input] Type mismatch got '%s', expected 'submit'", typ)
	}

	//
	// Lets submit a find request
	//
	rawBody := "targets=t1.com%0D%0At2.com%0D%0At3.com"
	doc, body, status = callController("http://localhost:3000/find", rawBody, "/find", console.FindDomainController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	expLinks := []string{
		"/links/t1.com",
		"/links/t2.com",
		"/links/t3.com",
	}
	links := doc.Find(".container table tbody tr a")
	if links.Size() != 3 {
		t.Fatalf("[.container table tbody tr a] Expected 3 elements")
	}

	count := 0
	links.Each(func(index int, sel *goquery.Selection) {
		href, hrefOk := sel.Attr("href")
		if !hrefOk {
			t.Fatalf("[.container table tbody tr a] Failed to find href attribute")
		} else if expLinks[count] != href {
			t.Fatalf("[.container table tbody tr a] href link mismatch, got '%v', expected '%v'", href, expLinks[count])
		}

		count++
	})

	//
	// Lets submit a bad find request
	//
	rawBody = "targets=NOTTHERE1.com%0D%0ANOTTHERE2.com%0D%0ANOTTHERE3.com"
	doc, body, status = callController("http://localhost:3000/find", rawBody, "/find", console.FindDomainController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	expMessages := []string{
		"Failed to find domain NOTTHERE1.com",
		"Failed to find domain NOTTHERE2.com",
		"Failed to find domain NOTTHERE3.com",
	}
	messages := doc.Find(".container > ul li")
	if messages.Size() != 4 {
		t.Fatalf("[.container ul li] Message mismatch got %d, expected 4", messages.Size())
	}
	count = 0
	messages.Each(func(index int, sel *goquery.Selection) {
		if count >= 3 {
			return
		}
		text := strings.TrimSpace(sel.Text())
		if expMessages[count] != text {
			t.Fatalf("[.container ul li] Text mismatch got '%v', expected '%v'", text, expMessages[count])
		}
		count++
	})

	//
	// Lets submit an empty request
	//
	rawBody = "  "
	doc, body, status = callController("http://localhost:3000/find", rawBody, "/find", console.FindDomainController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	sub := doc.Find(".info-li")
	if sub.Size() < 1 {
		t.Fatalf("[.info-li] Expected to find element")
	}
	text := strings.TrimSpace(sub.Text())
	etext := "Failed to specify any targets"
	if text != etext {
		t.Fatalf("[.info-li] Error message mismatch: got '%s', expected '%s'", text, etext)
	}

	//
	// Variation on an empty request
	//
	rawBody = "targets=%0D%0A"
	doc, body, status = callController("http://localhost:3000/find", rawBody, "/find", console.FindDomainController)
	if status != http.StatusOK {
		t.Errorf("TestFindDomains bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	sub = doc.Find(".info-li")
	if sub.Size() < 1 {
		t.Fatalf("[.info-li] Expected to find element")
	}
	text = strings.TrimSpace(sub.Text())
	etext = "Failed to specify any targets"
	if text != etext {
		t.Fatalf("[.info-li] Error message mismatch: got '%s', expected '%s'", text, etext)
	}
}

func TestFindLinks(t *testing.T) {
	spoofData()

	//
	// First get the domain page, and find the historical link
	//
	doc, body, status := callController("http://localhost:3000/findLinks", "", "/findLinks", console.FindLinksController)
	if status != http.StatusOK {
		t.Errorf("TestFindLinks bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	doc.Find(".container h2").Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != "Find Links" {
			t.Errorf("[.container h2] Header mismatch got '%s', expected '%s'", text, "Find Links")
		}
	})

	form := doc.Find(".container form")
	textarea := form.Find("textarea")
	input := form.Find("input")
	if textarea.Size() != 1 {
		t.Fatalf("[.container form textarea] Size mismatch got %d, expected 1", textarea.Size())
	}

	placeholder, placeholderOk := textarea.Attr("placeholder")
	if !placeholderOk {
		t.Fatalf("[.container form textarea] Failed to find placeholder attribute")
	} else {
		e := "Enter links: one per line"
		if placeholder != e {
			t.Fatalf("[.container form textarea] Bad placeholder attribute got %s, expected %s", placeholder, e)
		}
	}

	if input.Size() != 1 {
		t.Fatalf("[.container form input] Size mismatch got %d, expected 1", input.Size())
	}
	typ, typOk := input.Attr("type")
	if !typOk {
		t.Fatalf("[.container form input] Failed to find type attribute")
	} else if typ != "submit" {
		t.Fatalf("[.container form input] Type mismatch got '%s', expected 'submit'", typ)
	}

	//
	// Submit a find request
	//
	rawBody := "links=http://link.t1.com/page0.html%0D%0A"
	doc, body, status = callController("http://localhost:3000/findLinks", rawBody, "/findLinks", console.FindLinksController)
	if status != http.StatusOK {
		t.Errorf("TestFindLinks bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	res := doc.Find(".container table tbody tr td")
	if res.Size() != 5 {
		t.Errorf("[.container table tbody tr td] Size mismatch got %d, expected 1", res.Size())
	}
	res.First().Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		e := "http://link.t1.com/page0.html"
		if text != e {
			t.Fatalf("[.container table tbody tr td] Mismatched link got '%v', expected '%v'", text, e)
		}
	})
}

func TestAddLinks(t *testing.T) {
	spoofData()

	//
	// First get the domain page, and find the historical link
	//
	doc, body, status := callController("http://localhost:3000/add", "", "/add", console.AddLinkIndexController)
	if status != http.StatusOK {
		t.Errorf("TestAddLinks bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	doc.Find(".container h2").Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		e := "Add Links"
		if text != e {
			t.Fatalf("[.container h2] Header mismatch got '%s', expected '%s'", text, e)
		}
	})

	form := doc.Find(".container form")
	textarea := form.Find("textarea")
	if textarea.Size() != 1 {
		t.Fatalf("[.container form textarea] Size mismatch got %d, expected 1", textarea.Size())
	}
	placeholder, placeholderOk := textarea.Attr("placeholder")
	if !placeholderOk {
		t.Fatalf("[.container form textarea] Failed to find placeholder attribute")
	} else {
		e := "Enter links: one per line"
		if placeholder != e {
			t.Fatalf("[.container form textarea] Bad placeholder attribute got %s, expected %s", placeholder, e)
		}
	}

	input := form.Find("input[type=submit]")
	if input.Size() != 1 {
		t.Fatalf("[.container form input[type=submit]] Size mismatch got %d, expected 1", input.Size())
	}
	typ, typOk := input.Attr("type")
	if !typOk {
		t.Fatalf("[.container form input[type=submit]] Failed to find type attribute")
	} else if typ != "submit" {
		t.Fatalf("[.container form input[type=submit]] Bad type got %s, expected submit", typ)
	}

	input = form.Find("input[type=checkbox]")
	if input.Size() != 1 {
		t.Fatalf("[.container form input[type=checkbox]] Size mismatch got %d, expected 1", input.Size())
	}
	typ, typOk = input.Attr("type")
	if !typOk {
		t.Fatalf("[.container form input[type=checkbox]] Failed to find type attribute")
	} else if typ != "checkbox" {
		t.Fatalf("[.container form input[type=checkbox]] Bad type got %s, expected checkbox", typ)
	}

	//
	// Submit an add request
	//
	randDomain := fmt.Sprintf("rand%d.com", rand.Uint32())
	randLink := fmt.Sprintf("http://sub.%s.com/page0.html", randDomain)
	rawBody := "links=" + randLink + "%0D%0A"
	doc, body, status = callController("http://localhost:3000/add", rawBody, "/add", console.AddLinkIndexController)
	if status != http.StatusOK {
		t.Errorf("TestAddLinks bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}
	res := doc.Find(".container > ul li")
	if res.Size() != 1 {
		t.Fatalf("[.container > ul li] Size mismatch got %d, expected 1", res.Size())
	}
	res.Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		e := "All links added"
		if text != e {
			t.Fatalf("[.container ul li] Mismatched text got '%v', expected '%v'", text, e)
		}
	})

	//
	// Find the link
	//
	doc, body, status = callController("http://localhost:3000/findLinks", rawBody, "/findLinks", console.FindLinksController)
	if status != http.StatusOK {
		t.Errorf("TestAddLinks bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.Fatalf("")
	}
	res = doc.Find(".container table tbody tr td")
	if res.Size() != 5 {
		t.Errorf("[.container table tbody tr td] Bad size got %d, expected 5", res.Size())
	}
	res.First().Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != randLink {
			t.Fatalf("[.container table tbody tr td] Mismatched text got '%v', expected '%v'", text, randLink)
		}
	})

}

func TestFilterLinks(t *testing.T) {
	spoofData()

	//
	// First get the filter page and verify it looks correct
	//
	doc, body, status := callController("http://localhost:3000/filterLinks", "", "/filterLinks", console.FilterLinksController)
	if status != http.StatusOK {
		t.Errorf("TestFilterLinks bad status code got %d, expected %d", status, http.StatusOK)
		body = ""
		t.Log(body)
		t.FailNow()
	}

	expectedLabels := map[string]bool{
		"Domain":     true,
		"Link Regex": true,
	}
	doc.Find(".container .row h3").Each(func(index int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if !expectedLabels[text] {
			t.Errorf("[.container .row h3] Label mismatch got %q, but didn't find in expectedLabels", text)

		}
	})

	sub := doc.Find(".container .box input[type=text]")
	if sub.Size() != 2 {
		t.Errorf("[.container .box input[type=text]] Size mismatch got %d, expected 2", sub.Size())
	}

	sub = doc.Find(".container input[type=submit]")
	if sub.Size() != 1 {
		t.Errorf("[.container input[type=submit]] Size mismatch got %d, expected 1", sub.Size())

	}

	// Now test the post submittion
	rawBody := "domain=t1.com&regex=html"
	var emp map[string]interface{}
	doc, body, status, emp = callControllerFull("http://localhost:3000/filterLinks", rawBody, "/filterLinks", console.FilterLinksController)
	if status != http.StatusSeeOther {
		t.Errorf("TestFilterLinks bad status code got %d, expected %d", status, http.StatusSeeOther)
		body = ""
		t.Log(body)
		t.FailNow()
	}

	headersRaw, headersOk := emp["headers"]
	if !headersOk {
		t.Fatalf("TestFilterLinks failed to find headers in reply")
	}

	headers, headersOk := headersRaw.(http.Header)
	if !headersOk {
		t.Fatalf("TestFilterLinks failed to cast headers in reply")
	}

	loc, locOk := headers["Location"]
	if !locOk {
		t.Fatalf("TestFilterLinks failed to find Location in headers")
	}

	expectedLoc := "/links/t1.com?filterRegex=NB2G23A="
	if loc[0] != expectedLoc {
		t.Fatalf("TestFilterLinks redirect url got %q, but expected %q", loc[0], expectedLoc)
	}

	// Now test that regex is displayed correctly on links page
	expected := "Links for domain t1.com (filtered by /.*/)"
	doc, body, status = callController("http://localhost:3000/links/t1.com?filterRegex=FYVA====", "",
		"/links/{domain}", console.LinksController)
	sub = doc.Find(".container .row .col-xs-6 h2")
	if sub.Size() != 1 {
		t.Fatalf("[.container .row .col-xs-6 h2] Failed to find header message")
	} else if !strings.Contains(sub.Text(), expected) {
		t.Fatalf("[.container .row .col-xs-6 h2] Expected to find %q, but actually found %q", expected, sub.Text())
	}

}

func TestChangePriority(t *testing.T) {
	spoofData()

	// priority function will read the priority value out of the links page
	var doc *goquery.Document
	priority := func() int {
		sub := doc.Find(".container .row table tr").FilterFunction(func(index int, sel *goquery.Selection) bool {
			title := sel.Find("td").First().Text()
			return strings.Contains(title, "Priority")
		})

		if sub.Size() < 1 {
			t.Fatalf("Failed to find Priority row")
		}
		valueSel := sub.Find("td:nth-child(2)")
		if valueSel.Size() < 1 {
			t.Fatalf("Failed to find Priority value")
		}
		value, err := strconv.Atoi(strings.TrimSpace(valueSel.Text()))
		if err != nil {
			t.Fatalf("Failed to transform priority value: %v", err)
		}

		return value
	}

	//
	// First get the links page and verify it looks correct
	//
	doc, body, status := callController("http://localhost:3000/links/t1.com", "", "/links/{domain}",
		console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestChangePriority bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	prio := priority()
	if prio != 0 {
		t.Errorf("Expected initial priority to be zero, but found %d", prio)
	}

	//
	// Now set a new priority
	//
	doc, body, status = callController("http://localhost:3000/changePriority/t1.com/-4", "",
		"/changePriority/{domain}/{priority}",
		console.ChangePriorityController)
	if status != http.StatusFound {
		t.Errorf("TestChangePriority bad status code got %d, expected %d", status, http.StatusFound)
		t.FailNow()
	}

	//
	// Now verify that the priority got changed
	//
	doc, body, status = callController("http://localhost:3000/links/t1.com", "", "/links/{domain}",
		console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestChangePriority bad status code got %d, expected %d", status, http.StatusOK)
		t.FailNow()
	}

	prio = priority()
	if prio != -4 {
		t.Errorf("Expected modified priority to be -4, but found %d", prio)
	}
}

func TestSetPageLength(t *testing.T) {
	spoofData()

	var doc *goquery.Document
	// pageLen will return the number of results on the /links or /list pages
	pageLen := func() int {
		sub := doc.Find(".container .row .console-table tbody tr")
		return sub.Size()
	}

	// encodeLen will return a url encoded string with ln set for pageWindowLength
	encodeLen := func(ln int) string {
		return fmt.Sprintf("prevlist=&pushprev=&pageWindowLength=%d", ln)
	}

	//
	// Set /list length to 10
	//
	doc, body, status := callController("http://localhost:3000/list", encodeLen(10), "/list",
		console.ListDomainsController)
	if status != http.StatusOK {
		t.Errorf("TestChangePriority bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	if pageLen() != 10 {
		t.Fatalf("Failed to set /list page length to 10, found length %d", pageLen())
	}

	//
	// Set /list length to 25
	//
	doc, body, status = callController("http://localhost:3000/list", encodeLen(25), "/list",
		console.ListDomainsController)
	if status != http.StatusOK {
		t.Errorf("TestChangePriority bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	if pageLen() != 25 {
		t.Fatalf("Failed to set /list page length to 25, found length %d", pageLen())
	}

	//
	// Set /links length to 10
	//
	linksLink := "http://localhost:3000/links/t1.com/NB2HI4B2F4XWY2LONMXHIMJOMNXW2L3QMFTWKMJSFZUHI3LM"
	doc, body, status = callController(linksLink, encodeLen(10), "/links/{domain}/{seedURL}",
		console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestChangePriority bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	if pageLen() != 10 {
		t.Fatalf("Failed to set /links page length to 10, but found length %d", pageLen())
	}

	//
	// Set /links length > 10
	//
	doc, body, status = callController(linksLink, encodeLen(25), "/links/{domain}/{seedURL}",
		console.LinksController)
	if status != http.StatusOK {
		t.Errorf("TestChangePriority bad status code got %d, expected %d", status, http.StatusOK)
		t.Log(body)
		t.FailNow()
	}

	if pageLen() <= 10 {
		t.Fatalf("Failed to set /links page length greater than 10")
	}

}
