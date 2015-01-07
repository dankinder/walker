/*
	This file contains the web-facing handlers.
*/
package console

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"code.google.com/p/log4go"
	"github.com/gorilla/mux"
	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/cassandra"
)

var DS cassandra.ModelDatastore

type Route struct {
	Path       string
	Controller func(w http.ResponseWriter, req *http.Request)
}

// Simple aggregate datatype that holds both the link, and text of the given priority
type dropdownElement struct {
	Link string
	Text string
}

func Routes() []Route {
	return []Route{
		Route{Path: "/", Controller: HomeController},
		Route{Path: "/list", Controller: ListDomainsController},
		Route{Path: "/list/", Controller: ListDomainsController},
		Route{Path: "/list/{seed}", Controller: ListDomainsController},
		Route{Path: "/find", Controller: FindDomainController},
		Route{Path: "/find/", Controller: FindDomainController},
		Route{Path: "/add", Controller: AddLinkIndexController},
		Route{Path: "/add/", Controller: AddLinkIndexController},
		Route{Path: "/links/{domain}", Controller: LinksController},
		Route{Path: "/links/{domain}/{seedURL}", Controller: LinksController},
		Route{Path: "/historical/{url}", Controller: LinksHistoricalController},
		Route{Path: "/findLinks", Controller: FindLinksController},
		Route{Path: "/filterLinks", Controller: FilterLinksController},
		Route{Path: "/excludeToggle/{domain}/{direction}", Controller: ExcludeToggleController},
		Route{Path: "/changePriority/{domain}/{priority}", Controller: ChangePriorityController},
	}
}

func HomeController(w http.ResponseWriter, req *http.Request) {
	mp := map[string]interface{}{}
	Render.HTML(w, http.StatusOK, "home", mp)
	return
}

// The links and list templates have a hidden form that is used to track the list of previous links
// so that the previous button works correctly (see https://jira2.iparadigms.com/browse/TRN-134). The
// same form is used to allow the user to reset the window-length (i.e. number of results per page).
// The input arguments to the function are
//   (a) The http request with the hidden form.
//   (b) The Session pointer which holds client-side user data.
//   (c) The isLinks toggle which controls which session variable to store the new page dimensions into. When
//       isLinks is true the window is stored with Session.SetLinksPageWindowLength, otherwise it's stored with
//       Session.SetListPageWindowLength
// The return value of this function is
//   (a) the link that should be used for the Previous button href
//   (b) the encoded previous list to be inserted in the hidden-form on server dispatch.
//   (c) any errors that occur.
// It's also worth noting that, if the pageWindowLength field of the form is set, this method will
// update the session to reflect the new windowLength.
func processHiddenForm(req *http.Request, sess *Session, isLinks bool) (string, string, error) {
	// First grab prevlist
	var prevList string
	var err error
	prevlistArr, prevlistOk := req.Form["prevlist"]
	if prevlistOk && len(prevlistArr) > 0 {
		prevList, err = decode32(prevlistArr[0])
		if err != nil {
			return "", "", err
		}
	}

	// Now see if user requested page resize. If so save into session.
	isWindowResize := false
	pageWindowLengthArr, pageWindowLengthOk := req.Form["pageWindowLength"]
	if pageWindowLengthOk && len(pageWindowLengthArr) > 0 && len(pageWindowLengthArr[0]) > 0 {
		isWindowResize = true
		length, err := strconv.Atoi(pageWindowLengthArr[0])
		if err != nil {
			return "", "", err
		}

		foundP := false
		for _, p := range PageWindowLengthChoices {
			if p == length {
				foundP = true
				break
			}
		}
		if !foundP {
			return "", "", fmt.Errorf("PageWindowLength not found in PageWindowLengthChoices")
		}

		if isLinks {
			sess.SetLinksPageWindowLength(length)
		} else {
			sess.SetListPageWindowLength(length)
		}
		sess.Save()
		isWindowResize = true
	}

	// Detect if the user pushed the Previous button. Notice it's impossible for isPrev and isWindowResize to
	// be true at the same time.
	isPrev := false
	if !isWindowResize {
		pushprev, pushprevOk := req.Form["pushprev"]
		if pushprevOk && len(pushprev) > 0 && len(pushprev[0]) > 0 {
			isPrev = true
		}
	}

	//
	// Now set theLink and theList (see defn below) based on what kind of post this was. Note, upon exit
	// theList should contain a list of links visited, with the end element the current page. Conceptually,
	// then, theLink is always the second to the last element in the list. The logic below is more complicated than
	// that to handle the case when theList has 0 or 1 elements.
	//
	theLink := ""         // This variable will hold the link that should end up on the prev buttons href
	theList := []string{} // This variable holds the list of links already visited
	if prevList != "" {
		theList = strings.Split(prevList, ";")
	}

	end := len(theList) - 1
	if isWindowResize {
		// The previous list stays constant
		if end >= 1 {
			theLink = theList[end-1]
		} else {
			theLink = ""
		}
	} else if isPrev {
		// Pop the last element off the list, and set theLink to the (new) second to the last element
		if end >= 1 {
			if end == 1 {
				theLink = ""
			} else {
				theLink = theList[end-2]
			}
			theList = theList[:end]
		} else {
			theLink = ""
			theList = []string{}
		}
	} else {
		// Push current request onto the stack.
		if end >= 0 {
			theLink = theList[end]
		} else {
			theLink = ""
		}

		prefix := "/list"
		if isLinks {
			prefix = "/links"
		}

		if !(len(theList) == 0 && req.RequestURI == prefix) {
			theList = append(theList, strings.TrimPrefix(req.RequestURI, prefix))
		}
	}

	return theLink, encode32(strings.Join(theList, ";")), nil
}

func ListDomainsController(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	seed := vars["seed"]
	prevButtonClass := ""

	session, err := GetSession(w, req)
	if err != nil {
		replyServerError(w, fmt.Errorf("GetSession failed: %v", err))
		return
	}

	err = req.ParseForm()
	if err != nil {
		replyServerError(w, err)
		return
	}

	prevLink, prevList, err := processHiddenForm(req, session, false)
	if err != nil {
		replyServerError(w, fmt.Errorf("processHiddenForm failed: %v", err))
		return
	}

	query := cassandra.DQ{Limit: session.ListPageWindowLength()}
	if seed == "" {
		prevButtonClass = "disabled"
	} else {
		var err error
		s, err := url.QueryUnescape(seed)
		if err == nil {
			query.Seed = s
		}
	}

	dinfos, err := DS.ListDomains(query)
	if err != nil {
		err = fmt.Errorf("ListDomains failed: %v", err)
		replyServerError(w, err)
		return
	}

	nextLink := ""
	nextButtonClass := "disabled"
	if len(dinfos) == query.Limit {
		nextLink = url.QueryEscape(dinfos[len(dinfos)-1].Domain)
		nextButtonClass = ""
	}

	// set up page length dropdown
	pageLenDropdown := []dropdownElement{}
	for _, ln := range PageWindowLengthChoices {
		pageLenDropdown = append(pageLenDropdown, dropdownElement{
			Text: fmt.Sprintf("%d", ln),
		})
	}

	mp := map[string]interface{}{
		"PrevButtonClass": prevButtonClass,
		"NextButtonClass": nextButtonClass,
		"Domains":         dinfos,
		"Next":            nextLink,
		"Prev":            prevLink,
		"PrevList":        prevList,
		"PageLengthLinks": pageLenDropdown,
	}
	Render.HTML(w, http.StatusOK, "list", mp)
}

func FindDomainController(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		mp := map[string]interface{}{}
		Render.HTML(w, http.StatusOK, "find", mp)
		return
	}

	err := req.ParseForm()
	if err != nil {
		replyServerError(w, err)
		return
	}

	targetAll, ok := req.Form["targets"]
	if !ok || len(targetAll) < 1 {
		log4go.Error("Targets was not defined correctly %v", targetAll)
		mp := map[string]interface{}{
			"HasInfoMessage": true,
			"InfoMessage":    []string{"Failed to specify any targets"},
		}
		Render.HTML(w, http.StatusOK, "find", mp)
		return
	}

	rawLines := targetAll[0]
	lines := strings.Split(rawLines, "\n")
	targets := []string{}
	for i := range lines {
		t := strings.TrimSpace(lines[i])
		if t != "" {
			targets = append(targets, t)
		}
	}

	if len(targets) <= 0 {
		mp := map[string]interface{}{
			"HasInfoMessage": true,
			"InfoMessage":    []string{"Failed to specify any targets"},
		}
		Render.HTML(w, http.StatusOK, "find", mp)
		return
	}

	var dinfos []*cassandra.DomainInfo
	var errs []string
	var info []string
	for _, target := range targets {
		dinfo, err := DS.FindDomain(target)
		if err != nil {
			errs = append(errs, fmt.Sprintf("FindDomain failed: %v", err))
			continue
		}

		if dinfo == nil {
			info = append(info, fmt.Sprintf("Failed to find domain %s", target))
			continue
		}

		dinfos = append(dinfos, dinfo)
	}

	hasInfoMessage := len(info) > 0
	hasErrorMessage := len(errs) > 0

	if len(dinfos) == 0 {
		info = append(info, "Didn't find any links on previous try")
		hasInfoMessage = true
		mp := map[string]interface{}{
			"HasInfoMessage":  hasInfoMessage,
			"InfoMessage":     info,
			"HasErrorMessage": hasErrorMessage,
			"ErrorMessage":    errs,
		}
		Render.HTML(w, http.StatusOK, "find", mp)
	} else {
		mp := map[string]interface{}{
			"PrevButtonClass": "disabled",
			"NextButtonClass": "disabled",
			"Domains":         dinfos,
			"HasNext":         false,
			"HasInfoMessage":  hasInfoMessage,
			"InfoMessage":     info,
			"HasErrorMessage": hasErrorMessage,
			"ErrorMessage":    errs,
		}
		Render.HTML(w, http.StatusOK, "list", mp)
	}
}

// TODO: I think that we should have a confirm page after you add the links. But thats
// an advanced feature.
func AddLinkIndexController(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		mp := map[string]interface{}{}
		Render.HTML(w, http.StatusOK, "add", mp)
		return
	}

	err := req.ParseForm()
	if err != nil {
		replyServerError(w, err)
		return
	}

	linksExt, ok := req.Form["links"]
	if !ok {
		replyServerError(w, fmt.Errorf("Corrupt POST message: no links field"))
		return
	}

	text := linksExt[0]
	lines := strings.Split(text, "\n")
	links := make([]string, 0, len(lines))
	var errs []string
	for i := range lines {
		u := strings.TrimSpace(lines[i])
		if u == "" {
			continue
		}

		u, err := assureScheme(u)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}

		links = append(links, u)
	}

	if len(errs) > 0 {
		mp := map[string]interface{}{
			"HasText":         true,
			"Text":            text,
			"HasInfoMessage":  true,
			"InfoMessage":     []string{"No links added"},
			"HasErrorMessage": true,
			"ErrorMessage":    errs,
		}
		Render.HTML(w, http.StatusOK, "add", mp)
		return
	}

	excludeReason := ""
	if len(req.Form["exclude"]) > 0 {
		excludeReason = "Manual exclude"
	}

	errList := DS.InsertLinks(links, excludeReason)
	if len(errList) != 0 {
		for _, e := range errList {
			errs = append(errs, e.Error())
		}
		mp := map[string]interface{}{
			"HasErrorMessage": true,
			"ErrorMessage":    errs,
		}
		Render.HTML(w, http.StatusOK, "add", mp)
		return
	}

	mp := map[string]interface{}{
		"HasInfoMessage": true,
		"InfoMessage":    []string{"All links added"},
	}
	Render.HTML(w, http.StatusOK, "add", mp)
	return
}

//IMPL NOTE: Why does linksController encode the seedURL in base32, rather than URL encode it?
// The reason is that various components along the way are tripping on the appearance of the
// seedURL argument. First, it appears that the browser is unencoding the link BEFORE submitting it
// to the server. That looks like a problem with the browser to me. But in addition, the server appears
// to be choking on the url-encoded text as well. For example if the url encoded seedURL ends with
// .html, it appears that this is causing the server to throw a 301. Unknown why that is. But the net effect
// is that, if I totally disguise the link in base32, everything works.

func LinksController(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	domain := vars["domain"]
	if domain == "" {
		replyServerError(w, fmt.Errorf("User failed to specify domain for linksController"))
		return
	}

	dinfo, err := DS.FindDomain(domain)
	if err != nil {
		replyServerError(w, fmt.Errorf("FindDomain: %v", err))
		return
	}

	if dinfo == nil {
		replyServerError(w, fmt.Errorf("User failed to specify domain for linksController"))
		return
	}

	session, err := GetSession(w, req)
	if err != nil {
		replyServerError(w, fmt.Errorf("GetSession failed: %v", err))
		return
	}

	err = req.ParseForm()
	if err != nil {
		replyServerError(w, err)
		return
	}

	prevLink, prevList, err := processHiddenForm(req, session, true)
	if err != nil {
		replyServerError(w, fmt.Errorf("processHiddenForm failed: %v", err))
		return
	}

	query := cassandra.LQ{Limit: session.LinksPageWindowLength()}

	seedURL := vars["seedURL"]
	needHeader := false
	prevButtonClass := ""
	if seedURL == "" {
		needHeader = true
		query.Limit /= 2
		prevButtonClass = "disabled"
	} else {
		ss, err := decode32(seedURL)
		if err != nil {
			replyServerError(w, fmt.Errorf("decode32: %v", err))
			return
		}
		query.Seed, err = walker.ParseURL(ss)
		if err != nil {
			replyServerError(w, err)
			return
		}
	}

	//
	// Get the filterRegex if there is one
	//
	filterRegex := ""
	filterURLSuffix := ""
	filterRegexSuffix := ""
	filterRegexArr, filterRegexOk := req.Form["filterRegex"]
	if filterRegexOk && len(filterRegexArr) > 0 {
		filterRegex = filterRegexArr[0]
		filterURLSuffix = "?filterRegex=" + filterRegex
		query.FilterRegex, err = decode32(filterRegex)
		if err != nil {
			replyServerError(w, fmt.Errorf("decode32 error: %v", err))
			return
		}
		filterRegexSuffix = fmt.Sprintf("(filtered by /%s/)", query.FilterRegex)
	}

	//
	// Lets grab the links
	//
	linfos, err := DS.ListLinks(domain, query)
	if err != nil {
		replyServerError(w, fmt.Errorf("ListLinks: %v", err))
		return
	}

	//
	// Odds and ends
	//
	nextSeedURL := ""
	nextButtonClass := "disabled"
	if len(linfos) == query.Limit {
		// Use the last link result as seed for next page
		nextSeedURL = encode32(linfos[len(linfos)-1].URL.String())
		nextButtonClass = ""
	}

	var historyLinks []string
	for _, linfo := range linfos {
		path := "/historical/" + encode32(linfo.URL.String())
		historyLinks = append(historyLinks, path)
	}

	excludeTag := "Exclude"
	excludeColor := "green"
	excludeLink := fmt.Sprintf("/excludeToggle/%s/ex", domain)
	if dinfo.ExcludeReason != "" {
		excludeTag = "Unexclude"
		excludeColor = "red"
		excludeLink = fmt.Sprintf("/excludeToggle/%s/un", domain)
	}

	// set up priority dropdown
	prio := []dropdownElement{}
	for _, p := range cassandra.AllowedPriorities {
		prio = append(prio, dropdownElement{
			Link: fmt.Sprintf("/changePriority/%s/%d", domain, p),
			Text: fmt.Sprintf("%d", p),
		})
	}

	// set up page length dropdown
	pageLenDropdown := []dropdownElement{}
	for _, ln := range PageWindowLengthChoices {
		pageLenDropdown = append(pageLenDropdown, dropdownElement{
			Text: fmt.Sprintf("%d", ln),
		})
	}

	//
	// Lets render
	//
	mp := map[string]interface{}{
		"Dinfo":             dinfo,
		"NumberCrawled":     dinfo.NumberLinksTotal - dinfo.NumberLinksUncrawled,
		"HasHeader":         needHeader,
		"HasLinks":          len(linfos) > 0,
		"Linfos":            linfos,
		"NextSeedURL":       nextSeedURL,
		"FilterURLSuffix":   filterURLSuffix,
		"FilterRegexSuffix": filterRegexSuffix,

		"NextButtonClass": nextButtonClass,
		"PrevButtonClass": prevButtonClass,
		"HistoryLinks":    historyLinks,

		"ExcludeTag":   excludeTag,
		"ExcludeColor": excludeColor,
		"ExcludeLink":  excludeLink,

		"PriorityLinks": prio,

		"Prev":            prevLink,
		"PrevList":        prevList,
		"PageLengthLinks": pageLenDropdown,
	}
	Render.HTML(w, http.StatusOK, "links", mp)
	return
}

func LinksHistoricalController(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	url := vars["url"]
	if url == "" {
		replyServerError(w, fmt.Errorf("linksHistoricalController called without url"))
		return
	}
	nurl, err := decode32(url)
	if err != nil {
		replyServerError(w, fmt.Errorf("decode32 (%s): %v", url, err))
		return
	}

	u, err := walker.ParseURL(nurl)
	if err != nil {
		replyServerError(w, err)
		return
	}

	linfos, err := DS.ListLinkHistorical(u)
	if err != nil {
		replyServerError(w, fmt.Errorf("ListLinkHistorical (%v): %v", u, err))
		return
	}

	mp := map[string]interface{}{
		"LinkTopic": u.String(),
		"Linfos":    linfos,
	}
	Render.HTML(w, http.StatusOK, "historical", mp)
}

func FindLinksController(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		mp := map[string]interface{}{}
		Render.HTML(w, http.StatusOK, "findLinks", mp)
		return
	}

	err := req.ParseForm()
	if err != nil {
		replyServerError(w, err)
		return
	}

	linksExt, ok := req.Form["links"]
	if !ok {
		replyServerError(w, fmt.Errorf("Corrupt POST message: no links field"))
		return
	}

	text := linksExt[0]
	lines := strings.Split(text, "\n")
	var info []string
	var errs []string
	var linfos []*cassandra.LinkInfo
	for i := range lines {
		link := strings.TrimSpace(lines[i])
		if link == "" {
			continue
		}

		link, err := assureScheme(link)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}

		u, err := walker.ParseURL(link)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}

		linfo, err := DS.FindLink(u, false)
		if err != nil {
			errs = append(errs, fmt.Sprintf("FindLinks error: %v", err))
			continue
		} else if linfo == nil {
			info = append(info, fmt.Sprintf("Failed to find link '%v'", u))
			continue
		}
		linfos = append(linfos, linfo)
	}

	needErr := len(errs) > 0
	needInf := len(info) > 0

	if len(linfos) == 0 {
		info = append(info, "Failed to find any links")
		mp := map[string]interface{}{
			"Text":            text,
			"HasError":        needErr,
			"HasInfoMessage":  true,
			"InfoMessage":     info,
			"HasErrorMessage": needErr,
			"ErrorMessage":    errs,
		}
		Render.HTML(w, http.StatusOK, "findLinks", mp)
		return
	}

	var historyLinks []string
	for _, linfo := range linfos {
		path := "/historical/" + encode32(linfo.URL.String())
		historyLinks = append(historyLinks, path)
	}

	mp := map[string]interface{}{
		"HasLinks":       true,
		"Linfos":         linfos,
		"DisableButtons": true,
		"AltTitle":       true,
		"HistoryLinks":   historyLinks,

		"HasInfoMessage":  needInf,
		"InfoMessage":     info,
		"HasErrorMessage": needErr,
		"ErrorMessage":    errs,
	}

	Render.HTML(w, http.StatusOK, "links", mp)
	return
}

func ExcludeToggleController(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	domain := vars["domain"]
	direction := vars["direction"]
	if domain == "" || direction == "" {
		replyServerError(w, fmt.Errorf("Ill formed URL passed when trying to change domain exclusion"))
		return
	}
	info := &cassandra.DomainInfo{}
	switch direction {
	case "ex":
		info.Excluded = true
		info.ExcludeReason = "Manual exclude"
	case "un":
		info.Excluded = false
		info.ExcludeReason = ""
	default:
		replyServerError(w, fmt.Errorf("Ill formed URL passed when trying to change domain exclusion"))
		return
	}

	err := DS.UpdateDomain(domain, info, cassandra.DomainInfoUpdateConfig{Exclude: true})
	if err != nil {
		replyServerError(w, err)
		return
	}

	http.Redirect(w, req, fmt.Sprintf("/links/%s", domain), http.StatusFound)
}

func ChangePriorityController(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	domain := vars["domain"]
	priorityStr := vars["priority"]
	priority, err := strconv.Atoi(priorityStr)
	if err != nil {
		replyServerError(w, err)
		return
	}

	foundP := false
	for _, p := range cassandra.AllowedPriorities {
		if p == priority {
			foundP = true
			break
		}
	}
	if !foundP {
		replyServerError(w, fmt.Errorf("Priority value not found in AllowedPriority"))
		return
	}

	info := cassandra.DomainInfo{Priority: priority}
	cfg := cassandra.DomainInfoUpdateConfig{Priority: true}
	err = DS.UpdateDomain(domain, &info, cfg)
	if err != nil {
		err = fmt.Errorf("UpdateDomain failed: %v", err)
		replyServerError(w, err)
		return
	}

	http.Redirect(w, req, fmt.Sprintf("/links/%s", domain), http.StatusFound)
	return
}

func FilterLinksController(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		mp := map[string]interface{}{
			"InputDomainValue": "",
			"InputRegexValue":  "",
		}
		Render.HTML(w, http.StatusOK, "filterLinks", mp)
		return
	}

	err := req.ParseForm()
	if err != nil {
		replyServerError(w, err)
		return
	}

	domain, domainOk := req.Form["domain"]
	regex, regexOk := req.Form["regex"]
	if !domainOk || !regexOk {
		err = fmt.Errorf("Form input was ill formed")
		replyServerError(w, err)
		return
	}

	dinfo, err := DS.FindDomain(domain[0])
	if dinfo == nil || err != nil {
		reason := "Domain not found"
		if err != nil {
			reason = err.Error()
		}
		estring := fmt.Sprintf("Failed to find domain %q: %v", domain[0], reason)
		mp := map[string]interface{}{
			"HasErrorMessage":  true,
			"ErrorMessage":     []string{estring},
			"InputDomainValue": domain[0],
			"InputRegexValue":  regex[0],
		}
		Render.HTML(w, http.StatusOK, "filterLinks", mp)
		return
	}

	_, err = regexp.Compile(regex[0])
	if err != nil {
		err = fmt.Errorf("Failed to compile regex %q: %v", regex[0], err)
		mp := map[string]interface{}{
			"HasErrorMessage":  true,
			"ErrorMessage":     []string{err.Error()},
			"InputDomainValue": domain[0],
			"InputRegexValue":  regex[0],
		}
		Render.HTML(w, http.StatusOK, "filterLinks", mp)
		return
	}

	url := fmt.Sprintf("/links/%s?filterRegex=%s", domain[0], encode32(regex[0]))
	http.Redirect(w, req, url, http.StatusSeeOther)
	return
}

func assureScheme(url string) (string, error) {
	index := strings.LastIndex(url, ":")
	if index < 0 {
		return "http://" + url, nil
	}

	scheme := url[:index]
	for _, f := range walker.Config.Fetcher.AcceptProtocols {
		if scheme == f {
			return url, nil
		}
	}

	return "", fmt.Errorf("Scheme %q is not in AcceptProtocols", scheme)
}
