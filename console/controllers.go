/*
	This file contains the web-facing handlers.
*/
package console

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"code.google.com/p/log4go"
	"github.com/gorilla/mux"
	"github.com/iParadigms/walker"
)

type Route struct {
	Path       string
	Controller func(w http.ResponseWriter, req *http.Request)
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
		Route{Path: "/links/{domain}/{seedUrl}", Controller: LinksController},
		Route{Path: "/historical/{url}", Controller: LinksHistoricalController},
		Route{Path: "/findLinks", Controller: FindLinksController},
		Route{Path: "/excludeToggle/{domain}/{direction}", Controller: ExcludeToggleController},
	}
}

func HomeController(w http.ResponseWriter, req *http.Request) {
	mp := map[string]interface{}{}
	Render.HTML(w, http.StatusOK, "home", mp)
	return
}

func ListDomainsController(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	seed := vars["seed"]
	prevButtonClass := ""
	if seed == "" {
		seed = DontSeedDomain
		prevButtonClass = "disabled"
	} else {
		var err error
		seed, err = url.QueryUnescape(seed)
		if err != nil {
			seed = DontSeedDomain
		}
	}

	dinfos, err := DS.ListDomains(seed, PageWindowLength)
	if err != nil {
		err = fmt.Errorf("ListDomains failed: %v", err)
		replyServerError(w, err)
		return
	}

	nextDomain := ""
	nextButtonClass := "disabled"
	if len(dinfos) == PageWindowLength {
		nextDomain = url.QueryEscape(dinfos[len(dinfos)-1].Domain)
		nextButtonClass = ""
	}

	mp := map[string]interface{}{
		"PrevButtonClass": prevButtonClass,
		"NextButtonClass": nextButtonClass,
		"Domains":         dinfos,
		"Next":            nextDomain,
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

	var dinfos []DomainInfo
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

		dinfos = append(dinfos, *dinfo)
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

	errList := DS.InsertLinks(links, "")
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

//IMPL NOTE: Why does linksController encode the seedUrl in base32, rather than URL encode it?
// The reason is that various components along the way are tripping on the appearance of the
// seedUrl argument. First, it appears that the browser is unencoding the link BEFORE submitting it
// to the server. That looks like a problem with the browser to me. But in addition, the server appears
// to be choking on the url-encoded text as well. For example if the url encoded seedUrl ends with
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

	seedUrl := vars["seedUrl"]
	needHeader := false
	windowLength := PageWindowLength
	prevButtonClass := ""
	if seedUrl == "" {
		needHeader = true
		windowLength /= 2
		prevButtonClass = "disabled"
	} else {
		ss, err := decode32(seedUrl)
		if err != nil {
			replyServerError(w, fmt.Errorf("QueryUnescape: %v", err))
			return
		}
		seedUrl = ss
	}

	linfos, err := DS.ListLinks(domain, seedUrl, windowLength)
	if err != nil {
		replyServerError(w, fmt.Errorf("ListLinks: %v", err))
		return
	}

	nextSeedUrl := ""
	nextButtonClass := "disabled"
	if len(linfos) == windowLength {
		nextSeedUrl = encode32(linfos[len(linfos)-1].Url)
		nextButtonClass = ""
	}

	var historyLinks []string
	for _, linfo := range linfos {
		path := "/historical/" + encode32(linfo.Url)
		historyLinks = append(historyLinks, path)
	}

	excludeTag := "Exclude"
	excludeColor := "lightblue"
	excludeLink := fmt.Sprintf("/excludeToggle/%s/ex", domain)
	if dinfo.ExcludeReason != "" {
		excludeTag = "Unexclude"
		excludeColor = "red"
		excludeLink = fmt.Sprintf("/excludeToggle/%s/un", domain)
	}

	mp := map[string]interface{}{
		"Dinfo":           dinfo,
		"NumberCrawled":   dinfo.NumberLinksTotal - dinfo.NumberLinksUncrawled,
		"HasHeader":       needHeader,
		"HasLinks":        len(linfos) > 0,
		"Linfos":          linfos,
		"NextSeedUrl":     nextSeedUrl,
		"NextButtonClass": nextButtonClass,
		"PrevButtonClass": prevButtonClass,
		"HistoryLinks":    historyLinks,

		"ExcludeTag":   excludeTag,
		"ExcludeColor": excludeColor,
		"ExcludeLink":  excludeLink,
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
	url = nurl

	linfos, _, err := DS.ListLinkHistorical(url, DontSeedIndex, 500)
	if err != nil {
		replyServerError(w, fmt.Errorf("ListLinkHistorical (%s): %v", url, err))
		return
	}

	mp := map[string]interface{}{
		"LinkTopic": url,
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
	var linfos []LinkInfo
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

		linfo, err := DS.FindLink(u)
		if err != nil {
			errs = append(errs, fmt.Sprintf("FindLinks error: %v", err))
			continue
		} else if linfo == nil {
			info = append(info, fmt.Sprintf("Failed to find link '%v'", u))
			continue
		}
		linfos = append(linfos, *linfo)
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
		path := "/historical/" + encode32(linfo.Url)
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
	var exclude bool
	var reason string
	switch direction {
	case "ex":
		exclude = true
		reason = "Manual exclude"
	case "un":
		exclude = false
		reason = ""
	default:
		replyServerError(w, fmt.Errorf("Ill formed URL passed when trying to change domain exclusion"))
		return
	}

	err := DS.UpdateDomainExclude(domain, exclude, reason)
	if err != nil {
		replyServerError(w, err)
		return
	}

	http.Redirect(w, req, fmt.Sprintf("/links/%s", domain), http.StatusFound)
}

func assureScheme(url string) (string, error) {
	index := strings.LastIndex(url, ":")
	if index < 0 {
		return "http://" + url, nil
	}

	scheme := url[:index]
	for _, f := range walker.Config.AcceptProtocols {
		if scheme == f {
			return url, nil
		}
	}

	return "", fmt.Errorf("Scheme %q is not in AcceptProtocols", scheme)
}
