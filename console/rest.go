package console

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"code.google.com/p/log4go"
)

//
// IMPLEMENTATION NOTE: Few notes about the approach to REST used in this file:
//  1. Always exchange JSON
//  2. Any successful rest request returns HTTP status code 200. If the server can leave the HTTP body empty, it will
//  3. Any error is flagged by HTTP status != 200. A json encoded error message will always be returned with a 500.
//
// The next thing to note is the format of each message exchanged with the rest API. Each message will have at least
// a version attribute.
//

func RestRoutes() []Route {
	return []Route{
		Route{Path: "/rest/add", Controller: RestAdd},
	}
}

type restErrorResponse struct {
	Version int    `json:"version"`
	Tag     string `json:"tag"`
	Message string `json:"message"`
}

func buildError(tag string, format string, args ...interface{}) *restErrorResponse {
	return &restErrorResponse{
		Version: 1,
		Tag:     tag,
		Message: fmt.Sprintf(format, args...),
	}
}

type restAddRequest struct {
	Version int `json:"version"`
	Links   []struct {
		Url string `json:"url"`
	} `json:"links"`
}

func RestAdd(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	var adds restAddRequest
	err := decoder.Decode(&adds)
	if err != nil {
		log4go.Error("RestAdd failed to decode %v", err)
		Render.JSON(w, http.StatusBadRequest, buildError("bad-json-decode", "%v", err))
		return
	}

	if len(adds.Links) == 0 {
		Render.JSON(w, http.StatusBadRequest, buildError("empty-links", "No links provided to add"))
		return
	}

	var links []string
	for _, l := range adds.Links {
		u := l.Url
		if u == "" {
			Render.JSON(w, http.StatusBadRequest, buildError("bad-link-element", "No URL provided for link"))
			return
		}
		links = append(links, u)
	}

	errList := DS.InsertLinks(links)
	if len(errList) != 0 {
		var buffer bytes.Buffer
		for _, e := range errList {
			buffer.WriteString(e.Error())
			buffer.WriteString("\n")
		}
		Render.JSON(w, http.StatusBadRequest, buildError("insert-links-error", buffer.String()))
		return
	}

	Render.JSON(w, http.StatusOK, "")
	return
}
