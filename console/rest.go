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
//  3. Any error is flagged by HTTP status code 500. A json encoded error message will always be returned with a 500.
//
// The next thing to note is the format of each message exchanged with the rest API. Each message will have at least
// a version attribute and a type attribute. The type attribute is often superfluous in it's content (if you're sending
// a json blob to /rest/add you know it's an add request, so why set type="add"?). But by leaving type on the messages,
// it would be easier to determine the destination of the message if the message was stored someplace else (think in
// a log file somewhere).
//

func RestRoutes() []Route {
	return []Route{
		Route{Path: "/rest/add", Controller: RestAdd},
	}
}

type restErrorResponse struct {
	Version int    `json:"version"`
	Typ     string `json:"type"`
	Tag     string `json:"tag"`
	Message string `json:"message"`
}

func buildError(tag string, format string, args ...interface{}) *restErrorResponse {
	return &restErrorResponse{
		Version: 1,
		Typ:     "error",
		Tag:     tag,
		Message: fmt.Sprintf(format, args...),
	}
}

type restAddRequest struct {
	Version int    `json:"version"`
	Typ     string `json:"type"`
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
		Render.JSON(w, http.StatusInternalServerError, buildError("bad-json-decode", "%v", err))
		return
	}

	//XXX: We don't enforce the correct usage of the Typ field of adds. For consistency,
	// we might force our users to populate the type field with the string 'add'. The
	// advantage of maintaining consistency is that all the json messages we exchange
	// will be properly tagged, and thus easier to interpret when out of context (see above).

	if len(adds.Links) == 0 {
		Render.JSON(w, http.StatusInternalServerError, buildError("empty-links", "No links provided to add"))
		return
	}

	var links []string
	for _, l := range adds.Links {
		u := l.Url
		if u == "" {
			Render.JSON(w, http.StatusInternalServerError, buildError("bad-link-element", "No URL provided for link"))
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
		Render.JSON(w, http.StatusInternalServerError, buildError("insert-links-error", buffer.String()))
		return
	}

	Render.JSON(w, http.StatusOK, "")
	return
}
