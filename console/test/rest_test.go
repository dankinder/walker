package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/console"
	"github.com/iParadigms/walker/helpers"
)

func restReq(url string, mp map[string]interface{}) (map[string]interface{}, int) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	err := encoder.Encode(mp)
	if err != nil {
		panic(err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(buffer.Bytes()))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	status := resp.StatusCode
	rmp := map[string]interface{}{}
	if status != http.StatusOK {
		decoder := json.NewDecoder(resp.Body)
		err = decoder.Decode(&rmp)
		if err != nil {
			panic(fmt.Errorf("decoder.Decode %v", err))
		}
	}

	return rmp, status
}

func fixtureStart() {
	// see controllers_test.go for the def'n of this
	spoofData()

	console.Start()
	time.Sleep(time.Second)
}

func fixtureEnd() {
	console.Stop()
}

//target delivers the URL to hit the console at
func target(restPath string) string {
	return fmt.Sprintf("http://127.0.0.1:%d/rest/%s", walker.Config.Console.Port, restPath)
}

func TestAdd(t *testing.T) {
	fixtureStart()

	//
	// good add first
	//
	hex := fmt.Sprintf("%x", rand.Uint32())
	links := []string{
		fmt.Sprintf("http://sub%s.dom.com/page1.html", hex),
		fmt.Sprintf("http://sub%s.dom.com/page2.html", hex),
		fmt.Sprintf("http://sub%s.dom.com/page3.html", hex),
	}
	mp := map[string]interface{}{
		"version": 1,
		"links": []interface{}{
			map[string]interface{}{"url": links[0]},
			map[string]interface{}{"url": links[1]},
			map[string]interface{}{"url": links[2]},
		},
	}
	url := target("add")
	rmp, status := restReq(url, mp)
	if status != http.StatusOK {
		t.Fatalf("Failed to return 200 on good add:\n%v", rmp)
	}

	for _, link := range links {
		linfo, err := console.DS.FindLink(helpers.Parse(link), false)
		if err != nil {
			t.Errorf("Expected to find link %q in datastore, but found an error %v instead", link, err)
			continue
		}
		if linfo == nil {
			t.Errorf("Expected to find link %q in datastore, but didn't", link)
			continue
		}
	}

	//
	// Erroneous adds
	//
	mp = map[string]interface{}{
		"version": 1,
		"type":    "add",
	}
	rmp, status = restReq(url, mp)
	if status != http.StatusBadRequest {
		t.Fatalf("ErrorAdd1 Got status code %d, but expected status code %d", status, http.StatusBadRequest)
	}
	tag, tagOk := rmp["tag"].(string)
	tagExpect := "empty-links"
	if !tagOk || tag != tagExpect {
		t.Fatalf("ErrorAdd1 Error return had (tag, tagOk) = (%q, %v), expected tag = %q", tag, tagOk, tagExpect)
	}

	mp = map[string]interface{}{
		"version": 1,
		"links": []interface{}{
			map[string]interface{}{},
			map[string]interface{}{},
			map[string]interface{}{},
		},
	}
	rmp, status = restReq(url, mp)
	if status != http.StatusBadRequest {
		t.Fatalf("ErrorAdd2 Got status code %d, but expected status code %d", status, http.StatusBadRequest)
	}
	tag, tagOk = rmp["tag"].(string)
	tagExpect = "bad-link-element"
	if !tagOk || tag != tagExpect {
		t.Fatalf("ErrorAdd2 Error return had (tag, tagOk) = (%q, %v), expected tag = %q", tag, tagOk, tagExpect)
	}

	mp = map[string]interface{}{
		"version": 1,
		"type":    "add",
		"links": []interface{}{
			map[string]interface{}{"url": "bumLink"},
		},
	}
	rmp, status = restReq(url, mp)
	if status != http.StatusBadRequest {
		t.Fatalf("ErrorAdd3 Got status code %d, but expected status code %d", status, http.StatusBadRequest)
	}
	tag, tagOk = rmp["tag"].(string)
	tagExpect = "insert-links-error"
	if !tagOk || tag != tagExpect {
		t.Fatalf("ErrorAdd3 Error return had (tag, tagOk) = (%q, %v), expected tag = %q", tag, tagOk, tagExpect)
	}

	fixtureEnd()
}
