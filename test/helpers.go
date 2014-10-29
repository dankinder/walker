package test

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

// FakeDial makes connections to localhost, no matter what addr was given.
func FakeDial(network, addr string) (net.Conn, error) {
	_, port, _ := net.SplitHostPort(addr)
	return net.Dial(network, net.JoinHostPort("localhost", port))
}

// GetFakeTransport gets a http.RoundTripper that uses FakeDial
func GetFakeTransport() http.RoundTripper {
	return &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		Dial:                FakeDial,
		TLSHandshakeTimeout: 10 * time.Second,
	}
}

//
// http.Transport that tracks which requests where canceled.
//
type cancelTrackingTransport struct {
	http.Transport
	canceled map[string]int
}

func (self *cancelTrackingTransport) CancelRequest(req *http.Request) {
	key := req.URL.String()
	count := 0
	if c, cok := self.canceled[key]; cok {
		count = c
	}
	self.canceled[key] = count + 1
	self.Transport.CancelRequest(req)
}

//
// wontConnectDial has a Dial routine that will never connect
//
type wontConnectDial struct {
	quit chan struct{}
}

func (self *wontConnectDial) Close() error {
	close(self.quit)
	return nil
}

func (self *wontConnectDial) Dial(network, addr string) (net.Conn, error) {
	<-self.quit
	return nil, fmt.Errorf("I'll never connect!!")
}

func getWontConnectTransport() (*cancelTrackingTransport, io.Closer) {
	dialer := &wontConnectDial{make(chan struct{})}
	trans := &cancelTrackingTransport{
		Transport: http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			Dial:                dialer.Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		canceled: make(map[string]int),
	}

	return trans, dialer
}

//
// Spoof an Addr interface. Used by stallingConn
//
type emptyAddr struct{}

func (self *emptyAddr) Network() string {
	return ""
}

func (self *emptyAddr) String() string {
	return ""

}

//
// stallingConn will stall during any read or write
//
type stallingConn struct {
	closed bool
	quit   chan struct{}
}

func (self *stallingConn) Read(b []byte) (int, error) {
	<-self.quit
	return 0, fmt.Errorf("Staling Read")
}

func (self *stallingConn) Write(b []byte) (int, error) {
	<-self.quit
	return 0, fmt.Errorf("Staling Write")
}

func (self *stallingConn) Close() error {
	if !self.closed {
		close(self.quit)
	}
	self.closed = true
	return nil
}

func (self *stallingConn) LocalAddr() net.Addr {
	return &emptyAddr{}
}

func (self *stallingConn) RemoteAddr() net.Addr {
	return &emptyAddr{}
}

func (self *stallingConn) SetDeadline(t time.Time) error {
	return nil
}

func (self *stallingConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (self *stallingConn) SetWriteDeadline(t time.Time) error {
	return nil
}

//
// stallCloser tracks a bundle of stallingConn's
//
type stallCloser struct {
	stalls map[*stallingConn]bool
}

func (self *stallCloser) Close() error {
	for conn := range self.stalls {
		conn.Close()
	}
	return nil
}

func (self *stallCloser) newConn() *stallingConn {
	x := &stallingConn{quit: make(chan struct{})}
	self.stalls[x] = true
	return x
}

func (self *stallCloser) Dial(network, addr string) (net.Conn, error) {
	return self.newConn(), nil
}

func getStallingReadTransport() (*cancelTrackingTransport, io.Closer) {
	dialer := &stallCloser{make(map[*stallingConn]bool)}
	trans := &cancelTrackingTransport{
		Transport: http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			Dial:                dialer.Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		canceled: make(map[string]int),
	}
	return trans, dialer
}

// parse is a helper to just get a URL object from a string we know is a safe
// url (ParseURL requires us to deal with potential errors)
func parse(ref string) *walker.URL {
	u, err := walker.ParseURL(ref)
	if err != nil {
		panic("Failed to parse walker.URL: " + ref)
	}
	return u
}

// urlParse is similar to `parse` but gives a Go builtin URL type (not a walker
// URL)
func urlParse(ref string) *url.URL {
	u, err := url.Parse(ref)
	if err != nil {
		panic("Failed to parse url.URL: " + ref)
	}
	return u
}

func response404() *http.Response {
	return &http.Response{
		Status:        "404",
		StatusCode:    404,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Header:        http.Header{"Content-Type": []string{"text/html"}},
		Body:          ioutil.NopCloser(strings.NewReader("")),
		ContentLength: -1,
	}
}

func response307(link string) *http.Response {
	return &http.Response{
		Status:        "307",
		StatusCode:    307,
		Proto:         "HTTP/1.0",
		ProtoMajor:    1,
		ProtoMinor:    0,
		Header:        http.Header{"Location": []string{link}, "Content-Type": []string{"text/html"}},
		Body:          ioutil.NopCloser(strings.NewReader("")),
		ContentLength: -1,
	}
}

func response200() *http.Response {
	return &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
		Proto:      "HTTP/1.0",
		ProtoMajor: 1,
		ProtoMinor: 0,
		Header:     http.Header{"Content-Type": []string{"text/html"}},
		Body: ioutil.NopCloser(strings.NewReader(
			`<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>No Links</title>
</head>
<div id="menu">
</div>
</html>`)),
		ContentLength: -1,
	}
}

// mapRoundTrip maps input links --> http.Response. See TestRedirects for example.
type mapRoundTrip struct {
	responses map[string]*http.Response
}

func (mrt *mapRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) {
	res, resOk := mrt.responses[req.URL.String()]
	if !resOk {
		return response404(), nil
	}
	return res, nil
}

// This allows the mapRoundTrip to be canceled. Which is needed to prevent
// errant robots.txt GET's to break TestRedirects.
func (self *mapRoundTrip) CancelRequest(req *http.Request) {
}

var initdb sync.Once

func getDB(t *testing.T) *gocql.Session {
	initdb.Do(func() {
		err := walker.CreateCassandraSchema()
		if err != nil {
			t.Fatalf(err.Error())
		}
	})

	if walker.Config.Cassandra.Keyspace != "walker_test" {
		t.Fatal("Running tests requires using the walker_test keyspace")
		return nil
	}
	config := walker.GetCassandraConfig()
	db, err := config.CreateSession()
	if err != nil {
		t.Fatalf("Could not connect to local cassandra db: %v", err)
		return nil
	}

	tables := []string{"links", "segments", "domain_info"}
	for _, table := range tables {
		err := db.Query(fmt.Sprintf(`TRUNCATE %v`, table)).Exec()
		if err != nil {
			t.Fatalf("Failed to truncate table %v: %v", table, err)
		}
	}

	return db
}
