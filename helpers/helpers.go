package helpers

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/iParadigms/walker"
)

// LoadTestConfig loads the given test config yaml file. The given path is
// assumed to be relative to the `walker/helpers/` directory, the location of this
// file. This will panic if it cannot read the requested config file. If you
// expect an error or are testing walker.ReadConfigFile, use `GetTestFileDir()`
// instead.
func LoadTestConfig(filename string) {
	testdir := GetTestFileDir()
	err := walker.ReadConfigFile(path.Join(testdir, filename))
	if err != nil {
		panic(err.Error())
	}
}

// GetTestFileDir returns the directory where shared test files are stored, for
// example test config files. It will panic if it could not get the path from
// the runtime.
func GetTestFileDir() string {
	_, p, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get location of test source file")
	}
	return path.Dir(p)
}

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
// RecordingTransport counts how many times the Dial routine is called
//
type RecordingTransport struct {
	http.Transport
	Name   string
	Record []string
}

// RoundTrip implenets http.RoundTripper interface
func (rt *RecordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.Record = append(rt.Record, req.URL.String())
	return rt.Transport.RoundTrip(req)
}

// String implements Stringer interface
func (rt *RecordingTransport) String() string {
	return fmt.Sprintf("RecordingTransport named %v: %v", rt.Name, rt.Record)
}

// GetRecordingTransport returns a RecordingTransport with name set to name.
func GetRecordingTransport(name string) *RecordingTransport {
	r := &RecordingTransport{
		Transport: http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			TLSHandshakeTimeout: 10 * time.Second,
			Dial:                FakeDial,
		},
		Name: name,
	}

	return r
}

//
// CancelTrackingTransport isa http.Transport that tracks which requests where canceled.
//
type CancelTrackingTransport struct {
	http.Transport
	Canceled map[string]int
}

// CancelRequest will be called in the http stack to cancel this request.
func (ctt *CancelTrackingTransport) CancelRequest(req *http.Request) {
	key := req.URL.String()
	count := 0
	if c, cok := ctt.Canceled[key]; cok {
		count = c
	}
	ctt.Canceled[key] = count + 1
	ctt.Transport.CancelRequest(req)
}

//
// wontConnectDial has a Dial routine that will never connect
//
type wontConnectDial struct {
	quit chan struct{}
}

// Close allows usert to close the wontConnectDial
func (wcd *wontConnectDial) Close() error {
	close(wcd.quit)
	return nil
}

// Dial function won't return until quit is closed.
func (wcd *wontConnectDial) Dial(network, addr string) (net.Conn, error) {
	<-wcd.quit
	return nil, fmt.Errorf("I'll never connect!!")
}

// GetWontConnectTransport produces a CancelTrackingTransport instance with a closer to close down the faux-connection.
func GetWontConnectTransport() (*CancelTrackingTransport, io.Closer) {
	dialer := &wontConnectDial{make(chan struct{})}
	trans := &CancelTrackingTransport{
		Transport: http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			Dial:                dialer.Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		Canceled: make(map[string]int),
	}

	return trans, dialer
}

//
// Spoof an Addr interface. Used by stallingConn
//
type emptyAddr struct{}

func (ea *emptyAddr) Network() string {
	return ""
}

func (ea *emptyAddr) String() string {
	return ""

}

//
// stallingConn will stall during any read or write
//
type stallingConn struct {
	closed bool
	quit   chan struct{}
}

func (sc *stallingConn) Read(b []byte) (int, error) {
	<-sc.quit
	return 0, fmt.Errorf("Staling Read")
}

func (sc *stallingConn) Write(b []byte) (int, error) {
	<-sc.quit
	return 0, fmt.Errorf("Staling Write")
}

func (sc *stallingConn) Close() error {
	if !sc.closed {
		close(sc.quit)
	}
	sc.closed = true
	return nil
}

func (sc *stallingConn) LocalAddr() net.Addr {
	return &emptyAddr{}
}

func (sc *stallingConn) RemoteAddr() net.Addr {
	return &emptyAddr{}
}

func (sc *stallingConn) SetDeadline(t time.Time) error {
	return nil
}

func (sc *stallingConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (sc *stallingConn) SetWriteDeadline(t time.Time) error {
	return nil
}

//
// stallCloser tracks a bundle of stallingConn's
//
type stallCloser struct {
	stalls map[*stallingConn]bool
}

func (sc *stallCloser) Close() error {
	for conn := range sc.stalls {
		conn.Close()
	}
	return nil
}

func (sc *stallCloser) newConn() *stallingConn {
	x := &stallingConn{quit: make(chan struct{})}
	sc.stalls[x] = true
	return x
}

func (sc *stallCloser) Dial(network, addr string) (net.Conn, error) {
	return sc.newConn(), nil
}

// GetStallingReadTransport returns a CancelTrackingTransport with a closer.
func GetStallingReadTransport() (*CancelTrackingTransport, io.Closer) {
	dialer := &stallCloser{make(map[*stallingConn]bool)}
	trans := &CancelTrackingTransport{
		Transport: http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			Dial:                dialer.Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		Canceled: make(map[string]int),
	}
	return trans, dialer
}

// Parse is a helper to just get a walker.URL object from a string we know is a
// safe url (ParseURL requires us to deal with potential errors)
func Parse(ref string) *walker.URL {
	u, err := walker.ParseURL(ref)
	if err != nil {
		panic("Failed to parse walker.URL: " + ref)
	}
	return u
}

// URLParse is similar to `parse` but gives a Go builtin URL type (not a walker
// URL)
func URLParse(ref string) *url.URL {
	u := Parse(ref)
	return u.URL
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

// Response307 is a helpers that creates an http.Response object that is a 307 response
func Response307(link string) *http.Response {
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

// Response200 is a helper that creates an http.Response that is a 200 response.
func Response200() *http.Response {
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

// MapRoundTrip maps input links --> http.Response. See TestRedirects for example.
type MapRoundTrip struct {
	Responses map[string]*http.Response
}

// RoundTrip implements the http.RoundTripper interface
func (mrt *MapRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) {
	res, resOk := mrt.Responses[req.URL.String()]
	if !resOk {
		return response404(), nil
	}
	return res, nil
}

// CancelRequest allows the MapRoundTrip to be canceled. Which is needed to prevent
// errant robots.txt GET's to break TestRedirects.
func (mrt *MapRoundTrip) CancelRequest(req *http.Request) {
}
