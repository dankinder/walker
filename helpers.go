package walker

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"code.google.com/p/log4go"
)

// LoadTestConfig loads the given test config yaml file. The given path is
// assumed to be relative to the `walker/test/` directory, the location of this
// file. This will panic if it cannot read the requested config file. If you
// expect an error or are testing ReadConfigFile, use `GetTestFileDir()`
// instead.
func LoadTestConfig(filename string) {
	testdir := GetTestFileDir()
	err := ReadConfigFile(path.Join(testdir, filename))
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
	if !filepath.IsAbs(p) {
		log4go.Warn("Tried to use runtime.Caller to get the test file "+
			"directory, but the path is incorrect: %v\nMost likely this means the "+
			"-cover flag was used with `go test`, which does not return a usable "+
			"path when testing the walker package. Returning './test' as the test "+
			"directory; if CWD != the root walker directory, tests will fail.", p)
		return "test"
	}
	return path.Join(path.Dir(p), "test")
}

// fakeDial makes connections to localhost, no matter what addr was given.
func fakeDial(network, addr string) (net.Conn, error) {
	_, port, _ := net.SplitHostPort(addr)
	return net.Dial(network, net.JoinHostPort("localhost", port))
}

// getFakeTransport gets a http.RoundTripper that uses fakeDial
func getFakeTransport() http.RoundTripper {
	return &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		Dial:                fakeDial,
		TLSHandshakeTimeout: 10 * time.Second,
	}
}

//
// RecordingTransport counts how many times the Dial routine is called
//
type recordingTransport struct {
	http.Transport
	Name   string
	Record []string
}

func (self *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	self.Record = append(self.Record, req.URL.String())
	return self.Transport.RoundTrip(req)
}

func (self *recordingTransport) String() string {
	return fmt.Sprintf("recordingTransport named %v: %v", self.Name, self.Record)
}

func getRecordingTransport(name string) *recordingTransport {
	r := &recordingTransport{
		Transport: http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			TLSHandshakeTimeout: 10 * time.Second,
			Dial:                fakeDial,
		},
		Name: name,
	}

	return r
}

//
// CancelTrackingTransport isa http.Transport that tracks which requests where canceled.
//
type cancelTrackingTransport struct {
	http.Transport
	Canceled map[string]int
}

func (ctt *cancelTrackingTransport) CancelRequest(req *http.Request) {
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

func getWontConnectTransport() (*cancelTrackingTransport, io.Closer) {
	dialer := &wontConnectDial{make(chan struct{})}
	trans := &cancelTrackingTransport{
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

func getStallingReadTransport() (*cancelTrackingTransport, io.Closer) {
	dialer := &stallCloser{make(map[*stallingConn]bool)}
	trans := &cancelTrackingTransport{
		Transport: http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			Dial:                dialer.Dial,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		Canceled: make(map[string]int),
	}
	return trans, dialer
}

// MustParse is a helper for calling ParseURL when we kow the string is
// a safe URL. It will panic if it fails.
func MustParse(ref string) *URL {
	u, err := ParseURL(ref)
	if err != nil {
		panic("Failed to parse URL: " + ref)
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
	Responses map[string]*http.Response
}

func (mrt *mapRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) {
	res, resOk := mrt.Responses[req.URL.String()]
	if !resOk {
		return response404(), nil
	}
	return res, nil
}

// This allows the mapRoundTrip to be canceled. Which is needed to prevent
// errant robots.txt GET's to break TestRedirects.
func (self *mapRoundTrip) CancelRequest(req *http.Request) {
}
