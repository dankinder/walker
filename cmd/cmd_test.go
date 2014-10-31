// +build cassandra

package test

import (
	"io/ioutil"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/cmd"

	"github.com/stretchr/testify/mock"
)

func TestCommandsReadConfig(t *testing.T) {
	orig := os.Args
	defer func() {
		os.Args = orig
		// Reset config for the remaining tests
		loadTestConfig("test-walker.yaml")
	}()

	handler := &MockHandler{}
	cmd.Handler(handler)

	getDB(t) // Ensure that walker_test exists, as the console will try to connect
	datastore := &MockDatastore{}
	datastore.On("ClaimNewHost").Return("")
	datastore.On("ClaimNewHost").Return("")
	datastore.On("StoreParsedURL", mock.Anything, mock.Anything).Return()
	cmd.Datastore(datastore)

	dispatcher := &MockDispatcher{}
	dispatcher.On("StartDispatcher").Return(nil)
	dispatcher.On("StopDispatcher").Return(nil)
	cmd.Dispatcher(dispatcher)

	var walkerCommands = []string{"crawl", "fetch", "dispatch", "seed", "console"}
	for _, walkerCom := range walkerCommands {
		loadTestConfig("test-walker.yaml")
		expectedDefaultAgent := "Walker (http://github.com/iParadigms/walker)"
		if walker.Config.UserAgent != expectedDefaultAgent {
			t.Fatalf("Failed to reset default config value (user_agent), expected: %v\nBut got: %v",
				expectedDefaultAgent, walker.Config.UserAgent)
		}

		switch walkerCom {
		case "seed":
			os.Args = []string{os.Args[0], walkerCom, "--url=http://test.com", "--config=test-walker2.yaml"}
		default:
			os.Args = []string{os.Args[0], walkerCom, "--config=test-walker2.yaml"}
		}

		go func() {
			time.Sleep(100 * time.Millisecond)
			syscall.Kill(os.Getpid(), syscall.SIGINT)
		}()
		cmd.Execute()

		expectedTestAgent := "Test Agent (set in yaml)"
		if walker.Config.UserAgent != expectedTestAgent {
			t.Errorf("Failed to set config value (user_agent) via yaml, expected: %v\nBut got: %v",
				expectedTestAgent, walker.Config.UserAgent)
		}
	}
}

func TestCrawlCommand(t *testing.T) {
	orig := os.Args
	defer func() { os.Args = orig }()

	args := [][]string{
		[]string{os.Args[0], "crawl"},
		[]string{os.Args[0], "crawl", "--no-console"},
	}

	for index := range args {
		handler := &MockHandler{}
		cmd.Handler(handler)

		datastore := &MockDatastore{}
		datastore.On("ClaimNewHost").Return("")
		cmd.Datastore(datastore)

		dispatcher := &MockDispatcher{}
		dispatcher.On("StartDispatcher").Return(nil)
		dispatcher.On("StopDispatcher").Return(nil)
		cmd.Dispatcher(dispatcher)

		os.Args = args[index]

		go func() {
			time.Sleep(5 * time.Millisecond)
			syscall.Kill(os.Getpid(), syscall.SIGINT)
		}()
		cmd.Execute()

		handler.AssertExpectations(t)
		datastore.AssertExpectations(t)
		dispatcher.AssertExpectations(t)
	}
}

func TestFetchCommand(t *testing.T) {
	handler := &MockHandler{}
	cmd.Handler(handler)

	datastore := &MockDatastore{}
	datastore.On("ClaimNewHost").Return("")
	cmd.Datastore(datastore)

	// Set the dispatcher to ensure it doesn't receive any calls
	dispatcher := &MockDispatcher{}
	cmd.Dispatcher(dispatcher)

	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "fetch"}

	go func() {
		time.Sleep(5 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	cmd.Execute()

	handler.AssertExpectations(t)
	datastore.AssertExpectations(t)
	dispatcher.AssertExpectations(t)
}

func TestDispatchCommand(t *testing.T) {
	// Set a handler and datastore to ensure they doesn't receive any calls
	handler := &MockHandler{}
	cmd.Handler(handler)

	datastore := &MockDatastore{}
	cmd.Datastore(datastore)

	dispatcher := &MockDispatcher{}
	dispatcher.On("StartDispatcher").Return(nil)
	dispatcher.On("StopDispatcher").Return(nil)
	cmd.Dispatcher(dispatcher)

	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "dispatch"}

	go func() {
		time.Sleep(5 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	cmd.Execute()

	handler.AssertExpectations(t)
	datastore.AssertExpectations(t)
	dispatcher.AssertExpectations(t)
}

func TestSeedCommand(t *testing.T) {
	u, _ := walker.ParseURL("http://test.com")
	datastore := &MockDatastore{}
	datastore.On("StoreParsedURL", u, mock.AnythingOfType("*walker.FetchResults")).Return("")
	cmd.Datastore(datastore)

	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "seed", "--url=" + u.String()}

	go func() {
		time.Sleep(5 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	cmd.Execute()

	datastore.AssertExpectations(t)
}

func TestSchemaCommand(t *testing.T) {
	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "schema", "--out=test.cql"}
	cmd.Execute()
	defer os.Remove("test.cql")

	f, err := ioutil.ReadFile("test.cql")
	if err != nil {
		t.Fatalf("Failed to read test.cql: %v", err)
	}
	if !strings.HasPrefix(string(f), "-- The schema file for walker") {
		t.Fatalf("test.cql has unexpected contents: %v", f)
	}
}
