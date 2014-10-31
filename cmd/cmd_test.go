// +build cassandra

package cmd

import (
	"io/ioutil"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/cassandra"
	"github.com/iParadigms/walker/helpers"

	"github.com/stretchr/testify/mock"
)

func TestCommandsReadConfig(t *testing.T) {
	orig := os.Args
	defer func() {
		os.Args = orig
		// Reset config for the remaining tests
		helpers.LoadTestConfig("test-walker.yaml")
	}()

	handler := &helpers.MockHandler{}
	Handler(handler)

	cassandra.GetTestDB() // Ensure that walker_test exists, as the console will try to connect
	datastore := &helpers.MockDatastore{}
	datastore.On("ClaimNewHost").Return("")
	datastore.On("ClaimNewHost").Return("")
	datastore.On("StoreParsedURL", mock.Anything, mock.Anything).Return()
	Datastore(datastore)

	dispatcher := &helpers.MockDispatcher{}
	dispatcher.On("StartDispatcher").Return(nil)
	dispatcher.On("StopDispatcher").Return(nil)
	Dispatcher(dispatcher)

	var walkerCommands = []string{"crawl", "fetch", "dispatch", "seed", "console"}
	for _, walkerCom := range walkerCommands {
		helpers.LoadTestConfig("test-walker.yaml")
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
		Execute()

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
		handler := &helpers.MockHandler{}
		Handler(handler)

		datastore := &helpers.MockDatastore{}
		datastore.On("ClaimNewHost").Return("")
		Datastore(datastore)

		dispatcher := &helpers.MockDispatcher{}
		dispatcher.On("StartDispatcher").Return(nil)
		dispatcher.On("StopDispatcher").Return(nil)
		Dispatcher(dispatcher)

		os.Args = args[index]

		go func() {
			time.Sleep(5 * time.Millisecond)
			syscall.Kill(os.Getpid(), syscall.SIGINT)
		}()
		Execute()

		handler.AssertExpectations(t)
		datastore.AssertExpectations(t)
		dispatcher.AssertExpectations(t)
	}
}

func TestFetchCommand(t *testing.T) {
	handler := &helpers.MockHandler{}
	Handler(handler)

	datastore := &helpers.MockDatastore{}
	datastore.On("ClaimNewHost").Return("")
	Datastore(datastore)

	// Set the dispatcher to ensure it doesn't receive any calls
	dispatcher := &helpers.MockDispatcher{}
	Dispatcher(dispatcher)

	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "fetch"}

	go func() {
		time.Sleep(5 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	Execute()

	handler.AssertExpectations(t)
	datastore.AssertExpectations(t)
	dispatcher.AssertExpectations(t)
}

func TestDispatchCommand(t *testing.T) {
	// Set a handler and datastore to ensure they doesn't receive any calls
	handler := &helpers.MockHandler{}
	Handler(handler)

	datastore := &helpers.MockDatastore{}
	Datastore(datastore)

	dispatcher := &helpers.MockDispatcher{}
	dispatcher.On("StartDispatcher").Return(nil)
	dispatcher.On("StopDispatcher").Return(nil)
	Dispatcher(dispatcher)

	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "dispatch"}

	go func() {
		time.Sleep(5 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	Execute()

	handler.AssertExpectations(t)
	datastore.AssertExpectations(t)
	dispatcher.AssertExpectations(t)
}

func TestSeedCommand(t *testing.T) {
	u, _ := walker.ParseURL("http://test.com")
	datastore := &helpers.MockDatastore{}
	datastore.On("StoreParsedURL", u, mock.AnythingOfType("*walker.FetchResults")).Return("")
	Datastore(datastore)

	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "seed", "--url=" + u.String()}

	go func() {
		time.Sleep(5 * time.Millisecond)
		syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	Execute()

	datastore.AssertExpectations(t)
}

func TestSchemaCommand(t *testing.T) {
	orig := os.Args
	defer func() { os.Args = orig }()
	os.Args = []string{os.Args[0], "schema", "--out=test.cql"}
	Execute()
	defer os.Remove("test.cql")

	f, err := ioutil.ReadFile("test.cql")
	if err != nil {
		t.Fatalf("Failed to read test.cql: %v", err)
	}
	if !strings.HasPrefix(string(f), "-- The schema file for walker") {
		t.Fatalf("test.cql has unexpected contents: %v", f)
	}
}
