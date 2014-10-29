package test

import (
	"path"
	"reflect"
	"runtime"
	"testing"

	"code.google.com/p/log4go"

	"github.com/iParadigms/walker"
)

func init() {
	// Tests outside of config_test.go also require this configuration to be
	// loaded; Config tests should reset it by making this call
	loadTestConfig("test-walker.yaml")

	// For tests it's useful to see more than the default INFO
	log4go.AddFilter("stdout", log4go.DEBUG, log4go.NewConsoleLogWriter())
}

// loadTestConfig loads the given test config yaml file. The given path is
// assumed to be relative to the `walker/test/` directory, the location of this
// test file.
func loadTestConfig(filename string) {
	_, thisname, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get location of test source file")
	}
	walker.ReadConfigFile(path.Join(path.Dir(thisname), filename))
}

func TestConfigLoading(t *testing.T) {
	defer func() {
		// Reset config for the remaining tests
		loadTestConfig("test-walker.yaml")
	}()

	walker.Config.UserAgent = "Test Agent (set inline)"
	walker.SetDefaultConfig()
	expectedAgentInline := "Walker (http://github.com/iParadigms/walker)"
	if walker.Config.UserAgent != expectedAgentInline {
		t.Errorf("Failed to reset default config value (user_agent), expected: %v\nBut got: %v",
			expectedAgentInline, walker.Config.UserAgent)
	}
	err := walker.ReadConfigFile("test-walker2.yaml")
	if err != nil {
		t.Fatalf(err.Error())
	}
	expectedAgentYaml := "Test Agent (set in yaml)"
	if walker.Config.UserAgent != expectedAgentYaml {
		t.Errorf("Failed to set config value (user_agent) via yaml, expected: %v\nBut got: %v",
			expectedAgentYaml, walker.Config.UserAgent)
	}
}

type ConfigTestCase struct {
	file     string
	expected string
}

var ConfigTestCases = []ConfigTestCase{
	ConfigTestCase{
		"does-not-exist.yaml",
		"Failed to read config file (does-not-exist.yaml): open does-not-exist.yaml: no such file or directory",
	},
	ConfigTestCase{
		"invalid-syntax.yaml",
		"Failed to unmarshal yaml from config file (invalid-syntax.yaml): yaml: line 3: mapping values are not allowed in this context",
	},
	ConfigTestCase{
		"invalid-field-type.yaml",
		"Failed to unmarshal yaml from config file (invalid-field-type.yaml): yaml: unmarshal errors:\n  line 3: cannot unmarshal !!str `what?` into int",
	},
}

func TestConfigLoadingBadFiles(t *testing.T) {
	defer func() {
		// Reset config for the remaining tests
		loadTestConfig("test-walker.yaml")
	}()

	for _, c := range ConfigTestCases {
		err := walker.ReadConfigFile(c.file)
		if err == nil {
			t.Errorf("Expected an error trying to read %v but did not get one", c.file)
		} else if err.Error() != c.expected {
			t.Errorf("Reading config %v, expected: %v\nBut got: %v", c.file, c.expected, err)
		}
	}
}

// TestSequenceOverwrites tests a bug that we hit with go-yaml: for a sequence
// value in the yaml (a list like cassandra.hosts) it would append instead of
// overwriting.
func TestSequenceOverwrites(t *testing.T) {
	defer func() {
		// Reset config for the remaining tests
		loadTestConfig("test-walker.yaml")
	}()

	err := walker.ReadConfigFile("test-cassandra-host.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(walker.Config.Cassandra.Hosts, []string{"other.host.com"}) {
		t.Errorf("Yaml sequence did not properly get overwritten, got %v",
			walker.Config.Cassandra.Hosts)
	}
}
