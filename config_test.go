package walker

import (
	"path"
	"reflect"
	"regexp"
	"testing"

	"code.google.com/p/log4go"
)

func init() {
	// Tests outside of config_test.go also require this configuration to be
	// loaded; Config tests should reset it by making this call
	LoadTestConfig("test-walker.yaml")

	// For tests it's useful to see more than the default INFO
	log4go.AddFilter("stdout", log4go.DEBUG, log4go.NewConsoleLogWriter())
}

func TestConfigLoading(t *testing.T) {
	defer func() {
		// Reset config for the remaining tests
		LoadTestConfig("test-walker.yaml")
	}()

	Config.Fetcher.UserAgent = "Test Agent (set inline)"
	SetDefaultConfig()
	expectedAgentInline := "Walker (http://github.com/iParadigms/walker)"
	if Config.Fetcher.UserAgent != expectedAgentInline {
		t.Errorf("Failed to reset default config value (user_agent), expected: %v\nBut got: %v",
			expectedAgentInline, Config.Fetcher.UserAgent)
	}
	LoadTestConfig("test-walker2.yaml")
	expectedAgentYaml := "Test Agent (set in yaml)"
	if Config.Fetcher.UserAgent != expectedAgentYaml {
		t.Errorf("Failed to set config value (user_agent) via yaml, expected: %v\nBut got: %v",
			expectedAgentYaml, Config.Fetcher.UserAgent)
	}
}

type ConfigTestCase struct {
	file     string
	expected *regexp.Regexp
}

var ConfigTestCases = []ConfigTestCase{
	ConfigTestCase{
		"does-not-exist.yaml",
		regexp.MustCompile("Failed to read config file .*no such file or directory"),
	},
	ConfigTestCase{
		"invalid-syntax.yaml",
		regexp.MustCompile("Failed to unmarshal yaml"),
	},
	ConfigTestCase{
		"invalid-field-type.yaml",
		regexp.MustCompile("Failed to unmarshal yaml"),
	},
}

func TestConfigLoadingBadFiles(t *testing.T) {
	defer func() {
		// Reset config for the remaining tests
		LoadTestConfig("test-walker.yaml")
	}()

	testdir := GetTestFileDir()
	for _, c := range ConfigTestCases {
		err := ReadConfigFile(path.Join(testdir, c.file))
		if err == nil {
			t.Errorf("Expected an error trying to read %v but did not get one", c.file)
		} else if !c.expected.MatchString(err.Error()) {
			t.Errorf("Reading config %v, expected match: %v\nBut got: %v", c.file, c.expected, err)
		}
	}
}

// TestSequenceOverwrites tests a bug that we hit with go-yaml: for a sequence
// value in the yaml (a list like cassandra.hosts) it would append instead of
// overwriting.
func TestSequenceOverwrites(t *testing.T) {
	defer func() {
		// Reset config for the remaining tests
		LoadTestConfig("test-walker.yaml")
	}()

	LoadTestConfig("test-cassandra-host.yaml")
	if !reflect.DeepEqual(Config.Cassandra.Hosts, []string{"other.host.com"}) {
		t.Errorf("Yaml sequence did not properly get overwritten, got %v",
			Config.Cassandra.Hosts)
	}
}
