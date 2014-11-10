package walker

import (
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	"code.google.com/p/log4go"
)

// Config is the configuration instance the rest of walker should access for
// global configuration values. See WalkerConfig for available config members.
var Config WalkerConfig

// ConfigName is the path (can be relative or absolute) to the config file that
// should be read.
var ConfigName string = "walker.yaml"

func init() {
	err := readConfig()
	if err != nil {
		if strings.Contains(err.Error(), "no such file or directory") {
			log4go.Info("Did not find config file %v, continuing with defaults", ConfigName)
		} else {
			panic(err.Error())
		}
	}
}

// WalkerConfig defines the available global configuration parameters for
// walker. It reads values straight from the config file (walker.yaml by
// default). See sample-walker.yaml for explanations and default values.
type WalkerConfig struct {
	AddNewDomains           bool     `yaml:"add_new_domains"`
	AddedDomainsCacheSize   int      `yaml:"added_domains_cache_size"`
	MaxDNSCacheEntries      int      `yaml:"max_dns_cache_entries"`
	UserAgent               string   `yaml:"user_agent"`
	AcceptFormats           []string `yaml:"accept_formats"`
	AcceptProtocols         []string `yaml:"accept_protocols"`
	DefaultCrawlDelay       int      `yaml:"default_crawl_delay"`
	MaxHTTPContentSizeBytes int64    `yaml:"max_http_content_size_bytes"`
	IgnoreTags              []string `yaml:"ignore_tags"`
	//TODO: allow -1 as a no max value
	MaxLinksPerPage         int      `yaml:"max_links_per_page"`
	NumSimultaneousFetchers int      `yaml:"num_simultaneous_fetchers"`
	BlacklistPrivateIPs     bool     `yaml:"blacklist_private_ips"`
	HttpTimeout             string   `yaml:"http_timeout"`
	HonorMetaNoindex        bool     `yaml:"honor_meta_noindex"`
	HonorMetaNofollow       bool     `yaml:"honor_meta_nofollow"`
	ExcludeLinkPatterns     []string `yaml:"exclude_link_patterns"`
	IncludeLinkPatterns     []string `yaml:"include_link_patterns"`

	Dispatcher struct {
		MaxLinksPerSegment   int     `yaml:"num_links_per_segment"`
		RefreshPercentage    float64 `yaml:"refresh_percentage"`
		NumConcurrentDomains int     `yaml:"num_concurrent_domains"`
	} `yaml:"dispatcher"`

	// TODO: consider these config items
	// allowed schemes (file://, https://, etc.)
	// allowed return content types (or file extensions)
	// http timeout
	// http max delays (how many attempts to give a webserver that's reporting 'busy')
	// http content size limit
	// ftp content limit
	// ftp timeout
	// regex matchers for hosts, paths, etc. to include or exclude
	// max crawl delay (exclude or notify of sites that try to set a massive crawl delay)
	// max simultaneous fetches/crawls/segments

	Cassandra struct {
		Hosts             []string `yaml:"hosts"`
		Keyspace          string   `yaml:"keyspace"`
		ReplicationFactor int      `yaml:"replication_factor"`
		Timeout           string   `yaml:"timeout"`

		//TODO: Currently only exposing values needed for testing; should expose more?
		//CQLVersion       string
		//ProtoVersion     int
		//Timeout          time.Duration
		//Port             int
		//NumConns         int
		//NumStreams       int
		//Consistency      Consistency
		//Compressor       Compressor
		//Authenticator    Authenticator
		//RetryPolicy      RetryPolicy
		//SocketKeepalive  time.Duration
		//ConnPoolType     NewPoolFunc
		//DiscoverHosts    bool
		//MaxPreparedStmts int
		//Discovery        DiscoveryConfig
	} `yaml:"cassandra"`

	Console struct {
		Port              int    `yaml:"port"`
		TemplateDirectory string `yaml:"template_directory"`
		PublicFolder      string `yaml:"public_folder"`
	} `yaml:"console"`
}

// SetDefaultConfig resets the Config object to default values, regardless of
// what was set by any configuration file.
func SetDefaultConfig() {
	// NOTE: go-yaml has a bug where it does not overwrite sequence values
	// (i.e. lists), it appends to them.
	// See https://github.com/go-yaml/yaml/issues/48
	// Until this is fixed, for any sequence value, in readConfig we have to
	// nil it and then fill in the default value if yaml.Unmarshal did not fill
	// anything in

	Config.AddNewDomains = false
	Config.AddedDomainsCacheSize = 20000
	Config.MaxDNSCacheEntries = 20000
	Config.UserAgent = "Walker (http://github.com/iParadigms/walker)"
	Config.AcceptFormats = []string{"text/html", "text/*;"} //NOTE you can add quality factors by doing "text/html; q=0.4"
	Config.AcceptProtocols = []string{"http", "https"}
	Config.DefaultCrawlDelay = 1
	Config.MaxHTTPContentSizeBytes = 20 * 1024 * 1024 // 20MB
	Config.IgnoreTags = []string{"script", "img", "link"}
	Config.MaxLinksPerPage = 1000
	Config.NumSimultaneousFetchers = 10
	Config.BlacklistPrivateIPs = true
	Config.HttpTimeout = "30s"
	Config.HonorMetaNoindex = true
	Config.HonorMetaNofollow = false
	Config.ExcludeLinkPatterns = nil
	Config.IncludeLinkPatterns = nil

	Config.Dispatcher.MaxLinksPerSegment = 500
	Config.Dispatcher.RefreshPercentage = 25
	Config.Dispatcher.NumConcurrentDomains = 1

	Config.Cassandra.Hosts = []string{"localhost"}
	Config.Cassandra.Keyspace = "walker"
	Config.Cassandra.ReplicationFactor = 3
	Config.Cassandra.Timeout = "2s"

	Config.Console.Port = 3000
	Config.Console.TemplateDirectory = "console/templates"
	Config.Console.PublicFolder = "console/public"
}

// ReadConfigFile sets a new path to find the walker yaml config file and
// forces a reload of the config.
func ReadConfigFile(path string) error {
	ConfigName = path
	return readConfig()
}

func assertConfigInvariants() error {
	var errs []string
	dis := &Config.Dispatcher
	if dis.RefreshPercentage < 0.0 || dis.RefreshPercentage > 100.0 {
		errs = append(errs, "Dispatcher.RefreshPercentage must be a floating point number b/w 0 and 100")
	}
	if dis.MaxLinksPerSegment < 1 {
		errs = append(errs, "Dispatcher.MaxLinksPerSegment must be greater than 0")
	}
	if dis.NumConcurrentDomains < 1 {
		errs = append(errs, "Dispatcher.NumConcurrentDomains must be greater than 0")
	}

	_, err := time.ParseDuration(Config.HttpTimeout)
	if err != nil {
		errs = append(errs, fmt.Sprintf("HttpTimeout failed to parse: %v", err))
	}

	_, err = time.ParseDuration(Config.Cassandra.Timeout)
	if err != nil {
		errs = append(errs, fmt.Sprintf("Cassandra.Timeout failed to parse: %v", err))
	}

	_, err = aggregateRegex(Config.ExcludeLinkPatterns, "exclude_link_patterns")
	if err != nil {
		errs = append(errs, err.Error())
	}

	_, err = aggregateRegex(Config.IncludeLinkPatterns, "include_link_patterns")
	if err != nil {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		em := ""
		for _, err := range errs {
			log4go.Error("Config Error: %v", err)
			em += "\t"
			em += err
			em += "\n"
		}
		return fmt.Errorf("Config Error:\n%v\n", em)
	}

	return nil
}

func readConfig() error {
	SetDefaultConfig()

	// See NOTE in SetDefaultConfig regarding sequence values
	Config.AcceptFormats = []string{}
	Config.AcceptProtocols = []string{}
	Config.IgnoreTags = []string{}
	Config.Cassandra.Hosts = []string{}

	data, err := ioutil.ReadFile(ConfigName)
	if err != nil {
		return fmt.Errorf("Failed to read config file (%v): %v", ConfigName, err)
	}
	err = yaml.Unmarshal(data, &Config)
	if err != nil {
		return fmt.Errorf("Failed to unmarshal yaml from config file (%v): %v", ConfigName, err)
	}

	// See NOTE in SetDefaultConfig regarding sequence values
	if len(Config.AcceptFormats) == 0 {
		Config.AcceptFormats = []string{"text/html", "text/*;"}
	}
	if len(Config.AcceptProtocols) == 0 {
		Config.AcceptProtocols = []string{"http", "https"}
	}
	if len(Config.IgnoreTags) == 0 {
		Config.IgnoreTags = []string{"script", "img", "link"}
	}
	if len(Config.Cassandra.Hosts) == 0 {
		Config.Cassandra.Hosts = []string{"localhost"}
	}

	err = assertConfigInvariants()
	if err != nil {
		log4go.Info("Loaded config file %v", ConfigName)
	}
	return err
}
