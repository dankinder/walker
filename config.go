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

	//TODO: allow -1 as a no max value

	Fetcher struct {
		MaxDNSCacheEntries       int      `yaml:"max_dns_cache_entries"`
		UserAgent                string   `yaml:"user_agent"`
		AcceptFormats            []string `yaml:"accept_formats"`
		AcceptProtocols          []string `yaml:"accept_protocols"`
		MaxHTTPContentSizeBytes  int64    `yaml:"max_http_content_size_bytes"`
		IgnoreTags               []string `yaml:"ignore_tags"`
		MaxLinksPerPage          int      `yaml:"max_links_per_page"`
		NumSimultaneousFetchers  int      `yaml:"num_simultaneous_fetchers"`
		BlacklistPrivateIPs      bool     `yaml:"blacklist_private_ips"`
		HttpTimeout              string   `yaml:"http_timeout"`
		HonorMetaNoindex         bool     `yaml:"honor_meta_noindex"`
		HonorMetaNofollow        bool     `yaml:"honor_meta_nofollow"`
		ExcludeLinkPatterns      []string `yaml:"exclude_link_patterns"`
		IncludeLinkPatterns      []string `yaml:"include_link_patterns"`
		DefaultCrawlDelay        string   `yaml:"default_crawl_delay"`
		MaxCrawlDelay            string   `yaml:"max_crawl_delay"`
		PurgeSidList             []string `yaml:"purge_sid_list"`
		ActiveFetchersTTL        string   `yaml:"active_fetchers_ttl"`
		ActiveFetchersCacheratio float32  `yaml:"active_fetchers_cacheratio"`
		ActiveFetchersKeepratio  float32  `yaml:"active_fetchers_keepratio"`
	} `yaml:"fetcher"`

	Dispatcher struct {
		MaxLinksPerSegment       int     `yaml:"num_links_per_segment"`
		RefreshPercentage        float64 `yaml:"refresh_percentage"`
		NumConcurrentDomains     int     `yaml:"num_concurrent_domains"`
		MinLinkRefreshTime       string  `yaml:"min_link_refresh_time"`
		DispatchInterval         string  `yaml:"dispatch_interval"`
		CorrectLinkNormalization bool    `yaml:"correct_link_normalization"`
	} `yaml:"dispatcher"`

	Cassandra struct {
		Hosts                 []string `yaml:"hosts"`
		Keyspace              string   `yaml:"keyspace"`
		ReplicationFactor     int      `yaml:"replication_factor"`
		Timeout               string   `yaml:"timeout"`
		CQLVersion            string   `yaml:"cql_version"`
		ProtoVersion          int      `yaml:"proto_version"`
		Port                  int      `yaml:"port"`
		NumConns              int      `yaml:"num_conns"`
		NumStreams            int      `yaml:"num_streams"`
		DiscoverHosts         bool     `yaml:"discover_hosts"`
		MaxPreparedStmts      int      `yaml:"max_prepared_stmts"`
		AddNewDomains         bool     `yaml:"add_new_domains"`
		AddedDomainsCacheSize int      `yaml:"added_domains_cache_size"`
		StoreResponseBody     bool     `yaml:"store_response_body"`
		NumQueryRetries       int      `yaml:"num_query_retries"`
		//TODO: Currently only exposing values needed for testing; should expose more?
		//Consistency      Consistency
		//Compressor       Compressor
		//Authenticator    Authenticator
		//RetryPolicy      RetryPolicy
		//SocketKeepalive  time.Duration
		//ConnPoolType     NewPoolFunc
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

	Config.Fetcher.MaxDNSCacheEntries = 20000
	Config.Fetcher.UserAgent = "Walker (http://github.com/iParadigms/walker)"
	Config.Fetcher.AcceptFormats = []string{"text/html", "text/*;"} //NOTE you can add quality factors by doing "text/html; q=0.4"
	Config.Fetcher.AcceptProtocols = []string{"http", "https"}
	Config.Fetcher.MaxHTTPContentSizeBytes = 20 * 1024 * 1024 // 20MB
	Config.Fetcher.IgnoreTags = []string{"script", "img", "link"}
	Config.Fetcher.MaxLinksPerPage = 1000
	Config.Fetcher.NumSimultaneousFetchers = 10
	Config.Fetcher.BlacklistPrivateIPs = true
	Config.Fetcher.HttpTimeout = "30s"
	Config.Fetcher.HonorMetaNoindex = true
	Config.Fetcher.HonorMetaNofollow = false
	Config.Fetcher.ExcludeLinkPatterns = nil
	Config.Fetcher.IncludeLinkPatterns = nil
	Config.Fetcher.DefaultCrawlDelay = "1s"
	Config.Fetcher.MaxCrawlDelay = "5m"
	Config.Fetcher.PurgeSidList = nil
	Config.Fetcher.ActiveFetchersTTL = "15m"
	Config.Fetcher.ActiveFetchersCacheratio = 0.75
	Config.Fetcher.ActiveFetchersKeepratio = 0.75

	Config.Dispatcher.MaxLinksPerSegment = 500
	Config.Dispatcher.RefreshPercentage = 25
	Config.Dispatcher.NumConcurrentDomains = 1
	Config.Dispatcher.MinLinkRefreshTime = "0s"
	Config.Dispatcher.DispatchInterval = "10s"
	Config.Dispatcher.CorrectLinkNormalization = false

	Config.Cassandra.Hosts = []string{"localhost"}
	Config.Cassandra.Keyspace = "walker"
	Config.Cassandra.ReplicationFactor = 3
	Config.Cassandra.Timeout = "2s"
	Config.Cassandra.CQLVersion = "3.0.0"
	Config.Cassandra.ProtoVersion = 2
	Config.Cassandra.Port = 9042
	Config.Cassandra.NumConns = 2
	Config.Cassandra.NumStreams = 128
	Config.Cassandra.DiscoverHosts = false
	Config.Cassandra.MaxPreparedStmts = 1000
	Config.Cassandra.AddNewDomains = false
	Config.Cassandra.AddedDomainsCacheSize = 20000
	Config.Cassandra.StoreResponseBody = false
	Config.Cassandra.NumQueryRetries = 3

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

// MustReadConfigFile calls ReadConfigFile and panics on error.
func MustReadConfigFile(path string) {
	err := ReadConfigFile(path)
	if err != nil {
		panic(err.Error())
	}
}

func assertConfigInvariants() error {
	var errs []string
	var err error

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
	_, err = time.ParseDuration(dis.MinLinkRefreshTime)
	if err != nil {
		errs = append(errs, fmt.Sprintf("Dispatcher.MinLinkRefreshTime failed to parse: %v", err))
	}
	_, err = time.ParseDuration(dis.DispatchInterval)
	if err != nil {
		errs = append(errs, fmt.Sprintf("Dispatcher.DispatchInterval failed to parse: %v", err))
	}

	fet := &Config.Fetcher
	_, err = time.ParseDuration(fet.HttpTimeout)
	if err != nil {
		errs = append(errs, fmt.Sprintf("HttpTimeout failed to parse: %v", err))
	}
	_, err = aggregateRegex(fet.ExcludeLinkPatterns, "exclude_link_patterns")
	if err != nil {
		errs = append(errs, err.Error())
	}
	_, err = aggregateRegex(fet.IncludeLinkPatterns, "include_link_patterns")
	if err != nil {
		errs = append(errs, err.Error())
	}
	afTTL, err := time.ParseDuration(fet.ActiveFetchersTTL)
	if err != nil {
		errs = append(errs, fmt.Sprintf("Fetcher.ActiveFetchersTTL failed to parse: %v", err))
	}
	if int(afTTL/time.Second) < 1 {
		errs = append(errs, fmt.Sprintf("Fetcher.ActiveFetchersTTL must be 1s or larger", err))
	}

	def, err := time.ParseDuration(fet.DefaultCrawlDelay)
	if err != nil {
		errs = append(errs, fmt.Sprintf("DefaultCrawlDelay failed to parse: %v", err))
	}
	max, err := time.ParseDuration(fet.MaxCrawlDelay)
	if err != nil {
		errs = append(errs, fmt.Sprintf("MaxCrawlDelay failed to parse: %v", err))
	}
	if def > max {
		errs = append(errs, "Consistency problem: MaxCrawlDelay > DefaultCrawlDealy")
	}

	_, err = time.ParseDuration(Config.Cassandra.Timeout)
	if err != nil {
		errs = append(errs, fmt.Sprintf("Cassandra.Timeout failed to parse: %v", err))
	}

	keeprat := Config.Fetcher.ActiveFetchersKeepratio
	if keeprat < 0 || keeprat >= 1.0 {
		errs = append(errs, fmt.Sprintf("Fetcher.ActiveFetchersKeepratio failed to be in the correct range:"+
			" must choose X such that 0 <= X < 1", err))
	}

	cacherat := Config.Fetcher.ActiveFetchersCacheratio
	if cacherat < 0 || cacherat >= 1.0 {
		errs = append(errs, fmt.Sprintf("Fetcher.ActiveFetchersCacheratio failed to be in the correct range:"+
			" must choose X such that 0 <= X < 1", err))
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

// This function allows code to set up data structures that depend on the
// config. It is always called right after the config file is consumed. But
// it's also public so if you modify the config in a test, you may need to
// call this function. This function is idempotent; so you can call it as many
// times as you like.
func PostConfigHooks() {
	err := setupNormalizeURL()
	if err != nil {
		panic(err)
	}
}

func readConfig() error {
	SetDefaultConfig()

	// See NOTE in SetDefaultConfig regarding sequence values
	Config.Fetcher.AcceptFormats = []string{}
	Config.Fetcher.AcceptProtocols = []string{}
	Config.Fetcher.IgnoreTags = []string{}
	Config.Fetcher.PurgeSidList = []string{}

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
	fet := &Config.Fetcher
	if len(fet.AcceptFormats) == 0 {
		fet.AcceptFormats = []string{"text/html", "text/*;"}
	}
	if len(fet.AcceptProtocols) == 0 {
		fet.AcceptProtocols = []string{"http", "https"}
	}
	if len(fet.IgnoreTags) == 0 {
		fet.IgnoreTags = []string{"script", "img", "link"}
	}
	if len(fet.PurgeSidList) == 0 {
		fet.PurgeSidList = []string{"jsessionid", "phpsessid", "aspsessionid"}
	}

	if len(Config.Cassandra.Hosts) == 0 {
		Config.Cassandra.Hosts = []string{"localhost"}
	}

	err = assertConfigInvariants()
	if err != nil {
		log4go.Info("Loaded config file %v", ConfigName)
	}

	PostConfigHooks()

	return err
}
