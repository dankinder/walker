package cassandra

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

// GetConfig
func GetConfig() *gocql.ClusterConfig {
	timeout, err := time.ParseDuration(walker.Config.Cassandra.Timeout)
	if err != nil {
		// This shouldn't happen because it is tested in assertConfigInvariants
		panic(err)
	}

	config := gocql.NewCluster(walker.Config.Cassandra.Hosts...)
	config.Keyspace = walker.Config.Cassandra.Keyspace
	config.Timeout = timeout
	config.CQLVersion = walker.Config.Cassandra.CQLVersion
	config.ProtoVersion = walker.Config.Cassandra.ProtoVersion
	config.Port = walker.Config.Cassandra.Port
	config.NumConns = walker.Config.Cassandra.NumConns
	config.NumStreams = walker.Config.Cassandra.NumStreams
	config.DiscoverHosts = walker.Config.Cassandra.DiscoverHosts
	config.MaxPreparedStmts = walker.Config.Cassandra.MaxPreparedStmts
	return config
}

// CreateSchema creates the walker schema in the configured Cassandra database.
// It requires that the keyspace not already exist (so as to losing non-test
// data), with the exception of the walker_test schema, which it will drop
// automatically.
func CreateSchema() error {
	config := GetConfig()
	config.Keyspace = ""
	db, err := config.CreateSession()
	if err != nil {
		return fmt.Errorf("Could not connect to create cassandra schema: %v", err)
	}

	if walker.Config.Cassandra.Keyspace == "walker_test" {
		err := db.Query("DROP KEYSPACE IF EXISTS walker_test").Exec()
		if err != nil {
			return fmt.Errorf("Failed to drop walker_test keyspace: %v", err)
		}
	}

	schema := GetSchema()
	for _, q := range strings.Split(schema, ";") {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		err = db.Query(q).Exec()
		if err != nil {
			return fmt.Errorf("Failed to create schema: %v\nStatement:\n%v", err, q)
		}
	}
	return nil
}

// GetSchema returns the CQL schema for this version of the cassandra
// datastore. Certain values, like keyspace and replication factor, are
// dynamically inserted.
func GetSchema() string {
	t, err := template.New("schema").Parse(schemaTemplate)
	if err != nil {
		// Really shouldn't happen because we build this in
		panic(fmt.Sprintf("Failure parsing the CQL schema template: %v", err))
	}
	var b bytes.Buffer
	t.Execute(&b, walker.Config.Cassandra)
	return b.String()
}

const schemaTemplate string = `-- The schema file for walker
--
-- This file gets generated from a Go template so the keyspace and replication
-- can be configured (particularly for testing purposes)
CREATE KEYSPACE {{.Keyspace}}
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': {{.ReplicationFactor}} };

-- links stores all links we have parsed out of pages and crawled.
--
-- Links found in a page (or inserted with other means) that have not been
-- crawled yet have 'time' set to the epoch (Jan 1 1970). Because 'time' is
-- part of the primary key, Cassandra will deduplicate identical parsed links.
--
-- Every time a link is crawled the results are inserted here. Note that the
-- initial link (with time=epoch) is not overwritten. Rather, for every link,
-- this table contains one row for the initial insert and one for each fetch
-- thereafter. We can effectively see our crawl history for every single link.
CREATE TABLE {{.Keyspace}}.links (
	-- top-level domain plus one component, ex. "google.com"
	dom text,

	-- subdomain, ex. "www" (does not include .)
	subdom text,

	-- path with query parameters, ex. "/index.html?a=b"
	path text,

	-- protocol "http"
	proto text,

	-- time we crawled this link (or epoch, meaning not-yet-fetched)
	time timestamp,

	-- status code of the fetch (null if we did not fetch)
	stat int,

	-- error text, describes the error if we could not fetch (otherwise null)
	err text,

	-- true if this link was excluded from the crawl due to robots.txt rules
	-- (null implies we were not excluded)
	robot_ex boolean,

	-- If this link redirects to another link target, the target link is stored
	-- in this field
	redto_url text,

	-- getnow is true if this link should be queued ASAP to be crawled
	getnow boolean,

	-- mime type, also known as Content-Type (ex. "text/html")
	mime text,

	-- fnv fingerprint, a hash of the page contents for identity comparison
	fnv bigint,

	---- Items yet to be added to walker

	-- structure fingerprint, a hash of the page structure only (defined as:
	-- html tags only, all contents and attributes stripped)
	--structfp bigint,

	-- ip address of the remote server
	--ip text,

	-- referer, maybe can be kept for parsed links
	--ref text,



	-- encoding of the text, ex. "utf8"
	--encoding text,

	PRIMARY KEY (dom, subdom, path, proto, time)
) WITH compaction = { 'class' : 'LeveledCompactionStrategy' }
	AND caching = 'NONE';

-- segments contains groups of links that are ready to be crawled for a given domain.
-- Links belonging to the same domain are considered one segment.
CREATE TABLE {{.Keyspace}}.segments (
	dom text,
	subdom text,
	path text,
	proto text,

	-- time this link was last crawled, so that we can use if-modified-since headers
	time timestamp,

	PRIMARY KEY (dom, subdom, path, proto)
) WITH compaction = { 'class' : 'LeveledCompactionStrategy' }
	AND caching = 'NONE'
	-- Since we delete segments frequently, gc_grace_seconds = 0 indicates that
	-- we should immediately delete the records. In certain failure scenarios
	-- this could cause a deleted row to reappear, but for this table that is
	-- okay, we'll just crawl that link again, no harm.
	-- The performance cost of making this non-zero: D is the frequency (per
	-- second) that we crawl and dispatch a domain, and G is the grace period
	-- defined here (in seconds), then segment queries will cost roughly an
	-- extra factor of D*G in query time
	AND gc_grace_seconds = 0;

CREATE TABLE {{.Keyspace}}.domain_info (
	dom text,

	-- an arbitrary number indicating priority level for crawling this domain.
	-- High priority domains will have segments generated more quickly when they
	-- are exhausted and will be claimed more quickly for crawling
	priority int,

	-- UUID of the crawler that claimed this domain for crawling. This is the
	-- zero UUID if unclaimed (it cannot be null because we index the column).
	claim_tok uuid,

	-- The time this domain was last claimed by a crawler. It remains set after
	-- a crawler unclaims this domain (i.e. if claim_tok is the zero UUID then
	-- claim_time simply means the last time a crawler claimed it, though we
	-- don't know which crawler). Storing claim time is also useful for
	-- unclaiming domains if a crawler is taking too long (implying that it was
	-- stopped abnormally)
	claim_time timestamp, -- define as last time crawled?

	-- true if this domain has had a segment generated and is ready for crawling
	dispatched boolean,

	-- true if this domain is excluded from the crawl (null implies not excluded)
	excluded boolean,
	-- the reason this domain is excluded, null if not excluded
	exclude_reason text,

	-- how many links does this domain have
	tot_links int,

	-- how many uncrawled links does this domain have
	uncrawled_links int,

	-- how many links were queued last time the dispatcher updated segments for this
	-- domain
	queued_links int,

	---- Items yet to be added to walker

	-- If not null, identifies another domain as a mirror of this one
	--mirr_for text,

	PRIMARY KEY (dom)
) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };
CREATE INDEX ON {{.Keyspace}}.domain_info (claim_tok);
CREATE INDEX ON {{.Keyspace}}.domain_info (priority);
CREATE INDEX ON {{.Keyspace}}.domain_info (dispatched);

-- active_fetchers lists the uuids of running fetchers
CREATE TABLE {{.Keyspace}}.active_fetchers (
	tok uuid,
	PRIMARY KEY (tok)
)
`

// initdb ensures we only try to create the cassandra schema once in testing
var initdb sync.Once

// GetTestDB ensures that a cassandra schema is loaded and all data is purged
// for testing purposes. It returns a gocql session or panics if anything
// failed. For safety's sake it may ONLY be used if the cassandra keyspace is
// `walker_test` and will panic if it isn't.
func GetTestDB() *gocql.Session {
	if walker.Config.Cassandra.Keyspace != "walker_test" {
		panic(fmt.Sprintf("Running tests requires using the walker_test keyspace (not %v)",
			walker.Config.Cassandra.Keyspace))
	}

	initdb.Do(func() {
		err := CreateSchema()
		if err != nil {
			panic(err.Error())
		}
	})

	config := GetConfig()
	db, err := config.CreateSession()
	if err != nil {
		panic(fmt.Sprintf("Could not connect to local cassandra db: %v", err))
	}

	tables := []string{"links", "segments", "domain_info", "active_fetchers"}
	for _, table := range tables {
		err := db.Query(fmt.Sprintf(`TRUNCATE %v`, table)).Exec()
		if err != nil {
			panic(fmt.Sprintf("Failed to truncate table %v: %v", table, err))
		}
	}

	return db
}
