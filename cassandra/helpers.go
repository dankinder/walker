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

// GetConfig returns a fresh ClusterConfig, configured against walker.Config
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
	config.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: walker.Config.Cassandra.NumQueryRetries}
	return config
}

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

		panic(fmt.Sprintf("Failure parsing the CQL schema template: %v", err))
	}
	var b bytes.Buffer
	t.Execute(&b, walker.Config.Cassandra)
	return b.String()
}
