package cassandra

import (
	"fmt"
	"sync"

	"github.com/gocql/gocql"
	"github.com/iParadigms/walker"
)

// initdb ensures we only try to create the cassandra schema once
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

	tables := []string{"links", "segments", "domain_info"}
	for _, table := range tables {
		err := db.Query(fmt.Sprintf(`TRUNCATE %v`, table)).Exec()
		if err != nil {
			panic(fmt.Sprintf("Failed to truncate table %v: %v", table, err))
		}
	}

	return db
}
