package main

import (
	"fmt"

	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/cassandra"
	"github.com/spf13/cobra"
)

//TODO: auto-reset domain segments based on claim time and
//		remove/deprecate this command

func init() {
	UtilCommand.AddCommand(&cleandbCommand)
}

var cleandbCommand = cobra.Command{
	Use:   "cleandb",
	Short: "Reset dispatched domains to undispatched state",
	Long: `If a crawler has claimed a domain and crashed, it will prevent
crawling that domain until it is manually unclaimed. This tool deletes all
generated segments and resets all domains to the undispatched state
(CassandraDatastore only).
`,
	Run: cleandbFunc,
}

func cleandbFunc(cmd *cobra.Command, args []string) {
	if ConfigPath != "" {
		walker.MustReadConfigFile(ConfigPath)
	}

	ds, err := cassandra.NewDatastore()
	if err != nil {
		panic(fmt.Sprintf("Failed creating Cassandra datastore: %v", err))
	}

	err = ds.UnclaimAll()
	if err != nil {
		panic(err.Error())
	}
}
