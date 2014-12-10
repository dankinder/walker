/*
Package cmd provides access to build on the walker CLI

This package makes it easy to create custom walker binaries that use their own
Handler, Datastore, or Dispatcher. A crawler that uses the default for each of
these requires simply:

	func main() {
		cmd.Execute()
	}

To create your own binary that uses walker's flags but has its own handler:

	func main() {
		cmd.Handler(NewMyHandler())
		cmd.Execute()
	}

Likewise if you want to set your own Datastore and Dispatcher:

	func main() {
		cmd.DataStore(NewMyDatastore())
		cmd.Dispatcher(NewMyDatastore())
		cmd.Execute()
	}

cmd.Execute() blocks until the program has completed (usually by
being shutdown gracefully via SIGINT).
*/
package cmd

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"code.google.com/p/log4go"

	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/cassandra"
	"github.com/iParadigms/walker/console"
	"github.com/iParadigms/walker/simplehandler"
	"github.com/spf13/cobra"
)

//
// P U B L I C
//

// Handler sets the global handler for this process
func Handler(h walker.Handler) {
	commander.Handler = h
}

// Datastore sets the global datastore for this process
func Datastore(d walker.Datastore) {
	commander.Datastore = d
}

// Dispatcher sets the global dispatcher for this process
func Dispatcher(d walker.Dispatcher) {
	commander.Dispatcher = d
}

// Execute will run the command specified by the command line
func Execute() {
	commander.Execute()
}

//
// P R I V A T E
//

var commander struct {
	*cobra.Command
	Handler    walker.Handler
	Datastore  walker.Datastore
	Dispatcher walker.Dispatcher
}

// config is potentially set by CLI below
var config string

// initCommand performs generic steps to prepare the environment before a
// command, like reading the config file.
func initCommand() {
	if config != "" {
		if err := walker.ReadConfigFile(config); err != nil {
			panic(err.Error())
		}
	}

	if os.Getenv("WALKER_PPROF") == "1" {
		go func() {
			log4go.Debug("pprof enabled, starting http listener")
			err := http.ListenAndServe(":6060", nil)
			if err != nil {
				log4go.Error("Had problem listening for pprof handler: %v", err)
			}
		}()
	}
}

func fatalf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
	os.Exit(1)
}

func init() {
	walkerCommand := &cobra.Command{
		Use: "walker",
	}

	walkerCommand.PersistentFlags().StringVarP(&config,
		"config", "c", "", "path to a config file to load")

	var noConsole bool = false
	crawlCommand := &cobra.Command{
		Use:   "crawl",
		Short: "start an all-in-one crawler",
		Run: func(cmd *cobra.Command, args []string) {
			initCommand()

			if commander.Datastore == nil {
				ds, err := cassandra.NewDatastore()
				if err != nil {
					fatalf("Failed creating Cassandra datastore: %v", err)
				}
				commander.Datastore = ds
				commander.Dispatcher = &cassandra.Dispatcher{}
			}

			if commander.Handler == nil {
				commander.Handler = &simplehandler.Handler{}
			}

			manager := &walker.FetchManager{
				Datastore: commander.Datastore,
				Handler:   commander.Handler,
			}
			go manager.Start()

			if commander.Dispatcher != nil {
				go func() {
					err := commander.Dispatcher.StartDispatcher()
					if err != nil {
						panic(err.Error())
					}
				}()
			}

			if !noConsole {
				console.Start()
			}

			sig := make(chan os.Signal)
			signal.Notify(sig, syscall.SIGINT)
			<-sig

			if commander.Dispatcher != nil {
				commander.Dispatcher.StopDispatcher()
			}
			manager.Stop()
		},
	}
	crawlCommand.Flags().BoolVarP(&noConsole, "no-console", "C", false, "Do not start the console")
	walkerCommand.AddCommand(crawlCommand)

	fetchCommand := &cobra.Command{
		Use:   "fetch",
		Short: "start only a walker fetch manager",
		Run: func(cmd *cobra.Command, args []string) {
			initCommand()

			if commander.Datastore == nil {
				ds, err := cassandra.NewDatastore()
				if err != nil {
					fatalf("Failed creating Cassandra datastore: %v", err)
				}
				commander.Datastore = ds
				commander.Dispatcher = &cassandra.Dispatcher{}
			}

			if commander.Handler == nil {
				commander.Handler = &simplehandler.Handler{}
			}

			manager := &walker.FetchManager{
				Datastore: commander.Datastore,
				Handler:   commander.Handler,
			}
			go manager.Start()

			sig := make(chan os.Signal)
			signal.Notify(sig, syscall.SIGINT)
			<-sig

			manager.Stop()
		},
	}
	walkerCommand.AddCommand(fetchCommand)

	dispatchCommand := &cobra.Command{
		Use:   "dispatch",
		Short: "start only a walker dispatcher",
		Run: func(cmd *cobra.Command, args []string) {
			initCommand()

			if commander.Dispatcher == nil {
				commander.Dispatcher = &cassandra.Dispatcher{}
			}

			go func() {
				err := commander.Dispatcher.StartDispatcher()
				if err != nil {
					panic(err.Error())
				}
			}()

			sig := make(chan os.Signal)
			signal.Notify(sig, syscall.SIGINT)
			<-sig

			commander.Dispatcher.StopDispatcher()
		},
	}
	walkerCommand.AddCommand(dispatchCommand)

	var seedURL string
	seedCommand := &cobra.Command{
		Use:   "seed",
		Short: "add a seed URL to the datastore",
		Long: `Seed is useful for:
    - Adding starter links to bootstrap a broad crawl
    - Adding links when add_new_domains is false
    - Adding any other link that needs to be crawled soon

This command will insert the provided link and also add its domain to the
crawl, regardless of the add_new_domains configuration setting.`,
		Run: func(cmd *cobra.Command, args []string) {
			initCommand()

			orig := walker.Config.Cassandra.AddNewDomains
			defer func() { walker.Config.Cassandra.AddNewDomains = orig }()
			walker.Config.Cassandra.AddNewDomains = true

			if seedURL == "" {
				fatalf("Seed URL needed to execute; add on with --url/-u")
			}
			u, err := walker.ParseURL(seedURL)
			if err != nil {
				fatalf("Could not parse %v as a url: %v", seedURL, err)
			}

			if commander.Datastore == nil {
				ds, err := cassandra.NewDatastore()
				if err != nil {
					fatalf("Failed creating Cassandra datastore: %v", err)
				}
				commander.Datastore = ds
			}

			commander.Datastore.StoreParsedURL(u, nil)
		},
	}
	seedCommand.Flags().StringVarP(&seedURL, "url", "u", "", "URL to add as a seed")
	walkerCommand.AddCommand(seedCommand)

	var outfile string
	schemaCommand := &cobra.Command{
		Use:   "schema",
		Short: "output the walker schema",
		Long: `Schema prints the walker schema to stdout, substituting
schema-relevant configuration items (ex. keyspace, replication factor).
Useful for something like:
    $ <edit walker.yaml as desired>
    $ walker schema -o schema.cql
    $ <edit schema.cql further as desired>
    $ cqlsh -f schema.cql
`,
		Run: func(cmd *cobra.Command, args []string) {
			initCommand()
			if outfile == "" {
				fatalf("An output file is needed to execute; add with --out/-o")
			}

			out, err := os.Create(outfile)
			if err != nil {
				panic(err.Error())
			}
			defer out.Close()

			schema := cassandra.GetSchema()
			fmt.Fprint(out, schema)
		},
	}
	schemaCommand.Flags().StringVarP(&outfile, "out", "o", "", "File to write output to")
	walkerCommand.AddCommand(schemaCommand)

	consoleCommand := &cobra.Command{
		Use:   "console",
		Short: "Start up the walker console",
		Run: func(cmd *cobra.Command, args []string) {
			initCommand()
			console.Run()
		},
	}
	walkerCommand.AddCommand(consoleCommand)

	commander.Command = walkerCommand
}
