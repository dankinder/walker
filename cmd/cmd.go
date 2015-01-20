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
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	// allow http profile
	_ "net/http/pprof"

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

// CommanderStreams holds the i/o functions that the test harness can spoof. This is useful since
// (a) the test harness modifies the normal stdout/stderr streams, and this can cause strange behavior
//     with tests if we then try to modify stdout/stderr to capture.
// (b) there is no good way to spoof os.Exit, short of doing what we're doing by putting a layer of indirection
//     into the strack trace.
type CommanderStreams struct {
	Printf func(format string, args ...interface{})
	Errorf func(format string, args ...interface{})
	Exit   func(status int)
}

// Streams allows user to set global CommandStreams object
func Streams(cstream CommanderStreams) CommanderStreams {
	old := commander.Streams
	commander.Streams = cstream
	return old
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
	Streams    CommanderStreams
}

// config is potentially set by CLI below
var config string

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

	// Set default streams
	if commander.Streams.Printf == nil {
		commander.Streams.Printf = func(format string, args ...interface{}) {
			fmt.Printf(format, args...)
		}
	}
	if commander.Streams.Errorf == nil {
		commander.Streams.Errorf = func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stderr, format, args...)
		}
	}
	if commander.Streams.Exit == nil {
		commander.Streams.Exit = func(status int) {
			os.Exit(status)
		}
	}
}

func fatalf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Println()
	os.Exit(1)
}

// Options to control the readlink command
var readLinkLink string
var readLinkBodyOnly bool
var readLinkMetaOnly bool

// ReadLinkClearOptions allows tests to clear readLink options
func ReadLinkClearOptions() {
	readLinkLink = ""
	readLinkBodyOnly = false
	readLinkMetaOnly = false
}

var readLinkCommand = &cobra.Command{
	Use:   "readlink",
	Short: "Print information about a link",
	Run: func(cmd *cobra.Command, args []string) {
		initCommand()
		printf := commander.Streams.Printf
		errorf := commander.Streams.Errorf
		exit := commander.Streams.Exit

		if readLinkLink == "" {
			errorf("Failed to specify link to read; add --url/-u to your call\n")
			exit(1)
		}
		if readLinkBodyOnly && readLinkMetaOnly {
			errorf("Can't specify both --body-only/-b AND --meta-only/-m\n")
			exit(1)
		}

		if commander.Datastore == nil {
			ds, err := cassandra.NewDatastore()
			if err != nil {
				errorf("Failed creating Cassandra datastore: %v\n", err)
				exit(1)
			}
			commander.Datastore = ds
		}

		mds, ok := commander.Datastore.(cassandra.ModelDatastore)
		if !ok {
			errorf("Tried to use pre-configured datastore, but couldn't upgrade it to a cassandra.ModelDatastore\n")
			exit(1)
		}

		u, err := walker.ParseURL(readLinkLink)
		if err != nil {
			errorf("Failed to parse link %v: %v\n", readLinkLink, err)
			exit(1)
		}

		linfo, err := mds.FindLink(u, true)
		if err != nil {
			errorf("Failed FindLink: %v\n", err)
			exit(1)
		} else if linfo == nil {
			errorf("Failed to find link %v in datastore\n", readLinkLink)
			exit(1)
		}

		if linfo.CrawlTime.Equal(walker.NotYetCrawled) {
			printf("Link %v is present, but has not yet been fetched\n", readLinkLink)
			exit(0)
		}

		if !readLinkBodyOnly {
			estring := "\n"
			if linfo.Error != "" {
				lines := strings.Split(linfo.Error, "\n")
				for _, l := range lines {
					estring += fmt.Sprintf("    %v\n", l)
				}
			}

			printf("Url:                %v\n", linfo.URL)
			printf("HttpStatus:         %v\n", linfo.Status)
			printf("CrawlTime:          %v\n", linfo.CrawlTime)
			printf("Error:              %v", estring)
			printf("RobotsExcluded:     %v\n", linfo.RobotsExcluded)
			printf("RedirectedTo:       %v\n", linfo.RedirectedTo)
			printf("GetNow:             %v\n", linfo.GetNow)
			printf("Mime:               %v\n", linfo.Mime)
			printf("FnvFingerprint:     %v\n", linfo.FnvFingerprint)
			printf("FnvTextFingerprint: %v\n", linfo.FnvTextFingerprint)
			if linfo.Headers == nil {
				printf("HEADERS:        <none>\n")
			} else {
				// Gotta sort these keys so that printout is reproducible
				keys := []string{}
				for k := range linfo.Headers {
					keys = append(keys, k)
				}
				sort.Strings(keys)

				printf("HEADERS:\n")
				for _, k := range keys {
					for _, v := range linfo.Headers[k] {
						printf("    %v: %v\n", k, v)
					}
				}
			}
		}

		if !readLinkMetaOnly {
			if !readLinkBodyOnly {
				if linfo.Body == "" {
					printf("BODY:           <none>\n")
				} else {
					printf("BODY:\n%v\n", linfo.Body)
				}
			} else {
				printf("%v\n", linfo.Body)
			}
		}
		exit(0)
	},
}

func init() {
	walkerCommand := &cobra.Command{
		Use: "walker",
	}

	walkerCommand.PersistentFlags().StringVarP(&config,
		"config", "c", "", "path to a config file to load")

	var noConsole = false
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
			u, err := walker.ParseAndNormalizeURL(seedURL)
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

	readLinkCommand.Flags().StringVarP(&readLinkLink, "url", "u", "", "Url to lookup")
	readLinkCommand.Flags().BoolVarP(&readLinkBodyOnly, "body-only", "b", false,
		"Use this flag to get the http body only")
	readLinkCommand.Flags().BoolVarP(&readLinkMetaOnly, "meta-only", "m", false,
		"Use this flag to omit the body from printed results")
	walkerCommand.AddCommand(readLinkCommand)

	commander.Command = walkerCommand
}
