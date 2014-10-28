package console

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"code.google.com/p/log4go"
	"github.com/codegangsta/negroni"
	"github.com/gorilla/mux"
	"github.com/iParadigms/walker"
)

//
// Pulled the stopable listener from the link at:
// http://www.hydrogen18.com/blog/stop-listening-http-server-go.html
//
type stoppableListener struct {
	*net.TCPListener          //Wrapped listener
	stopchan         chan int //Channel used only to indicate listener should shutdown
}

func newStopableListner(l net.Listener) (*stoppableListener, error) {
	tcpL, ok := l.(*net.TCPListener)

	if !ok {
		return nil, fmt.Errorf("Cannot wrap listener")
	}

	retval := &stoppableListener{}
	retval.TCPListener = tcpL
	retval.stopchan = make(chan int)

	return retval, nil
}

var stoppedErrorMark = fmt.Errorf("Listener stopped")
var stopingPollTime time.Duration = 1 * time.Second

func (sl *stoppableListener) Accept() (net.Conn, error) {

	for {
		//Wait up to one second for a new connection
		sl.TCPListener.SetDeadline(time.Now().Add(stopingPollTime))
		newConn, err := sl.TCPListener.AcceptTCP()

		//Check for the channel being closed
		select {
		case <-sl.stopchan:
			return nil, stoppedErrorMark
		default:
			//If the channel is still open, continue as normal
		}

		if err != nil {
			netErr, ok := err.(net.Error)

			//If this is a timeout, then continue to wait for
			//new connections
			if ok && netErr.Timeout() && netErr.Temporary() {
				continue
			}
		}

		// Emulate ListenAndServer (This code is taken from http://golang.org/src/pkg/net/http/server.go)
		newConn.SetKeepAlive(true)
		newConn.SetKeepAlivePeriod(3 * time.Minute)

		return newConn, err
	}
}

func (sl *stoppableListener) stop() {
	close(sl.stopchan)
}

//
// We need to be able to keep track of outstanding handler go-routines
//
var shutdownWaitGroup sync.WaitGroup
var shutdownChannel chan struct{}

//buildControllerCounter will wrap a handler, and count outstanding handlers to make sure they
// are all complete. We could time this out if we have straggler handlers. But I see no evidence
// that is needed right now.
func buildControllerCounter(toWrap func(w http.ResponseWriter, req *http.Request)) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		shutdownWaitGroup.Add(1)
		defer shutdownWaitGroup.Done()
		toWrap(w, req)
	}
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// Start the console. NOTE: we only support a single instance of console
// at a time. You must match all your Start() calls with Stop() calls or else
// bad things happen.
func Start() {
	shutdownChannel = make(chan struct{})
	shutdownWaitGroup = sync.WaitGroup{}

	shutdownWaitGroup.Add(1)
	go func() {
		defer shutdownWaitGroup.Done()

		//
		// Do some resource sanity
		//
		if !isDir(walker.Config.Console.TemplateDirectory) {
			dir, err := os.Getwd()
			if err != nil {
				dir = "UNKNOWN"
			}
			err = fmt.Errorf("Unable to locate templates in directory %q (cwd=%q)", walker.Config.Console.TemplateDirectory, dir)
			log4go.Error("CONSOLE PANIC: %v", err)
			panic(err)
		} else {
			log4go.Info("Console setting templates directory to %q", walker.Config.Console.TemplateDirectory)
		}

		if !isDir(walker.Config.Console.PublicFolder) {
			dir, err := os.Getwd()
			if err != nil {
				dir = "UNKNOWN"
			}
			err = fmt.Errorf("Unable to locate public folder in directory %q (cwd=%q)", walker.Config.Console.PublicFolder, dir)
			log4go.Error("CONSOLE PANIC: %v", err)
			panic(err)
		} else {
			log4go.Info("Console setting public folder to %q", walker.Config.Console.PublicFolder)
		}

		//
		// Set up data store
		//
		ds, err := NewCqlModel()
		if err != nil {
			panic(fmt.Errorf("Failed to start data source: %v", err))
		}
		DS = ds
		defer ds.Close()

		//
		// Set up template renderer
		//
		BuildRender()

		//
		// Set up router to point to controllers
		//
		router := mux.NewRouter()
		routes := Routes()
		routes = append(routes, RestRoutes()...)
		for _, route := range routes {
			log4go.Info("Registering path %s", route.Path)
			router.HandleFunc(route.Path, buildControllerCounter(route.Controller))
		}

		//
		// Set up middleware
		//
		neg := negroni.New(negroni.NewRecovery(), negroni.NewLogger(), negroni.NewStatic(http.Dir(walker.Config.Console.PublicFolder)))
		neg.UseHandler(router)

		//
		// Set up stopable listener apparatus
		//
		port := walker.Config.Console.Port

		// Build a stock tcp listener
		originalListener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			log4go.Error("Startup error: %v", err)
			panic(err)
		}

		// Now wrap that listener to include stop functionality
		stopper, err := newStopableListner(originalListener)
		if err != nil {
			log4go.Error("Startup error: %v", err)
			panic(err)
		}

		//
		// Shutdown server on signal or if shutdownChannel is closed
		//
		stop := make(chan os.Signal)
		signal.Notify(stop, syscall.SIGINT)
		go func() {
			select {
			case <-stop:
				log4go.Info("Console caught SIGINT")
			case <-shutdownChannel:
			}
			stopper.stop()
		}()

		//
		// Listen and Serve
		//
		server := http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: neg,
		}
		log4go.Info("Console starting up address http://127.0.0.1:%d/", port)

		// this call will block until stopper.stop() is called
		server.Serve(stopper)

		log4go.Info("Console stopped listening to http://127.0.0.1:%d/", port)
		log4go.Info("Console shutting down ...")
		return
	}()
}

//Stop will stop the console from running. Currently unused, but I'm leaving it here for now, as
// it seems like something one might want to be able to do.
func Stop() {
	close(shutdownChannel)
	shutdownWaitGroup.Wait()
	log4go.Info("Console shutdown complete")
}

//Run will run console until SIGINT is caught
func Run() {
	Start()
	shutdownWaitGroup.Wait()
	log4go.Info("Console shutdown complete")
}
