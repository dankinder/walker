Walker
======

An efficient, scalable, continuous crawler leveraging Go/Cassandra

[![Build Status](https://travis-ci.org/iParadigms/walker.svg?branch=master)](https://travis-ci.org/iParadigms/walker)
[![GoDoc](https://godoc.org/github.com/iParadigms/walker?status.svg)](https://godoc.org/github.com/iParadigms/walker)

# Documentation
- [The walker-user mailing
  list](https://groups.google.com/forum/#!forum/walker-user)
- [API GoDoc](http://godoc.org/github.com/iParadigms/walker)
- [Travis CI](https://travis-ci.org/iParadigms/walker)
- [FAQ](FAQ.md)
- [contributing](contributing.md) (see about development, logging, and running
  tests)

# Alpha Warning
This project is a work in progress and not ready for production release. Much
of the design described below is pending development. Stay tuned for an Alpha
release.

# Overview

Walker is a web crawler on it's feet. It has been built from the start to be
horizontally scalable, smart about recrawling, lean on storage, flexible about
what can be done with data, and easy to set up. Use it if you:
- Want a broad or scalable focused crawl of the web
- Want to prioritize what you (re)crawl, and how often
- Want control over where you store crawled data and what you use it for
  (walker needs to store links and metadata, where to store and what to do with
  returned data is up to you)
- Want a smart crawler that will avoid junk (ex. crawler traps)
- Want the performance of Cassandra and flexibility to do batch processing
- Want to crawl non-html file types
- Aren't interested in built-in page rank generation and search indexing (or
  want to do it yourself)

# Architecture in brief

Walker takes advantage of Cassandra's distributed nature to store all links it
has crawled and still needs to crawl. The database holds these links, all
domains we've seen (with metadata), and new segments (groups of links) to crawl
for a given domain.

The *fetcher manager* component claims domains (meaning: fetchers can be
distributed to anywhere they can connect to Cassandra), reads in their
segments, and crawls pages politely, respecting robots.txt rules. It will parse
pages for new links to feed into the system and output crawled content. You can
add your own content processor or use a built-in one like writing pages to
local files.

The *dispatcher* runs batch jobs looking for domains that don't yet have
segments generated, reads the links we already have, and intelligently chooses
a subset to crawl next.

_Note_: the fetchers uses a pluggable *datastore* component to tell it what to
crawl (see the `Datastore` interface). Though the Cassandra datastore is the
primarily supported implementation, the fetchers could be backed by alternative
implementations (in-memory, classic SQL, etc.) that may not need a dispatcher
to run at all.

# Console

Walker comes with a friendly console accessible from the browser. It provides
an easy way to add new links to your crawl and see information about what you
have crawled so far.

![walker-console](https://cloud.githubusercontent.com/assets/5198575/4909655/a0dbc666-6475-11e4-87e5-726502ed2fe7.png)

# Getting started

## Setup

Make sure you have [go installed and a GOPATH set](https://golang.org/doc/install):

```sh
go get github.com/iParadigms/walker/...
```

To get going quickly, you need to install Cassandra. A simple install of
Cassandra on Centos 6 is demonstrated below. See the [datastax
documentation](http://www.datastax.com/documentation/cassandra/2.0/cassandra/install/install_cassandraTOC.html)
non-RHEL-based installs and recommended settings (Oracle Java is recommended
but not required)

```sh
echo "[datastax]
name = DataStax Repo for Apache Cassandra
baseurl = http://rpm.datastax.com/community
enabled = 1
gpgcheck = 0" | sudo tee /etc/yum.repos.d/datastax.repo

sudo yum install java-1.7.0-openjdk dsc20

sudo service cassandra start # it can take a few minutes for this to actually start up
```

In order to run walker and cassandra on your local machine, you may need to
make the following changes to
[cassandra.yaml](http://www.datastax.com/documentation/cassandra/2.0/cassandra/configuration/configCassandra_yaml_r.html):
- Change `listen_address` to empty
- Change `rpc_address` to `0.0.0.0`
- `sudo service cassandra restart`

Once you have this step completed you can go ahead and build the `walker` binary:
```sh
go install ./walker
```

The next step is to build the cassandra schema:

```sh
walker schema -o schema.txt
cqlsh -f schema.txt # optionally change replication information for the keyspace in schema.txt
```

This will create the schema for you. At this point the console can be loaded
via `walker console`

## Basic crawl

Once you've built a `walker` binary, you can crawl with the default handler
easily, which simply writes pages to a directory structure in `$PWD`.

```sh
# These assume walker is in your $PATH
walker crawl # start crawling; runs a fetch manager, dispatcher, and console all-in-one
walker seed -u http://<test_site>.com # give it a seed URL
# Visit http://<your_machine>:3000 in your browser to see the console

# See more help info and other commands:
walker help
```

## Writing your own handler

In most cases you will want to use walker for some kind of processing. The
easiest way is to create a new Go project that implements your own handler. You
can still take advantage of walker's command-line interface. For example:

```go
package main

import (
	"github.com/iParadigms/walker"
	"github.com/iParadigms/walker/cmd"
)

type MyHandler struct{}

func (h *MyHandler) HandleResponse(res *walker.FetchResults) {
	// Do something with the response...
}

func main() {
	cmd.Handler(&MyHandler{})
	cmd.Execute()
}
```

You can then run walker using your own handler easily:

```sh
go run main.go # Has the same CLI as the walker binary
```

## Advanced features and configuration

See [walker.yaml](walker.yaml) for extensive descriptions of the various
configuration parameters available for walker. This file is the primary way of
configuring your crawl. It is not required to exist, but will be read if it is
in the working directory of the walker process or configured with a command
line parameter.

A small sampling of common configuration items:
```yaml
# Fetcher configuration
fetcher:
    # Configure the User-Agent header
    user_agent: Walker (http://github.com/iParadigms/walker)

    # Configure which formats this crawler Accepts
    accept_formats: ["text/html", "text/*"]

    # Which link to accept based on protocol (a.k.a. schema)
    accept_protocols: ["http", "https"]

    # Maximum size of http content
    max_http_content_size_bytes: 20971520 # 20MB

    # Crawl delay duration to use when unspecified by robots.txt. 
    default_crawl_delay: 1s

# Dispatcher configuration
dispatcher:
    # maximum number of links added to segments table per dispatch (must be >0)
    num_links_per_segment: 500

    # refresh_percentage is the percentage of links added per dispatch that have already been crawled.
    # So refresh_percentage = 25 means that 25% of the links added to segments on the next dispatch
    # will be refreshed (i.e. already crawled) links. This value must be >= 0 and <= 100.
    refresh_percentage: 25

# Cassandra configuration for the datastore.
# Generally these are used to create a gocql.ClusterConfig object
# (https://godoc.org/github.com/gocql/gocql#ClusterConfig).
#
cassandra:
    hosts: ["localhost"]
    timeout: "2s"

    # replication_factor is used when defining the initial keyspace.
    # For production clusters we recommend 3 replicas.
    replication_factor: 1

    # Whether to dynamically add new-found domains (or their links) to the crawl (a
    # broad crawl) or discard them, assuming desired domains are manually seeded.
    add_new_domains: false
```

# License

All code contributed to the Walker repository is open source software released
under the BSD 3-clause license. See [LICENSE.txt](LICENSE.txt) for details.
