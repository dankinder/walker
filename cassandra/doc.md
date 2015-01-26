Cassandra Dispatcher and Datastore
==================================

The Cassandra package provides the primary Datastore and Dispatcher
implementation for walker.

# Datastore

As a datastore, Cassandra naturally partitions data across a cluster. The
largest table, `links` stores all links parsed out of pages and crawled. For
more details on the data model see `schema.go` or use the `walker schema`
command.

One key feature provided by Cassandra is clustered storage. Cassadra will
partition the data across a cluster but also stores data in Sorted String
Tables (SSTables), which automatically causes de-duplication of links as they
are inserted (common as pages from the same site are repeatedly fetched).

# Dispatcher

## Overview

The Cassandra Dispatcher continuously iterates domains checking for ones that
need a new link segment generated. It reads in all existing links for the
domain to perform analysis and choose a new set of links to be in the next
segment.

## Link analysis and scheduling rules

A few basic rules determine what links will be chosen next for crawling:

- Links are first filtered according to the domain's normalization rules; see
  "Per-domain link filtering"
- Links parsed from pages that have not yet been crawled will be crawled in
  arbitrary order
- Previously-crawled links will be recrawled in order by age (least recently
  crawled first)
- How many new and previously-crawled links will be added to a segment depends
  on the `refresh_percentage` walker.yaml parameter

## Per-domain link filtering

Within a domain, query filtering rules are constructed to prevent crawling
duplicate data or getting stuck in infinite link loops. For examle a site
`test.com` may have a query parameter `lang=en` that does not affect page
content. Naive crawling would cause twice as much crawling to happen (`/page1`
and `/page1?lang=en` would always both be crawled).

While crawling, Walker generates a hash fingerprint of the downloaded page's
text. During analysis, it discovers which links have the same content
(fingerprint) and path. Comparing these it learns which query parameters do not
affect the page content, and filters those parameters out from similar pages.

For example, suppose the following 2 links produce the same fingerprint:
- test.com/page1.html
- test.com/page1.html?lang=en

And we have a third link:
- test.com/page1.html?lang=en&lang=en

Both links with `lang=en` will be filtered, and only test.com/page1.html will
be crawled.
