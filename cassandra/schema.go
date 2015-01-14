package cassandra

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

	-- body stores the content for this link (if cassandra.store_response_body is true)
	body text,

	-- headers stores the http headers for this link (if cassandra.store_response_headers is true)
	headers MAP<text,text>,

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

	-- How many links does this domain have. NOTE: this data item is updated by the dispatcher during dispatch. That
	-- means that this number could be stale if the dispatcher hasn't run recently. uncrawled_links and queued_links
	-- has the same pathology.
	tot_links int,

	-- How many uncrawled links does this domain have. See NOTE over tot_links above.
	uncrawled_links int,

	-- How many links were queued last time the dispatcher updated segments for this
	-- domain. See NOTE over tot_links above.
	queued_links int,


	-- The last time this domain was dispatched
	last_dispatch timestamp,

	-- The last time the dispatcher saw that this domain had no links to dispatch
	last_empty_dispatch timestamp,

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
);

CREATE TABLE {{.Keyspace}}.domain_counters (
	dom text,
	next_crawl counter,
	PRIMARY KEY (dom)
);

CREATE TABLE {{.Keyspace}}.walker_globals (
	key text,
	val int,
	PRIMARY KEY (key)
);`
