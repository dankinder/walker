# FAQ

Feel free to update this document as questions arise and are addressed.

## Cassandra questions

#### How do you tune Cassandra for Walker? What parameters are optimal?

See [the Datastax
documentation](http://www.datastax.com/documentation/cassandra/2.1/cassandra/configuration/configTOC.html)
for basic configuration for getting started and more details on what effect
the following changes have.

*cassandra.yaml*
- Change concurrent_reads to match your CPU profile.
- Increase `tombstone_warn_threshold` (default 1000, suggested 10000), since
  the way segments are selected can easily cause this warning.
- Increase `column_index_size_in_kb` if you have individual sites with millions
  of links or more (default 64, suggested 2048)
- Increase timeouts (`read_request_timeout_in_ms`,
  `range_request_timeout_in_ms`, `write_request_timeout_in_ms`) if needed,
  since Walker does not require requests to have low latency. Suggested 3x
  increase.

*cassandra-env.conf*
- Dispatching can cause many allocations of short-lived objects. Increasing
  HEAP_NEWSIZE (and MAX_HEAP_SIZE accordingly) can help if your walker cluster
  starts to have long GC pauses. See [this
  article](http://tech.shift.com/post/74311817513/cassandra-tuning-the-jvm-for-read-heavy-workloads).

#### It looks like Walker uses Cassandra as a queue for work. Isn't this a known anti-pattern with performance problems?

Using Cassandra with a pattern of frequent deletions can cause performance
problems, and yes Walker frequently deletes data as it writes new crawl
segments (links to crawl for a given domain) and then deletes them.

Deletes in Cassandra cause two separate problems:
- The larger problem: if you frequently issue queries with criteria that would
  select deleted data (if it hadn't yet been deleted), then your queries will
  slow way down as they select more and more tombstones
- The lesser problem: for a transient data set, the volume of data will be
  larger than is immediately obvious

We currently don't face these problems in walker because we set
[gc_grace_seconds](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/tabProp.html)
to 0, so rows in the segments table will be deleted on the next compaction.
This is okay in walker because even if row(s) in the segments table came back
due to a [failure
condition](http://lostechies.com/ryansvihla/2014/10/20/domain-modeling-around-deletes-or-using-cassandra-as-a-queue-even-when-you-know-better/),
we will simply crawl them again. The aforementioned failure condition should
also be rare, especially with [hinted
handoff](http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_about_hh_c.html).

To briefly illustrate how Walker would behave if `gc_grace_seconds` were not 0,
imagine we dispatch and crawl `test.com` twice per day, and `gc_grace_seconds`
is set to 5 days. We will delete and create segments (500 links by default)
each time we crawl, and those deletion records will remain for 5 days. This
means `500*2*5 = 5000` links will stay around on disk for `test.com` at a time.

Regarding *selecting* segments: Cassandra will have to ignore 4500 of those
5000 links due to tombstones, which is bearable.

Regarding *disk*: Cassandra will be storing 10x more than is necessary in this
case.

In other words, you can set `gc_grace_seconds` to a non-zero number if you
wish, but 0 works more optimally.

#### Can I run Walker with Cassandra on (Mac OS X | Windows | etc.)?

Yes, though Walker is primary qualified for Linux. In most cases you can use an
install package from http://planetcassandra.org/cassandra/

However on Mac OS X you may not have a recent enough version of Java. If that
is the case:
- Download the [cassandra tarball](http://planetcassandra.org/cassandra/)
- Install a [newer version of Java](https://www.java.com/en/download/index.jsp)
- Run `bin/cassandra` with JAVA_HOME set to `/Library/Internet
  Plug-Ins/JavaAppletPlugin.plugin/Contents/Home`

In command-line terms:

    <First, manually install Java from https://www.java.com/en/download/index.jsp>
    curl -L http://downloads.datastax.com/community/dsc.tar.gz | tar xz
    cd dsc-cassandra*
    JAVA_HOME="/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home" bin/cassandra

This should sufficiently allow Walker testing and development on your Mac.

#### How do interpret the "priority" integer attached to each domain?

The dispatcher will schedule domains to be crawled based on the value of this priority integer. The rule is that domains
are crawled in proportion to this priority.  For example, consider two domains A and B, where the priority of A is twice
that of B. Then we expect that (approximately) twice as many links from domain A (compared to B) should be dispatched.





