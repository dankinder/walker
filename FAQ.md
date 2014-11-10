# FAQ

Feel free to update this document as questions arise and are addressed.

## Walker's Cassandra data model

#### It looks like Walker uses Cassandra as a queue for work. Isn't this a known anti-pattern with performance problems?

Using Cassandra with a pattern of frequent deletions can cause performance problems, and yes Walker frequently deletes data as it writes new crawl segments (links to crawl for a given domain) and then deletes them.

Deletes in Cassandra cause two separate problems:
- The larger problem: if you frequently issue queries that could select deleted data if it hadn't been deleted, then your queries will slow way down as they select more and more tombstones
- The lesser problem: for a transient data set, the volume of data will be larger than is immediately obvious

We currently don't face these problems in walker because we set [gc_grace_seconds](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/tabProp.html) to 0, so rows in the segments table will be deleted on the next compaction. This is okay in walker because even if row(s) in the segments table came back due to a [failure condition](http://lostechies.com/ryansvihla/2014/10/20/domain-modeling-around-deletes-or-using-cassandra-as-a-queue-even-when-you-know-better/), we will simply crawl them again. The aforementioned failure condition should also be rare, especially with [hinted handoff](http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_about_hh_c.html).

To briefly illustrate how Walker would behave if gc_grace_seconds were not 0, imagine we dispatch and crawl `test.com` twice per day, and gc_grace_seconds is set to 5 days. We will delete and create segments (500 links by default) each time we crawl, and those deletion records will remain for 5 days. This means 500*2*5 = 5000 links will stay around on disk for `test.com` at a time.

Regarding *selecting* segments: Cassandra will have to ignore 4500 of those 5000 links due to tombstones, which is bearable.

Regarding *disk*: Cassandra will be storing 10x more than is necessary in this case.

In other words, you can set gc_grace_seconds to a non-zero number if you wish, but 0 works more optimaly.
