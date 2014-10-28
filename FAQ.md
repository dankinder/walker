# FAQ

Feel free to update this document as questions arise and are addressed.

## Walker's Cassandra data model

#### It looks like Walker uses Cassandra as a queue for work. Isn't this a known anti-pattern with performance problems?

Using Cassandra with a pattern of frequent deletions can cause performance problems, and yes Walker frequently deletes data as it writes new crawl segments (links to crawl for a given domain) and then deletes them.

Deletes in Cassandra cause two separate problems:
- The larger problem: if you frequently issue queries that could select deleted data if it hadn't been deleted, then your queries will slow way down as they select more and more tombstones
- The lesser problem: for a transient data set, the volume of data will be larger than is immediately obvious

When accessing new crawl segments, Walker selects specifically by domain, and so does not face extreme query slowdown (the larger problem). It does mean the segments table will be larger on disk than it might look.

To illustrate, imagine we dispatch and crawl `test.com` twice per day, and gc_grace_seconds is set to 5 days. We will delete and create segments (500 links by default) each time we crawl, and those deletion records will remain for 5 days. This means 500*2*5 = 5000 links will stay around on disk for `test.com` at a time.

Regarding *selecting* segments: Cassandra will have to ignore 4500 of those 5000 links due to tombstones, which is bearable.

Regarding *disk*: Cassandra will be storing 10x more than is necessary in this case.

Since segments reads are infrequent (compared to link inserts, for example), we don't consider this a problem, but reducing gc_grace_seconds does help to optimize Walker.
