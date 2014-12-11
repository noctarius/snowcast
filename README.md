# snowcast

**snowcast** is an auto-configuration, distributed, scalable ID generator on top of Hazelcast.

### The Problem

In distributed systems generating unique IDs is a problem. Either calculation is expensive, network traffic is involved or there is a chance of creating unexpected conflicting IDs. Especially the last problem is commonly only recognized when storing data to a relational database. The application would have to recognize this error and handle it gracefully.

### Common Solutions

A common practice for distributed, unique ID generation is to setup a ID generator service. All cluster members connect to that service if they need a new ID. The problem with this approach is the scalability or performance under high load. This may not be a problem for web applications but for low latency and high creation rate systems like gameservices.

Another common problem is that distributed ID generators often pre-acquire big bunches of IDs from a central registry to minimize the network traffic involved. This prevents generated IDs from being sorted by "creation-time" which means items using those IDs can't be ordered by their creation time since the IDs are not consistently increasing.

A third practice is using UUIDs which also is not optimal. UUIDs are not guarateed to be unique but are in 99.99% of all cases. In addition there are multiple ways to generate UUIDs with more likely or unlikely to happen collisions. The latest specification requires native OS calls to gather the mac address to make it part of the UUID which are kind of costly.

### So What's Now?

The goal is now to find an approach that solves the above mentioned problems:
- guaranteed uniqueness
- low network interaction
- order guarantee (natural ordering)
- low latency, high rate generation
 
An approach was offered by Twitter ([Snowflake](https://github.com/twitter/snowflake)) which sadly seems to be discontinued. Still there are other implementations available, also on other languages and operating systems (such as [Instagram](http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram) which doesn't seem to be open sources).

### The Solution

An extremely scalable approach is to generate IDs based on 64 bits and split those bits into multiple parts. The most common approach is to use 3 parts.

```plain
| 41 bits                                | 13 bits    | 10 bits |
```

The first 41 bits store an offset of milliseconds to a customly defined epoch. Using 41 bits offer us about 69 years of milliseconds before we run out of new IDs. This should probably enough for most systems.

The next 13 bits store a unique logical cluster node ID which must be unique for a given point in time. Nodes are not required to retrieve the same cluster node ID over and over again but it must be unique while runtime. 13 bits offer us 8,192 (2^13) unique cluster node IDs.

The last 10 bits store the auto-incrementing counter part. This counter is increasing only per millisecond to guarantee the order of generated IDs to *almost* comply to the natural ordering requirement. Using 10 bits enables us to generate up to 1,024 (2^10) IDs per millisecond per logical cluster node.

The last two parts are able to be changed in the number of bits (e.g. less logical cluster nodes but more IDs per node). In any way this enables us to generate 8,388,608 (2^23) guaranteed unique IDs per millisecond.

### Pseudo Implementation

To set this up we need to define a custom epoch the milliseconds start at, as an example we imagine to start our epoch on January 1st, 2014 (GMT) and we want to generate an ID at March 3rd, 2014 at 5:12:12.

To generate our IDs we need to configure the custom epoch as:

```plain
EPOCH_OFFSET = 1388534400000 (2014-01-01--00:00:00)
```

In addition every cluster node is required to know its own logical cluster node ID:

```plain
SHARD_ID = 1234 (Unique Cluster ID)
```

Knowing the ID bit offsets generating a new ID is now pretty straight forward:

```plain
currentTimeStamp = 1393823532000 (2014-03-03--05:12:12)

epochDelta = currentTimeStamp - EPOCH_OFFSET => 5289132000

id = epochDelta << (64 - 41)
id |= SHARD_ID << (64 - 41 - 13)
id |= casInc(counter [0, 1024])
```

### Why snowcast?

As you might already have guessed, snowcast is a wordplay based on Snowflake and Hazelcast.

Hazelcast is a distributed cluster environment to offer partitioned in-memory speed. It is the perfect background system to build snowcast on top of. Using Hazelcast, snowcast offers auto-configuration for logical cluster node IDs and fast startup times.

Snowflake was the base of this implementation idea so I love to reflect it in the name and giving credits to the amazing guys at Twitter!

### Usage of snowcast

TODO
