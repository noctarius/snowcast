# ![snowcast](https://raw.githubusercontent.com/noctarius/snowcast/master/snowcast_name.png)

**snowcast** is an auto-configuration, distributed, scalable ID generator on top of [Hazelcast](http://www.hazelcast.org).

### Disclaimer

The snowcast project is not sponsored or started by nor affiliated to Hazelcast, Inc. in any possible way. It is a spare-time project and Hazelcast, even though I employed by Hazelcast, does not offer any support to this project. There is also no commercial support, not at the current point in time. Apart from that I will give support in the best way possible.

## Table of Contents
* [The Problem](#the-problem)
* [Common Solutions](#common-solutions)
* [So What's Now?](#so-whats-now)
* [The Solution](#the-solution)
* [Pseudo Implementation](#pseudo-implementation)
* [Why snowcast?](#why-snowcast)
* [Usage of snowcast](#usage-of-snowcast)
* [Hazelcast Configuration](#hazelcast-configuration)
* [Maven Coordinates](#maven-coordinates)
* [Javadoc](#javadoc)
* [Multithreading](#multithreading)
* [Sequencer States](#sequencer-states)
* [Number of Nodes](#number-of-nodes)
* [Backups](#backups)
* [Migration and Split Brain](#migration-and-split-brain)
* [Hazelcast Clients](#hazelcast-clients)
* [Build Information](#build-information)

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
LOGICAL_NODE_ID = 1234 (Unique Logical Cluster Node Id)
```

Knowing the ID bit offsets generating a new ID is now pretty straight forward:

```plain
currentTimeStamp = 1393823532000 (2014-03-03--05:12:12)

epochDelta = currentTimeStamp - EPOCH_OFFSET => 5289132000

id = epochDelta << (64 - 41)
id |= LOGICAL_NODE_ID << (64 - 41 - 13)
id |= casInc(counter [0, 1024])
```

### Why snowcast?

As you might already have guessed, snowcast is a wordplay based on Snowflake and Hazelcast.

Hazelcast is a distributed cluster environment to offer partitioned in-memory speed. It is the perfect background system to build snowcast on top of. Using Hazelcast, snowcast offers auto-configuration for logical cluster node IDs and fast startup times.

Snowflake was the base of this implementation idea so I love to reflect it in the name and giving credits to the amazing guys at Twitter!

### Usage of snowcast

To use snowcast you obviously need a running Hazelcast cluster. Cluster nodes can easily be integrated into snowcast then.

In snowcast the ID generators are called `SnowcastSequencer`. Those `com.noctarius.snowcast.SnowcastSequencer`s are generated based on a few simple configuration properties that can be passed into the factory function. `SnowcastSequencer`s, as all Hazelcast structures, are referenced by a name. This name is bound to the configuration when the sequencer is first being acquired. They cannot be changed without destroying and recreating the `SnowcastSequencer`.

To retrieve a `SnowcastSequencer` we first have to create a snowcast instance which acts as a factory to create or destroy sequencers.

```java
HazelcastInstance hazelcastInstance = getHazelcastInstance();
Snowcast snowcast = SnowcastSystem.snowcast( hazelcastInstance );
```

In addition to our `com.noctarius.snowcast.Snowcast` factory, a custom epoch must be created to define the offset from the standard linux timestamp. The `com.noctarius.snowcast.SnowcastEpoch` class offers a couple of factory methods to create an epoch from different time sources.

```java
Calendar calendar = GregorianCalendar.getInstance();
calendar.set( 2014, 1, 1, 0, 0, 0 );
SnowcastEpoch epoch = SnowcastEpoch.byCalendar( calendar );
```

Preparations are done by now. Creating a `SnowcastSequencer` using the `Snowcast` factory instance and the epoch, together with a reference name, is now as easy as the following snippet:

```java
SnowcastSequencer sequencer = snowcast.createSequencer( "sequencerName", epoch );
```

That's it, that is the sequencer. It is immediately available to be used to create IDs.

Every call to the `Snowcast::createSequencer` method must pass in the same configuration on every node! A call with a different configuration will result in a `SnowcastSequencerAlreadyRegisteredException` be thrown.

```java
long nextId = sequencer.next();
```

The `SnowcastSequencer::next` operation will return as fast as a ID is available. Depending on how many IDs can be generated per millisecond (to configure generatable IDs, please see [Number of Nodes](#number-of-nodes)) the operation will return immediately with the new ID, if the number of IDs for this millisecond (and node) is exceeded, the method blocks until it can retrieve the next ID. Therefore the method might throw an `InterruptedException` when the thread becomes interrupted while waiting for a new ID. All ID generation is a local only operation, no network interaction is required!

This is basically it, the last step is to destroy sequencers eventually (or shutdown the cluster ;-)). To destroy a `SnowcastSequencer` the following snippet is enough.

```java
snowcast.destroySequencer( sequencer );
```

Destroying a sequencer is a cluster operation and will destroy all sequencers referred to by the same name on all nodes. After that point the existing `SnowcastSequencer` instances are in a destroyed state and cannot be used anymore. To find out more about sequencer states, please read [Sequencer States](#sequencer-states).

### Hazelcast Configuration

Hazelcast requires custom services to be configured upfront using either the Configuration API or by utilizing the, XML based, declarative configuration.

snowcast offers three different ways to register the snowcast service with Hazelcast by providing support for the two already named ones and additionally is equipped with a hack to lazily registers itself as a Hazelcast service on first acquisition. This way is not meant to be used in production, the reason will be shown in a bit.

#### Using the Configuration API

The simplest way to configure snowcast is using the Hazelcast Configuration API. snowcast provides the user with a helper class to configure all necessary options.

If no other configuration is necessary let the snowcast helper create the `com.hazelcast.config.Config` instance for you, using the same way as Hazelcast itself would do it, and retrieve a pre-configured `Config` ready to be used to create a Hazelcast instance.

```java
Config config = SnowcastNodeConfigurator.buildSnowcastAwareConfig();
HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance( config );
```

If there is already a `Config` instance this can be passed in and configured to use snowcast.

```java
Config config = new Config();
config = SnowcastNodeConfigurator.buildSnowcastAwareConfig( config );
HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance( config );
```

#### Using the Declarative Configuration

For people configuring Hazelcast using the XML based configuration only, snowcast also supports a configuration based on declarative configuration. As a disadvantage using the declarative configuration the internal classname of the service is bound to the configuration. Whenever the classname might change for any reason instantiating Hazelcast might fail for an non-obvious reason. Using the [Configuration API](#using-the-configuration-api) is recommended.

```xml
<hazelcast xsi:schemaLocation="http://www.hazelcast.com/schema/config hazelcast-config-3.4.xsd"
           xmlns="http://www.hazelcast.com/schema/config"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">

  <services enable-defaults="true">
    <service enabled="true">
      <name>noctarius::SequencerService</name>
      <class-name>com.noctarius.snowcast.impl.NodeSequencerService</class-name>
    </service>
  </services>
</hazelcast>
```

That way the snowcast service is registered into Hazelcast.

#### Lazy Configuration Hack

This lazy initialization is meant to be used for convenience and the out-of-the-box experience, as the heading suggests this is not meant to be used in production. The reason is simple. All Hazelcast nodes need to have the service registered before the sequencer is retrieved. That said it is hard to achieve this state with more than one cluster node.

No further configuration or user interaction is necessary to use lazy configuration as it happens automatically when no Hazelcast service is yet created. A warning is printed whenever this way of configuration is used. Please do not use it in production.

```plain
                                             __
   _________  ____ _      ___________ ______/ /_
  / ___/ __ \/ __ \ | /| / / ___/ __ `/ ___/ __/
 (__  ) / / / /_/ / |/ |/ / /__/ /_/ (__  ) /_
/____/_/ /_/\____/|__/|__/\___/\__,_/____/\__/

snowcast node mode - version: 1.0.0-SNAPSHOT    build-date: Unknown build-date
WARNING: LAZY CONFIGURATION USED! DO NOT DO THIS IN PRODUCTION!
```

For Technical Operations: To prevent lazy configuration from happening and to provide a fast-fail behavior please set the Java system property:

```plain
-Dcom.noctarius.snowcast.prevent.lazy.configuration
```

The property does not have to have a value, the simple existence of that property is enough to prevent lazy initialization from happening.

### Maven Coordinates

snowcast is deployed as a Apache Maven artifact. All release candidates as well as final releases (GA) are deployed to Maven Central and available directly using Maven, Gradle and similar build systems.

```xml
<dependency>
  <groupId>com.noctarius.snowcast</groupId>
  <artifactId>snowcast</artifactId>
  <version>1.0.0-RC1</version>
</dependency>
```

Automatically build snapshot builds are stored in the OSS Nexus environment at Sonatype, therefore you have to set up an external snapshot repository to your pom.xml as shown in the following snippet:

```xml
<repositories>
  <repository>
    <id>sonatype-nexus-snapshots</id>
    <name>SonaType snapshots repository</name>
    <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>
```

The Maven coordinates for the snowcast artifacts are:

```plain
<dependency>
  <groupId>com.noctarius.snowcast</groupId>
  <artifactId>snowcast</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Javadoc

snowcast build process offers an automatically generated Javadoc. This is, at the time of writing, always a snapshow of the latest build.

The Javadoc snapshot is available here: [Javadoc](http://noctarius.github.io/snowcast/)

### Multithreading

`SnowcastSequencer`s and `Snowcast` factories are threadsafe by design. They are meant to be used by multiple threads concurrently. Sequencers are guaranteed to never generate the same ID twice. Creating and destroying a sequencer is also threadsafe and destroyed sequencers cannot be used anymore after the sequencer was destroyed.

### Sequencer States

Retrieved sequencers can be in three different states. Those states describe if it is possible to generate IDs at a given point in time or not.

Possible states are:

1. `Attached`: A `SnowcastSequencer` in the state `Attached` has a logical node id assigned and **can** be used to generate IDs.
2. `Detached`: A `SnowcastSequencer` in the state `Detached` is not destroyed but **cannot** be used to generate IDs since there is no logical node id assigned.
3. `Destroyed`: A `SnowcastSequencer` in the state `Destroyed` does not have a legal configuration anymore. This instance **can never ever** be used again to generate IDs. A sequencer with the same referral name might be created again at that point.

By default, right after creation of a `SnowcastSequencer`, the state is `Attached`. In this state IDs can be generated by calling `SnowcastSequencer::next`.

At lifetime of the sequencer the state can be changed back and forth from `Attached` to `Detached` (and otherwise) an unlimited number of times. This might be interesting if less logical node ids are configured than actual nodes exist. Nodes can detach themselves whenever there is no need to generate IDs at a given time. Attaching and Detaching are single round-trip remote operations to the owning node of the sequencer.

To detach a sequencer the `SnowcastSequencer::detachLogicalNode` method is called. This call blocks until the owning node of the sequencer has unregistered the logical node id from the calling node. At this point no new IDs can be generated. A call to `SnowcastSequencer::next` will throw a `SnowcastStateException` to indicate that the sequencer is in the wrong state.

To re-attach a sequencer a call to the `SnowcastSequencer::attachLogicalNode` method will perform the necessary assignment operation for a logical node id. Most likely this will **not** be the same logical node id as previously assigned to the node! After the call returns, IDs can be generated again.

Any call to `Snowcast::destroySequencer` will immediately destroy the given sequencer locally **and** remotely. The sequencer cannot be used to generate IDs anymore afterwards. It can also not re-attached anymore!

### Number of Nodes

By default the number of possible nodes defaults to 2^13 (8,192) nodes. This means, as described earlier, that 2^10 (1,024) IDs can be generated per millisecond per node. The overall number of IDs per millisecond is 2^23 (8,388,608) and cannot be changed but it is possible to change the IDs per nodes by decreasing the bits for the logical node ids.

The number of nodes can be set per `SnowcastSequencer` and will, after creation, be part of the provisioned sequencer configuration. It cannot be changed until destroy and recreation of the sequencer. The node count can be set to any power of two between 128 and 8,192. All given non power of two counts will be rounded up to the next power of two. The smaller the number of nodes the bigger the number of IDs per node.

To configure the number of nodes just pass in an additional parameter while creating the sequencer.

```java
SnowcastSequencer sequencer = snowcast.createSequencer( "sequencerName", epoch, 128 );
```

This way only 7 bits are used for the logical node id and the rest can be used to generate IDs, giving a range of 65,536 possible IDs per millisecond and per node.

### Backups

snowcast by default keeps one backup of the internal logicalNodeId assignment tables to provide graceful failover if the normal partition owner dies. The backup will be activated and possibly migrated to the new owner. Afterwards a new backup will be created.

The number of backups itself might be changed by the user when creating the actual snowcast instance as in the following snippet:

```java
HazelcastInstance hazelcastInstance = getHazelcastInstance();
// Configure the snowcast system to use 3 backups instead of 1
Snowcast snowcast = SnowcastSystem.snowcast( hazelcastInstance, 3 );
```

Same is possible on clients:

```java
HazelcastInstance hazelcastInstance = getHazelcastClient();
// Configure the snowcast system to use 3 backups instead of 1
Snowcast snowcast = SnowcastSystem.snowcast( hazelcastInstance, 3 );
```

The number of backups will be part of the internal definition of the sequencers, therefore the number of backups need to stay consistent over all nodes or clients creating the snowcast system. Different number of backups on nodes at creation time will throw an exception explaining that you try to register a different sequencer definition.

The backup count can be any number between 0 and 32767. 0  means to keep no backup at all, in case of node failures, those assignments are lost and the system might assign the same logicalNodeId another time. As long as you won't change the topology as well as attach or detach sequencers you can be sure to stay in a consistent operational state *but* it is highly recommended to gracefully restart the system before making any of the previously mentioned changes.

### Migration and Split Brain

Migration cases, like topology changes, are handled internally by supporting backup partitions. Those backup partitions reside on other nodes inside the cluster and might be moved along on topology changes. In case of node failures, lost localNodeId assignments are recreated using those backups while migration. As discussed before there is, by default, one backup but the number can be changed depending on the number of nodes inside the cluster and the number of nodes you might be able to lose without data loss.

Split brain, on the other hand, cannot be handled gracefully. In [Split Brain](http://en.wikipedia.org/wiki/Split-brain_%28computing%29) situations you might generate the same IDs multiple times since the splitted clusters do not know about each other anymore. If you don't use attach or detach and do not create new members while split brain, you still will be in a consistent state!

### Hazelcast Clients

Hazelcast Clients are handled quite similar to node sequencers. Therefore all of the above is still true and a short example is shown on how to use snowcast with Hazelcast clients.

```java
HazelcastInstance hazelcastClient = getHazelcastClient();
Snowcast snowcast = SnowcastSystem.snowcast( hazelcastClient );

Calendar calendar = GregorianCalendar.getInstance();
calendar.set( 2014, 1, 1, 0, 0, 0 );
SnowcastEpoch epoch = SnowcastEpoch.byCalendar( calendar );

SnowcastSequencer sequencer = snowcast.createSequencer( "sequencerName", epoch );

long nextId = sequencer.next();

snowcast.destroySequencer( sequencer );
```

As an important note, clients behave exactly as cluster nodes. Cluster communication is only necessary in case of changes of the sequencer topologies (creation, destroy of sequencers).

### Build Information

snowcast is build using Jenkins for Continuous Integration. The project build is publically available for review. It contains Code and Test Coverage reports and many more information.

Please find the Jenkins build here: [Jenkins build](https://noctarius.ci.cloudbees.com/job/snowcast/)
