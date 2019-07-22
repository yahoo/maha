# Maha Producer and Consumer Local Lookups Guide

## Setup

These are the versions currently in use for local testing.

Any version updates will succeed as long as Druid Lookups are supported.

**Most of this setup is redundant from the main README**, the main differences being the extra node and the lookup name changes.

- MySQL (Through workbench 8.0)
- [Maha (current)](https://github.com/yahoo/maha)
- [Kafka 2.12-2.2.0]()
- Zookeeper 3.4.11 (Local testing uses Zookeeper inherited with Kafka installation)
- [Druid 0.11.0](http://druid.io/docs/0.11.0/tutorials/quickstart.html)

### MySQL

Load MySQL, noting which port it is attached to.

Create a table with (at minimum) all columns relevant to the lookup

To ensure some initial data to cache, insert one row into the table (don't forget the Timestamp column!  I use CURRENT_TIMESTAMP).

### Zookeeper

Start Zookeeper using its default config (current Kafka has a mini Zookeeper preconfigured):

	```bin/zookeeper-server-start.sh config/zookeeper.properties```

### Kafka

Run the kafka-server-start script (ensure Zookeeper is already running, or Kafka will stall!):

	```bin/kafka-server-start.sh config/server.properties```

- Note: When shutting down Kafka, ensure Zookeeper is running or Kafka will hang in searching-for-config state during shutdown.
- If you want to monitor your Producer using a test consumer, run this command:

	```bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ad_test --from-beginning```

### Maha

Run Maha using mvn clean install.
- make a copy of druid-lookups/target/maha-druid-lookups*.zip in the Druid extensions directory, renamed as maha-druid-lookups.zip.
- Unzip this copy here to create maha-druid-lookups/ containing all potential dependencies.

### Druid

From the default Druid config, several changes will need to be made.
- Under `conf-quickstart/druid/`, make a copy of `historical/`
	- This copy should have *two* differences in **runtime.properties**: A different *port* and a different *lookupTier* name to indicate leader function.
- Under `conf-quickstart/druid/`, make ANOTHER copy of `historical/` (or make modifications to the original)
	- This copy should ALSO have ANOTHER different *port* and ANOTHER different *lookupTier* name to indicate its follower function.
- In `conf-quickstart/druid/common/common.runtime.properties`:
	- Change druid.extensions.loadList to contain at least ```["maha-druid-lookups"]``` (my local version has ONLY this in its loadList since it is all I am testing.)
	- **Add in all variables listed in maha-druid-lookups main Documentation README**.


#### Cluster Startup

Start local Druid with these commands (Hadoop is a required dependency, so we pass it as a command):

Clean start commands:
- `rm -rf var/`
- `rm -rf log/`
- `./bin/init`

Node startups:

- ```java `cat conf-quickstart/druid/coordinator/jvm.config | xargs` -cp "conf-quickstart/druid/_common:conf-quickstart/druid/coordinator:lib/*:hadoop-dependencies/hadoop-client/2.7.3/*" io.druid.cli.Main server coordinator```

- ```java `cat conf-quickstart/druid/historicalLeader/jvm.config | xargs` -cp "conf-quickstart/druid/_common:conf-quickstart/druid/historicalLeader:lib/*:hadoop-dependencies/hadoop-client/2.7.3/*" io.druid.cli.Main server historical```

- ```java `cat conf-quickstart/druid/historicalFollower/jvm.config | xargs` -cp "conf-quickstart/druid/_common:conf-quickstart/druid/historicalFollower:lib/*:hadoop-dependencies/hadoop-client/2.7.3/*" io.druid.cli.Main server historical```

#### Create Lookups

Create your lookups.  Don't forget to create an empty JSON to initialize the Druid environment!

Also, pay close attention to lookupName, as lookup names must be unique!
- empty.json: 

```	
	{}
```

- leaderLookup.json (pay close attention to pollPeriod, as it can be adjusted for testing):

```
{
  "historicalLookupTierLeader": {
    "ad_lookup": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "cachedNamespace",
        "extractionNamespace": {
          "type": "mahajdbcleaderfollower",
          "lookupName": "ad_lookup",
          "connectorConfig": {
            "createTables": false,
            "connectURI": "jdbc:mysql://localhost:3306/test?serverTimezone=UTC&useSSL=false",
            "user": "mySQLUsername",
            "password": "mySQLPassword"
          },
          "kafkaTopic": "ad_test",
          "isLeader": true,
          "kafkaProperties": {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-consumer-group",
            "partitioner.class": "org.apache.kafka.clients.producer.internals.DefaultPartitioner",
            "acks": "1"
          },
          "table": "ad",
          "columnList": [
            "id",
            "gpa",
            "date",
            "title",
            "status",
            "name"
          ],
          "primaryKeyColumn": "id",
          "tsColumn": "last_updated",
          "pollPeriod": "PT1M",
          "cacheEnabled": true
        },
        "firstCacheTimeout": 0
      }
    }
  }
}
```

- followerLookup.json (pay close attention to pollPeriod, as it can be adjusted for testing):

```
{
  "historicalLookupTierFollower": {
    "ad_lookup_consumer": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "cachedNamespace",
        "extractionNamespace": {
          "type": "mahajdbcleaderfollower",
          "lookupName": "ad_lookup_consumer1",
          "connectorConfig": {
            "createTables": false,
            "connectURI": "jdbc:mysql://localhost:3306/test?serverTimezone=UTC&useSSL=false",
            "user": "mySQLUsername",
            "password": "mySQLPassword"
          },
          "kafkaTopic": "ad_test",
          "isLeader": false,
          "kafkaProperties": {
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-consumer-group",
            "partitioner.class": "org.apache.kafka.clients.producer.internals.DefaultPartitioner",
            "acks": "1"
          },
          "table": "ad",
          "columnList": [
            "id",
            "gpa",
            "date",
            "title",
            "status",
            "name"
          ],
          "primaryKeyColumn": "id",
          "tsColumn": "last_updated",
          "pollPeriod": "PT1M",
          "cacheEnabled": true
        },
        "firstCacheTimeout": 0
      }
    }
  }
}
```

#### Load Lookups

Commands to load in new lookups:

- ```curl -XPOST -H'Content-Type: application/json' -d '@test/empty.json' http://localhost:8081/druid/coordinator/v1/lookups/config```
- ```curl -XPOST -H'Content-Type: application/json' -d '@test/leaderLookup.json' http://localhost:8081/druid/coordinator/v1/lookups/config```
- ```curl -XPOST -H'Content-Type: application/json' -d '@test/followerLookup.json' http://localhost:8081/druid/coordinator/v1/lookups/config```

#### Validate Lookups

After being loaded, these lookups can take up to two pollPeriods to run their first lookup.  

Initial lookup runs with default JDBC Lookup behavior on both node types, so its result returns in the form of:
- `Finished loading <number> values for extractionNamespace[<namespaceName>]`. (note there is no space between Namespace and the name)

After this happens, load new values into the mySQL table so that subsequent pollPeriods will load in new information.  
You will see KafkaProducer/Consumer startup values populate & different behavior begin.

- A successful Producer run will write its own cache as well as Kafka output on the topic you select.
- If cache is disabled, neither node will read or produce anything.
- A successful consumer run will READ from the populated Kafka topic.  The consumer does not validate this topic, so only the producer should be writing to it.

Verifying data loaded into the new lookups.  These commands will run namespace queries to return some useful information about the innards of your caches.

Note the **Port number** AND **lookup name** in use, as it should correspond to the *HISTORICAL* you want to hit:

Check value in producer: ```curl "http://localhost:8073/druid/v1/namespaces/ad_lookup_consumer1?namespaceclass=com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespaceWithLeaderAndFollower&key=1234&valueColumn=id"```

Check value in consumer: ```curl "http://localhost:8083/druid/v1/namespaces/ad_lookup?namespaceclass=com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespaceWithLeaderAndFollower&key=1234&valueColumn=name"```

Check cache size: ```curl "http://localhost:8073/druid/v1/namespaces/ad_lookup_consumer1?namespaceclass=com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespaceWithLeaderAndFollower"```

## Maha Local Testing Suite

In order to run local tests within Maha