
# Maha Druid Lookups
An extension to druid which provides for MongoDB, JDBC and RocksDB (for high cardinality dimensions) based lookups.  For RocksDB, the lookups provide an interface to update entities via Kafka topics using the same protobuf format utilized for reading the RocksDB lookups.  The default behavior for RocksDB is to grab a snapshot from HDFS daily and apply updates from Kafka from beginning of topic retention period.

# Key Features
* Multi value lookups - Druid's default lookup extension only allows for a simple key/value pair.  Our lookups allow you to have multiple columns for the value.  At registration time, the spec defines the list of columns.  At query time, the extractionFn definition provides which value column to render.
* High cardinality support - Druid's default lookup extension provides both off-heap and on-heap implementations for simple key/value pair.  We extend this by allowing multiple columns and using RocksDB for on SSD lookups with off-heap LRU for high cardinality use cases.
* Lookup service for real-time tasks - Provides a built in lookup service which is used to query lookups on historicals at query time for real-time tasks.  Otherwise, real-time tasks would have to keep local copy of all lookups and that can get costly if they are high cardinality.

## Getting Started
Adding maha druid lookups to druid is simple.  The command below will produce a zip file with all the jars in target directory which you can include in your druid installation's class path.

```
#this builds the jar and then assembles the zip file with all dependent jars
mvn clean install

#the zip file can be found with this:
ls -l target/maha-druid-lookups-* 

-rw-r--r--  1 patelh  user  16084081 Feb 25 12:35 target/maha-druid-lookups-5.242-SNAPSHOT.zip


#now copy the zip file to your build server and unzip to get all the jars needed for the lookups and put them in druid jars directory.
```

### Configuration in common runtime.properties

```
# This is config for auth header factory, if you have your own implementation with whatever method you use to secure druid connections, set it here
druid.lookup.maha.namespace.authHeaderFactory=com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.NoopAuthHeaderFactory

# This is your implementation of protobuf schema factory, only needed for RocksDB based lookups which require protobuf schema
druid.lookup.maha.namespace.schemaFactory=com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.NoopProtobufSchemaFactory

# This is the scheme used by the lookup service, which is used by real-time nodes for looking up on historicals.  Set this to https if using secured druid.
druid.lookup.maha.namespace.lookupService.service_scheme=http

# This is the port your historicals are configured to use, needed by lookup service
druid.lookup.maha.namespace.lookupService.service_port=4080

# List of historicals which can be used by lookup service
druid.lookup.maha.namespace.lookupService.service_nodes=hist1,hist2,hist3

# Local storage directory for rocksdb based lookups
druid.lookup.maha.namespace.rocksdb.localStorageDirectory=/tmp

# Block cache size for rocksdb based lookups
druid.lookup.maha.namespace.rocksdb.blockCacheSize=1048576
```

## Registering Druid Lookups
Druid lookups are managed using APIs on coordinators.  Refer [here](http://druid.io/docs/latest/querying/lookups.html).

Example POST to /druid/coordinator/v1/lookups/config :

```
{
  "__default": {
    "advertiser_lookup": {
      "version": "v1",
      "lookupExtractorFactory": {
        "type": "cachedNamespace",
        "firstCacheTimeout": 180000,
        "injective": false,
        "extractionNamespace": {
          "type": "mahamongo",
          "connectorConfig": {
            "hosts": "1.1.1.1:1111,1.1.1.2:1111",
            "dbName": "mydb",
            "user": "myuser",
            "password": {
              "type": "default",
              "password": "mypassword"
            },
            "clientOptions": {
              "socketTimeout": 180000
            }
          },
          "collectionName": "advertiser",
          "tsColumn": "updated_at",
          "tsColumnEpochInteger": true,
          "pollPeriod": "PT60S",
          "cacheEnabled": true,
          "lookupName": "advertiser_lookup",
          "documentProcessor": {
            "type": "flatmultivalue",
            "columnList": [
              "name",
              "country",
              "currency"
            ],
            "primaryKeyColumn": "_id"
          },
          "mongoClientRetryCount": 3
        }
      }
    }
  }
}
```

### Example queries using above lookup

Example query using lookup in filter, we filter advertisers by doing a lookup on country:

```
{
	"dataSource": "advertiser_stats",
	"queryType": "topN",
	"aggregations": [{
		"type": "doubleSum",
		"name": "spend"
	}],
	"context": {
		"timeout": 30000,
		"queryId": "abcd"
	},
	"filter": {
		"type": "and",
		"fields": [{
			"type": "selector",
			"dimension": "advertiser_id",
			"value": "US",
			"extractionFn": {
				"type": "mahaRegisteredLookup",
				"lookup": "advertiser_lookup",
				"retainMissingValue": false,
				"replaceMissingValueWith": "N/A",
				"injective": false,
				"optimize": true,
				"valueColumn": "country",
				"dimensionOverrideMap": {},
				"useQueryLevelCache": false
			}
		}]
	},
	"granularity": "all",
	"threshold": 10,
	"metric": "spend",
	"dimension": "advertiser_id",
	"intervals": [
		"2019-02-25T00:00:00.000Z/2019-02-26T00:00:00.000Z"
	]
}
```

## Auditing lookup integrity
TODO

## Production Environment
Our production environment utilizes both the JDBC and RocksDB based lookups.  The RocksDB based lookups have cardinality of greater than 100 million.  We publish updates to entities via Kafka topics.

### Recommended JAVA options
We recommended heaps of no more than 48GB on historicals.  Here is an example production configuration with 48 core host:

```
-server -Xmx48g -Xms48g -XX:G1HeapRegionSize=32m -XX:+UnlockExperimentalVMOptions -XX:G1NewSizePercent=8 -XX:InitiatingHeapOccupancyPercent=30 -XX:ConcGCThreads=18 -XX:ParallelGCThreads=36 -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -Doracle.net.tns_admin=/etc/conf/tns/ -Djava.security.egd=file:///dev/urandom
```
