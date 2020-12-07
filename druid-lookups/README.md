
# Maha Druid Lookups
An extension to druid which provides for MongoDB, JDBC and RocksDB (for high cardinality dimensions) based lookups.  For RocksDB, the lookups provide an interface to update entities via Kafka topics using the same protobuf format utilized for reading the RocksDB lookups.  The default behavior for RocksDB is to grab a snapshot from HDFS daily and apply updates from Kafka from beginning of topic retention period.

# Key Features
* Multi value lookups - Druid's default lookup extension only allows for a simple key/value pair.  Our lookups allow you to have multiple columns for the value.  At registration time, the spec defines the list of columns.  At query time, the extractionFn definition provides which value column to render.
* High cardinality support - Druid's default lookup extension provides both off-heap and on-heap implementations for simple key/value pair.  We extend this by allowing multiple columns and using RocksDB for on SSD lookups with off-heap LRU for high cardinality use cases.
* Lookup service for real-time tasks - Provides a built in lookup service which is used to query lookups on historicals at query time for real-time tasks.  Otherwise, real-time tasks would have to keep local copy of all lookups and that can get costly if they are high cardinality.

#### Assumptions:
* You have some dimension dataset on HDFS in a readable format from a Java application(e.g. CSV, TSV, or some delimited format).
* The dataset is snapshot of all dimension data at some interval. E.g. daily snapshot. Each how has the last updated timestamp column.
* You have a Kafka topic with a TTL at slightly larger then the snapshot interval. E.g. snapshot is every 24 hours, the Kafka topic retains messages for 26 hours.
* You have updates to the dimensions which you can publish to a kafka topic in the same key/value format you create the rocksdb instance (see below) with a valid last updated timestamp from your source of truth system.

#### Steps:
* Define your protobuf message format. Remember to include the last updated timestamp column. Create a jar library which has the java protobuf definitions so you can copy it to the druid historical nodes and put it in the druid libs folder.
* Create a application which reads your dataset from HDFS and creates a rocksdb instance and inserts all the rows into the rocksdb instance in the same format as you expect to read it in maha druid lookups. E.g. the key would just be the String.getBytes() and the value would be the serialized protobuf bytes. Once all rows are inserted close the rocksDb instance, zip it up and upload it to HDFS path.
* Schedule your application to run every day after your dimension snapshots are available.
* Configure maha druid lookup extension on your historicals.
* Register your lookup via the API

## Guide for Protobuf/FlatBuffer based RocksDB lookups as example project
* https://github.com/pranavbhole/maha-druid-lookups-example
 
## Getting Started
Here is tutorial of how to set up maha-druid-lookups as an extensions of Druid in your local box.  
For convenience, we use `/druid` as our druid root path for the following document.
### Requirement
* [druid-0.17.1](http://druid.io/docs/0.17.1/tutorials/quickstart.html)
* zookeeper
* your local datasource (mysql, oracle, etc.)

### Zookeeper setup
#### Download:
```
wget http://www.gtlib.gatech.edu/pub/apache/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
tar -xzf zookeeper-3.4.14.tar.gz
cd zookeeper-3.4.14
cp conf/zoo_sample.cfg conf/zoo.cfg
```
#### Starting up zookeeper:
```
cd zookeeper-3.4.14
./bin/zkServer.sh start
```

_NOTE: for shutting down the zoopkeeper, please use `./bin/zkServer.sh stop`_

### Using **maha-druid-lookups** package
Adding maha druid lookups to druid is simple.  The command below will produce a zip file with all the jars in target directory which you can include in your druid installation's class path.

```
$ mvn clean install //this builds the jar and then assembles the zip file with all dependent jars

$ ls -l target/maha-druid-lookups-* //the zip file can be found with this
-rw-r--r--  1 patelh  user  16084081 Feb 25 12:35 target/maha-druid-lookups-5.242-SNAPSHOT.zip
```

Now unzip assembled zip file and move all the jars to a new repo for your package under`/druid/extensions/`. 
The path will be something like:
```/druid/extensions/maha-druid-lookups/some-jar-file.jar```

### Configuration: using micro-quickstart for quick setup
Here we take advantage of config files under `/druid/conf/druid/single-server/micro-quickstart/`, which is originally for druid tutorial, for our local setup, there are multiple files we need to modify:

#### /druid/conf/druid/single-server/micro-quickstart/_common/common.runtime.properties
1. add package name **maha-druid-lookups** to druid.extensions.loadList:

```druid.extensions.loadList=["extension1", "extension2" , … , "maha-druid-lookups"]```

2. add maha-druid-lookups config:
```
# This is config for auth header factory, if you have your own implementation with whatever method you use to secure druid connections, set it here
druid.lookup.maha.namespace.authHeaderFactory=com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.NoopAuthHeaderFactory

# This is your implementation of protobuf schema factory, only needed for RocksDB based lookups which require protobuf schema
druid.lookup.maha.namespace.schemaFactory=com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.NoopProtobufSchemaFactory

# This is the scheme used by the lookup service, which is used by real-time nodes for looking up on historicals.  Set this to https if using secured druid.
druid.lookup.maha.namespace.lookupService.service_scheme=http

# This is the port your historicals are configured to use, needed by lookup service
druid.lookup.maha.namespace.lookupService.service_port=8083

# List of historicals which can be used by lookup service
druid.lookup.maha.namespace.lookupService.service_nodes=historical

# Local storage directory for rocksdb based lookups
druid.lookup.maha.namespace.rocksdb.localStorageDirectory=/tmp

# Block cache size for rocksdb based lookups
druid.lookup.maha.namespace.rocksdb.blockCacheSize=1048576
```

#### /druid/conf/druid/single-server/micro-quickstart/historical/runtime.properties
add a line for historical lookup tier:
```druid.lookup.lookupTier=historicalLookupTier```

####/druid/conf/druid/single-server/micro-quickstart/broker/runtime.properties (Optional)
add a line for broker lookup tier:
```druid.lookup.lookupTier=brokerLookupTier```

_NOTE: skip this setp if you just want to check the functionality of a lookup and don't need to query it via broker._

#### Include hadoop dependencies in `bin/run-druid`
Add `hadoop-dependencies/hadoop-client/2.8.5/*` into -cp list
```
exec "$JAVA_BIN"/java `cat "$CONFDIR"/"$WHATAMI"/jvm.config | xargs` \
  -cp "$CONFDIR"/"$WHATAMI":"$CONFDIR"/_common:"$CONFDIR"/_common/hadoop-xml:"$CONFDIR"/../_common:"$CONFDIR"/../_common/hadoop-xml:"$WHEREAMI/../lib/*":hadoop-dependencies/hadoop-client/2.8.5/* \
  `cat "$CONFDIR"/$WHATAMI/main.config | xargs`
```

### Starting up Druid
#### Start Druid services: 
```./bin/start-micro-quickstart```

_NOTE: to reset druid for a clean start, do`rm -rf var/* && rm -rf log && ./bin/start-micro-quickstart`_

### Troubleshooting
* JDBC Driver
If encountering the following error:
```
org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException: java.sql.SQLException: No suitable driver found for jdbc:mysql://localhost:3306/...
```

**Solution:** 

Instead of putting the jar under your package repo, you need to include the jdbc connector for your local datasource to  /druid/lib, for example:
`/druid/lib/mysql-connector-java-8.0.16.jar`

* HDFS Configuration
If encounter the following error:
```
Exception in thread "main" com.google.common.util.concurrent.ExecutionError: com.google.common.util.concurrent.ExecutionError: java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration
    at com.google.common.cache.LocalCache$Segment.get(LocalCache.java:2199)
    at com.google.common.cache.LocalCache.get(LocalCache.java:3934)
    …
```
This is caused by lack of Hadoop dependency.  

**Solution:** 

For Druid-0.17.1, it already has the hadoop client jars under `hadoop-dependencies/hadoop-client/2.8.5/*`.  Just make sure you have included the path in `bin/run-druid`

### Registering Druid Lookups
Druid lookups are managed using APIs on coordinators.  Refer [here](http://druid.io/docs/latest/querying/lookups.html).

Example Lookup JSON:

```
// mahajdbc_lookup_config_for_historical.json
{ 
  "historicalLookupTier": { //the tier of the lookup
    "advertiser_lookup": {
      "version": "v0",
      "lookupExtractorFactory": {
        "type": "cachedNamespace",
        "extractionNamespace": {
          "type": "mahajdbc",
          "lookupName": "advertiser_lookup",
          "connectorConfig": {
            "createTables": false,
            "connectURI": "jdbc:mysql://localhost:3306/test?serverTimezone=UTC",
            "user": "jay",
            "password": "jay"
          },
          "table": "advertiser",
          "columnList": [
            "id",
            "status",
            "mdm_company_name"

          ],
          "primaryKeyColumn": "id",
          "tsColumn": "last_updated",
          "pollPeriod": "PT3M",
          "cacheEnabled": true
        },
        "firstCacheTimeout": 0
      }
    }
  }
}
```

_NOTE1: for the details of parameters, please check [here](http://druid.io/docs/0.17.1/development/extensions-core/lookups-cached-global.html)._

_NOTE2: set "cacheEnabled" to true for building cache(hasmap) in the node._

### Sample Commands for lookups
#### Initialization
```
curl -XPOST -H'Content-Type: application/json' -d '{}' http://localhost:8081/druid/coordinator/v1/lookups/config
```

#### Update or Create
For historical lookup:
```
curl -XPOST -H'Content-Type: application/json' -d '@mahajdbc_lookup_config_for_historical.json' http://localhost:8081/druid/coordinator/v1/lookups/config
```

For broker lookup:
```
curl -XPOST -H'Content-Type: application/json' -d '<@mahajdbc_lookup_config_for_broker.json>' http://localhost:8081/druid/coordinator/v1/lookups/config
```

#### Delete
```
curl -XDELETE http://localhost:8081/druid/coordinator/v1/lookups/config/historicalLookupTier/advertiser_lookup
```

#### Get lookup schema
```
curl http://localhost:8081/druid/coordinator/v1/lookups/config/historicalLookupTier/advertiser_lookup
```

#### Query lookup hashmap size (GET request to historical node)
```
curl "http://localhost:8083/druid/v1/namespaces/advertiser_lookup?namespaceclass=com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace"
```

#### Query lookup with key (GET request to historical node)
```
curl "http://localhost:8083/druid/v1/namespaces/advertiser_lookup?namespaceclass=com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace&key=1&valueColumn=status&debug=true"
```

### Example queries using above lookup (require setup for broker)
Example query JSON using lookup for advertisers' status:

```
// query_groupby_lookup.json
{
  "queryType": "groupBy",
  "dataSource": "advertiser_stats",
  "granularity": "day",
  "dimensions": [
    {
      "type": "default",
      "dimension": "id",
      "outputName": "Advertiser ID",
      "outputType": "STRING"
    },
    {
      "type": "extraction",
      "dimension": "id",
      "outputName": "status",
      "outputType": "STRING",
      "extractionFn": {
        "type": "mahaRegisteredLookup",
        "lookup": "advertiser_lookup",
        "retainMissingValue": false,
        "replaceMissingValueWith": "null",
        "injective": false,
        "optimize": true,
        "valueColumn": "status",
        "decode": null,
        "dimensionOverrideMap": {},
        "useQueryLevelCache": false
      }
    }
  ],
  "aggregations": [
    { "type": "longSum", "name": "SPEND", "fieldName": "spend" }
  ],
  "intervals": [ "2015-09-12T00:00:00.000/2015-09-13T00:00:00.000" ]
}
```
Send GET request to broker node:
```
curl -L -H 'Content-Type:application/json' -XPOST --data-binary '@query_groupby_lookup.json' 'http://localhost:8082/druid/v2/?pretty'
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

### Example JSON for mahajdbc lookups using kerberos (e.g. Presto/Hive)
- If your table contains more than one complete snapshot, please specify the partition column in `secondaryTsColumn` in the configuration so that only one snapshot will be used when updating the lookup.
```
{
	"version": "v0",
	"lookupExtractorFactory": {
		"type": "cachedNamespace",
		"extractionNamespace": {
			"type": "mahajdbc",
			"lookupName": "advertiser_lookup",
			"connectorConfig": {
				"createTables": false,
				"connectURI": "jdbc:presto://presto.path:4443/path/path"
			},
			"kerberosProperties": {
				"user": "user1",
				"SSL": "true",
				"SSLTrustStorePath": "/path/to/cert",
				"SSLTrustStorePassword": "changeit",
				"KerberosRemoteServiceName": "HTTP",
				"KerberosPrincipal": "user1@a.b.com",
				"KerberosUseCanonicalHostname": "false",
				"KerberosConfigPath": "/path/to/krb5.conf",
				"KerberosKeytabPath": "/home/user1/user1.keytab"
			},
			"table": "advertiser",
			"columnList": [
				"id",				
				"name"
			],
			"primaryKeyColumn": "id",
			"tsColumn": "last_updated",
			"tsColumnConfig": {
				"name": "last_updated",
				"type": "bigint",
				"format": "yyyyMMddhhmm",
				"secondaryTsColumn": "load_time",
				"secondaryTsColumnCondition": "="
			},

			"pollPeriod": "PT15M",
			"cacheEnabled": true
		},
		"firstCacheTimeout": 600000
	}
}
```
