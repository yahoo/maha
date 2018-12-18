
# Maha Druid Lookups
An extension to druid which provides for JDBC and RocksDB based lookups for high cardinality dimensions.  For RocksDB, the lookups provide an interface to update entities via Kafka topics using the same protobuf format utilized for reading the RocksDB lookups.

# Key Features
TODO

## Getting Started
TODO

## Registering Druid Lookups
TODO

## Auditing lookup integrity
TODO

## Production Environment
Our production environment utilizes both the JDBC and RocksDB based lookups.  The RocksDB based lookups have cardinality of greater than 100 million.  We publish updates to entities via Kafka topics.

### Recommended JAVA options
We recommended heaps of no more than 48GB on historicals.  Here is an example production configuration with 48 core host:

```
-server -Xmx48g -Xms48g -XX:G1HeapRegionSize=32m -XX:+UnlockExperimentalVMOptions -XX:G1NewSizePercent=8 -XX:InitiatingHeapOccupancyPercent=30 -XX:ConcGCThreads=18 -XX:ParallelGCThreads=36 -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -Doracle.net.tns_admin=/etc/conf/tns/ -Djava.security.egd=file:///dev/urandom
```
