// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.rocksdb.RocksDB;

import java.util.concurrent.ConcurrentMap;

public class RocksDBSnapshot {

    public String dbPath;
    @JsonIgnore
    public RocksDB rocksDB;
    public String kafkaConsumerGroupId;
    public ConcurrentMap<Integer, Long> kafkaPartitionOffset;

}
