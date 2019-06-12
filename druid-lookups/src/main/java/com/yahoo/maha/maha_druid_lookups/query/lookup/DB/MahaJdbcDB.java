package com.yahoo.maha.maha_druid_lookups.query.lookup.DB;

import org.rocksdb.RocksDB;

public class MahaJdbcDB extends MahaDB {
    private RocksDB rocksDb;

    @Override
    public void validate() throws IllegalArgumentException {

    }

    @Override
    public byte[] get(byte[] var1) {

        return new byte[0];
    }

    @Override
    public void put(byte[] var1, byte[] var2) {

    }
}
