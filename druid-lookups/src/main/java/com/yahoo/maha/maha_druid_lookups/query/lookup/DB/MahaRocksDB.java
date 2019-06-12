package com.yahoo.maha.maha_druid_lookups.query.lookup.DB;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class MahaRocksDB extends MahaDB<RocksDB> {
    private RocksDB rocksDb;

    public MahaRocksDB(RocksDB rocksDb) {
        this.rocksDb = rocksDb;
    }

    @Override
    public RocksDB getDB() {
        return rocksDb;
    }

    @Override
    public void validate() throws IllegalArgumentException {

    }

    @Override
    public byte[] get(byte[] var1) throws RocksDBException {
        return rocksDb.get(var1);
    }

    @Override
    public void put(byte[] var1, byte[] var2) throws RocksDBException {
        rocksDb.put(var1, var2);
    }
}
