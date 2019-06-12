package com.yahoo.maha.maha_druid_lookups.query.lookup.DB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MahaDB<T> {
    Logger log = LoggerFactory.getLogger(MahaDB.class);

    T dbObject;

    MahaDB(T dbObject) {
        this.dbObject = dbObject;
    }

    MahaDB() {

    }

    public T getDB() {
        return dbObject;
    }

    void validate() throws IllegalArgumentException {}

    public void put(byte[] var1, byte[] var2) throws Exception {}

    public byte[] get(byte[] var1) throws Exception { return null; }
}
