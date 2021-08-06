// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;

import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.FileRetention;
import org.apache.commons.lang.time.DateUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.File;
import java.util.Date;

public class FileRetentionTest {

    @Test
    public void testCleanup() throws Exception {
        String[] pathnames;
        new File("target/rocksdbcleanup").mkdirs();

        Date Retention_date = DateUtils.addDays(new Date(), -12);
        long Retention_date_epoch = Retention_date.getTime() / 1000L;

        File file1 = File.createTempFile( "Test", "1", new File("target/rocksdbcleanup"));
        File file2 = File.createTempFile( "Test", "2", new File("target/rocksdbcleanup"));
        File file3 = File.createTempFile( "Test", "3", new File("target/rocksdbcleanup"));
        File file4 = File.createTempFile( "Test", "4", new File("target/rocksdbcleanup"));

        file1.setLastModified(Retention_date_epoch);
        file2.setLastModified(Retention_date_epoch);

        FileRetention.cleanup("target/rocksdbcleanup",10);

        File f = new File("target/rocksdbcleanup");
        pathnames = f.list();
        Assert.assertEquals(pathnames.length, 2);
        file3.deleteOnExit();
        file4.deleteOnExit();
    }
}