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
/*
    This Test would create a temp directory with files 1 to file 4 and a sub directory(temp dir) and file5 in it.
    file 1,2 and 5 last modified date is less than retention date and would be deleted.
    Sub directories aren't deleted.
 */
    @Test
    public void testCleanup() throws Exception {
        String[] pathnames;
        new File("target/rocksdbcleanup").mkdirs();
        new File("target/rocksdbcleanup/tempdir").mkdirs();


        Date Retention_date = DateUtils.addDays(new Date(), -12);
        long Retention_date_epoch = Retention_date.getTime();

        File file1 = File.createTempFile( "Test", "1", new File("target/rocksdbcleanup"));
        File file2 = File.createTempFile( "Test", "2", new File("target/rocksdbcleanup"));
        File file3 = File.createTempFile( "Test", "3", new File("target/rocksdbcleanup"));
        File file4 = File.createTempFile( "Test", "4", new File("target/rocksdbcleanup"));
        File file5 = File.createTempFile( "Test", "5", new File("target/rocksdbcleanup/tempdir"));

        file1.setLastModified(Retention_date_epoch);
        file2.setLastModified(Retention_date_epoch);
        file5.setLastModified(Retention_date_epoch);

        FileRetention.cleanup("target/rocksdbcleanup",10);

        File f = new File("target/rocksdbcleanup");
        pathnames = f.list();
        Assert.assertEquals(pathnames.length, 3);
        file3.deleteOnExit();
        file4.deleteOnExit();
    }
}