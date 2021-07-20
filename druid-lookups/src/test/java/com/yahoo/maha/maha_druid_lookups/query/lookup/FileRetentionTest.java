// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup;


import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.FileRetention;
import org.apache.commons.lang.time.DateUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;


public class FileRetentionTest {

    @Test
    public void testCleanup() throws Exception {
        FileRetention FileRenention = new FileRetention();
        String[] pathnames;
        Path tempDirWithPrefix = Files.createTempDirectory("prefix");

        Date Retention_date = DateUtils.addDays(new Date(), -12);
        long Retention_date_epoch = Retention_date.getTime() / 1000L;

        File file1 = File.createTempFile( "Test", "1", new File(String.valueOf(tempDirWithPrefix)));
        File file2 = File.createTempFile( "Test", "2", new File(String.valueOf(tempDirWithPrefix)));
        File file3 = File.createTempFile( "Test", "3", new File(String.valueOf(tempDirWithPrefix)));
        File file4 = File.createTempFile( "Test", "4", new File(String.valueOf(tempDirWithPrefix)));


        file1.setLastModified(Retention_date_epoch);
        file2.setLastModified(Retention_date_epoch);

        FileRenention.cleanup(String.valueOf(tempDirWithPrefix),-10);

        File f = new File(String.valueOf(tempDirWithPrefix));
        pathnames = f.list();
        Assert.assertEquals(pathnames.length, 2);
        file3.deleteOnExit();
        file4.deleteOnExit();

    }













}