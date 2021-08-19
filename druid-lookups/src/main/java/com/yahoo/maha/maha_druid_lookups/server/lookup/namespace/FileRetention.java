package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.lang.time.DateUtils;
import org.apache.druid.java.util.common.logger.Logger;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.stream.Stream;

/**
 * FileRetention to be used to clean up files older than last modifed date.
 * dir_path refers to the directory
 * num_of_rentention_days refers to the number of date from sysdate the files should be retained.
 * All files older than retention date would be deleted.
 * Sub Directory is not deleted.
 **/
public class FileRetention {
    private static final Logger LOG = new Logger(FileRetention.class);

    public static void cleanup(String dir_path,int num_of_rentention_days){

        Date Retention_date = DateUtils.addDays(new Date(), num_of_rentention_days * -1);
        long Retention_date_epoch = Retention_date.getTime();

        try (Stream<Path> filePathStream = Files.walk(Paths.get(dir_path))) {
            filePathStream.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    if(filePath.toFile().lastModified() <= Retention_date_epoch)
                    {
                        try {
                            FileDeleteStrategy.FORCE.delete(filePath.toFile());
                            LOG.info("File [%s] deleted at path [%s] "  ,filePath.toFile().getName(),filePath.toFile().getAbsolutePath());
                        } catch (IOException e) {
                            LOG.error(e,"exception when trying to delete [%s] " ,filePath.toFile().getName());
                        }

                    }
                }
            });
        }
        catch (Exception e)
        {
            LOG.error(e,"Exception while trying to clean up ");
        }
    }
}

