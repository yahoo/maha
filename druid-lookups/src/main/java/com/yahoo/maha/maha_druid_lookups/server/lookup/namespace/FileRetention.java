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
 * All files older than rentention date would be deleted
 **/
public class FileRetention {
    private static final Logger LOG = new Logger(FileRetention.class);

    public void cleanup(String dir_path,int num_of_rentention_days){

        Date Retention_date = DateUtils.addDays(new Date(), num_of_rentention_days * -1);
        long Retention_date_epoch = Retention_date.getTime() / 1000L;

        try (Stream<Path> filePathStream = Files.walk(Paths.get(dir_path))) {
            filePathStream.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    if(filePath.toFile().lastModified() <= Retention_date_epoch)
                    {
                        try {
                            FileDeleteStrategy.FORCE.delete(filePath.toFile());
                            LOG.info("File " + filePath.toFile().getName() + "deleted at path " + filePath.toFile().getAbsolutePath());
                        } catch (IOException e) {
                            LOG.error("exception when trying to delete " + filePath.toFile().getName() + e);
                        }

                    }
                }
            });
        }
        catch (Exception e)
        {
            LOG.error("Exception while trying to clean up "+ e);
        }
    }
}

