package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import org.apache.commons.lang.time.DateUtils;
import org.apache.druid.java.util.common.logger.Logger;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.stream.Stream;

public class FileRetention {
    private static final Logger LOG = new Logger(FileRetention.class);


    public void cleanup(String dir_path,int num_of_rentention_days) throws IOException {

        Date Retention_date = DateUtils.addDays(new Date(), num_of_rentention_days);
        long Retention_date_epoch = Retention_date.getTime() / 1000L;

        try (Stream<Path> filePathStream = Files.walk(Paths.get(dir_path))) {
            filePathStream.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    if(filePath.toFile().lastModified() <= Retention_date_epoch)
                    {
                        filePath.toFile().delete();
                       LOG.info(filePath.toFile().getName() + "deleted");

                    }

                }
            });

        }
        catch (Exception e)
        {
            LOG.info("Exception while trying to clean up "+ e);
        }


    }
}

