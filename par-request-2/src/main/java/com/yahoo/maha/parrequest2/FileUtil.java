// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.parrequest2;

import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;

public class FileUtil {

    public static String getDataFromFile(String fileName, Logger logger) {
        StringBuffer buffer = new StringBuffer();
        byte[] bytes = new byte[4096];
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        if (is == null) {
            logger.warn("Config file " + fileName + " cannot be read.");
            return null;
        }
        try {
            while (is.read(bytes) != -1) {
                buffer.append(new String(bytes, "UTF-8"));
            }
        } catch (IOException e) {
            logger.error("IOException caught. Exception -  {}", e);
            return null;
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                logger.error("IOException caught while closing stream. Exception -  {}", e);
            }
        }
        return buffer.toString().trim();
    }
}
