// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.yahoo.maha.maha_druid_lookups.query.lookup.dynamic.*;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.rocksdb.*;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ManageLifecycle
public class RocksDBManager {

    private static final Logger LOG = new Logger(RocksDBManager.class);
    private static final ConcurrentMap<String, RocksDBSnapshot> rocksDBSnapshotMap = new ConcurrentHashMap<>();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEMPORARY_PATH = StandardSystemProperty.JAVA_IO_TMPDIR.value();
    private static final String ROCKSDB_LOCATION_PROP_NAME = "localStorageDirectory";
    private static final String ROCKSDB_BLOCK_CACHE_SIZE_PROP_NAME = "blockCacheSize";
    private static final String SNAPSHOT_FILE_NAME = "/rocksDBSnapshot";
    private static final int UPLOAD_LOOKUP_AUDIT_MAX_RETRY = 3;
    private static final Random RANDOM = new Random();
    private static final int BOUND = 6 * 60 * 60 * 1000;
    private static final String STATS_KEY = "rocksdb.stats";
    private static final long DEFAULT_BLOCK_CACHE_SIZE = (long)2 * 1024 * 1024 * 1024;
    private static final Object DYNAMIC_SCHEMA_JSON_FILE = "dynamic-schema.json";

    private String localStorageDirectory;
    private long blockCacheSize;
    private FileSystem fileSystem;

    @Inject
    KafkaManager kafkaExtractionManager;
    @Inject
    ServiceEmitter serviceEmitter;
    @Inject
    DynamicLookupSchemaManager dynamicLookupSchemaManager;

    static {
        RocksDB.loadLibrary();
    }

    @Inject
    public RocksDBManager(final MahaNamespaceExtractionConfig mahaNamespaceExtractionConfig, Configuration config) throws IOException {
        //updating configs - https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
        config.set("fs.hdfs.impl",
                DistributedFileSystem.class.getName()
        );
        config.set("fs.file.impl",
                LocalFileSystem.class.getName()
        );
        this.localStorageDirectory = mahaNamespaceExtractionConfig.getRocksDBProperties().getProperty(ROCKSDB_LOCATION_PROP_NAME, TEMPORARY_PATH);
        this.blockCacheSize = Long.parseLong(mahaNamespaceExtractionConfig.getRocksDBProperties().getProperty(ROCKSDB_BLOCK_CACHE_SIZE_PROP_NAME, String.valueOf(DEFAULT_BLOCK_CACHE_SIZE)));
        Preconditions.checkArgument(blockCacheSize > 0);
        this.fileSystem = FileSystem.get(config);
    }

    public String createDB(final RocksDBExtractionNamespace extractionNamespace,
                           final String lastVersion) throws RocksDBException, IOException {

        String loadTime = LocalDateTime.now().minus(1, ChronoUnit.DAYS)
                .format(DateTimeFormatter.ofPattern("yyyyMMdd0000"));
        final long currentUpdate = Long.parseLong(loadTime);
        final long lastUpdate = lastVersion == null ? 0L : Long.parseLong(lastVersion);

        if (currentUpdate <= lastUpdate) {
            LOG.debug(String.format("currentUpdate [%s] is less than or equal to lastUpdate [%s]",
                    currentUpdate, lastUpdate));
            return loadTime;
        }

        String successMarkerPath = String.format("%s/load_time=%s/_SUCCESS",
                extractionNamespace.getRocksDbInstanceHDFSPath(), loadTime);

        LOG.info(String.format("successMarkerPath [%s], lastUpdate [%s]", successMarkerPath, lastUpdate));

        if (!isFilePresentOnHdfs(successMarkerPath)) {
            if(lastUpdate == 0) {
                LOG.error(String.format("RocksDB instance not present for namespace [%s] loadTime [%s], will check for previous loadTime", extractionNamespace.getNamespace(), loadTime));
                loadTime = LocalDateTime.now().minus(2, ChronoUnit.DAYS)
                        .format(DateTimeFormatter.ofPattern("yyyyMMdd0000"));
                successMarkerPath = String.format("%s/load_time=%s/_SUCCESS",
                        extractionNamespace.getRocksDbInstanceHDFSPath(), loadTime);
                if (!isFilePresentOnHdfs(successMarkerPath)) {
                    LOG.error(String.format("RocksDB instance not present for previous loadTime [%s] too for namespace [%s]", loadTime, extractionNamespace.getNamespace()));
                    serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_ROCKSDB_INSTANCE_NOT_PRESENT, 1));
                    return String.valueOf(lastUpdate);
                }
            } else {
                return String.valueOf(lastUpdate);
            }
        }

        final String hdfsPath = String.format("%s/load_time=%s/rocksdb.zip",
                extractionNamespace.getRocksDbInstanceHDFSPath(), loadTime);

        LOG.error(String.format("hdfsPath [%s]", hdfsPath));

        if(!isRocksDBInstanceCreated(hdfsPath)) {
            serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_ROCKSDB_INSTANCE_NOT_PRESENT, 1));
            return String.valueOf(lastUpdate);
        }

        final File file = new File(String.format("%s/%s", localStorageDirectory, extractionNamespace.getNamespace()));
        if(!file.exists()) {
            FileUtils.forceMkdir(file);
        }

        final String localZippedFileNameWithPath = String.format("%s/%s/%srocksdb_%s.zip",
                localStorageDirectory, extractionNamespace.getNamespace(), loadTime, getLocalPathSuffix(extractionNamespace.isRandomLocalPathSuffixEnabled()));
        LOG.info(String.format("localZippedFileNameWithPath [%s]", localZippedFileNameWithPath));

        final String localPath = FilenameUtils.removeExtension(localZippedFileNameWithPath);

        if(lastUpdate != 0 && !Strings.isNullOrEmpty(extractionNamespace.getKafkaTopic())) {
            // this is non deployment time and kafka is configured to get real time updates, so rocksdb instance download can be delayed
            try {
                int waitTime = RANDOM.nextInt(BOUND);
                LOG.error("Going to sleep for [%s] ms before RocksDB instance is downloaded and kafka messages are applied for [%s]", waitTime, extractionNamespace.getNamespace());
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                LOG.error(e, "Interrupted while sleeping for RocksDB downloading.");
            }
            LOG.info("non-deployment time: starting a new RocksDB instance after sleep for namespace[%s]...", extractionNamespace.getNamespace());
            return startNewInstance(extractionNamespace, loadTime, hdfsPath, localZippedFileNameWithPath, localPath);
        }

        File snapShotFile = new File(localPath + SNAPSHOT_FILE_NAME);

        if(snapShotFile.exists()) {
            try {
                return useSnapshotInstance(extractionNamespace, loadTime, localPath, snapShotFile);
            } catch (Exception e) {
                LOG.error(e, "Caught exception while using the snapshot.");
            }
        }
        LOG.info("starting new instance for namespace[%s]...", extractionNamespace.getNamespace());
        return startNewInstance(extractionNamespace, loadTime, hdfsPath, localZippedFileNameWithPath, localPath);
    }

    private String getLocalPathSuffix(boolean enabled) {
        return enabled ? UUID.randomUUID().toString() + "/" : "";
    }

    private String useSnapshotInstance(final RocksDBExtractionNamespace extractionNamespace,
                                       final String loadTime,
                                       final String localPath,
                                       final File snapShotFile) throws IOException, RocksDBException {

        LOG.error("Snapshot file [%s%s] exists and hence using it", localPath, SNAPSHOT_FILE_NAME);
        RocksDBSnapshot rocksDBSnapshot = OBJECT_MAPPER.readValue(snapShotFile, RocksDBSnapshot.class);
        rocksDBSnapshot.dbPath = localPath;
        rocksDBSnapshot.rocksDB = openRocksDB(rocksDBSnapshot.dbPath);
        rocksDBSnapshot.isRandomLocalPathSuffixEnabled = extractionNamespace.isRandomLocalPathSuffixEnabled();

        rocksDBSnapshotMap.put(extractionNamespace.getNamespace(), rocksDBSnapshot);

        // kafka topic is not empty then add listener for the topic
        if (!Strings.isNullOrEmpty(extractionNamespace.getKafkaTopic())) {
            LOG.info("useSnapshotInstance: adding Listener...");
            kafkaExtractionManager.addListener(extractionNamespace, rocksDBSnapshot.kafkaConsumerGroupId, rocksDBSnapshot.kafkaPartitionOffset, true);
        }
        return loadTime;
    }

    private String startNewInstance(final RocksDBExtractionNamespace extractionNamespace,
                                    final String loadTime,
                                    final String hdfsPath,
                                    final String localZippedFileNameWithPath,
                                    final String localPath) throws IOException, RocksDBException {

        downloadRocksDBInstanceFromHDFS(hdfsPath, localZippedFileNameWithPath);
        unzipFile(localZippedFileNameWithPath);

        RocksDBSnapshot rocksDBSnapshot = new RocksDBSnapshot();
        rocksDBSnapshot.dbPath = localPath;
        rocksDBSnapshot.rocksDB = openRocksDB(rocksDBSnapshot.dbPath);
        rocksDBSnapshot.isRandomLocalPathSuffixEnabled = extractionNamespace.isRandomLocalPathSuffixEnabled();

        // kafka topic is not empty then add listener for the topic
        if (!Strings.isNullOrEmpty(extractionNamespace.getKafkaTopic())) {
            rocksDBSnapshot.kafkaConsumerGroupId = UUID.randomUUID().toString();
            rocksDBSnapshot.kafkaPartitionOffset = new ConcurrentHashMap<Integer, Long>();
            LOG.info("startNewInstance: applying change since beginning...");
            kafkaExtractionManager.applyChangesSinceBeginning(extractionNamespace, rocksDBSnapshot.kafkaConsumerGroupId, rocksDBSnapshot.rocksDB, rocksDBSnapshot.kafkaPartitionOffset);
            LOG.info(rocksDBSnapshot.rocksDB.getProperty(STATS_KEY));

            if (extractionNamespace.isLookupAuditingEnabled()) {
                long sleepTime = 30000;
                int retryCount = 0;
                lookupAuditing(localZippedFileNameWithPath, extractionNamespace, loadTime, sleepTime, retryCount);
            }
            LOG.info("startNewInstance: adding Listener...");
            kafkaExtractionManager.addListener(extractionNamespace, rocksDBSnapshot.kafkaConsumerGroupId, rocksDBSnapshot.kafkaPartitionOffset, false);
        }
        if (extractionNamespace.isDynamicSchemaLookup()) {
            final String schemaHdfsPath = String.format("%s/load_time=%s/%s",
                    extractionNamespace.getRocksDbInstanceHDFSPath(), loadTime, DYNAMIC_SCHEMA_JSON_FILE);
            if (isFilePresentOnHdfs(schemaHdfsPath)) {
                LOG.info("Downloading Dynamic Lookup Schema json from [%s] to [%s]", schemaHdfsPath, localPath);
                fileSystem.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
                LOG.info("Downloaded Dynamic Lookup Schema json from [%s] to [%s]", schemaHdfsPath, localPath);

                String localSchemaPath = String.format("%s/%s", localPath, DYNAMIC_SCHEMA_JSON_FILE);
                // TODO Read Schema and update it to dynamicLookupSchemaManager

            } else {
                LOG.error("Failed to find the Dynamic Lookup Schema json at hdfs path "+schemaHdfsPath);
            }
        }

        final String key = extractionNamespace.getNamespace();
        RocksDB oldDb = null;
        String oldDbPath = null;
        if (rocksDBSnapshotMap.containsKey(key)) {
            oldDb = rocksDBSnapshotMap.get(key).rocksDB;
            oldDbPath = rocksDBSnapshotMap.get(key).dbPath;
        }
        rocksDBSnapshotMap.put(key, rocksDBSnapshot);

        if (oldDb != null) {
            try {
                LOG.info(oldDb.getProperty(STATS_KEY));
                LOG.info("Waiting for 10 seconds before cleaning old DB connection/path");
                Thread.sleep(10000);
                oldDb.close();
                cleanup(oldDbPath);
            } catch (InterruptedException ie) {
                LOG.error(ie, "Exception while cleaning old instance");
            }
        }

        return loadTime;
    }

    private RocksDB openRocksDB(String localPath) throws RocksDBException {

        String optionsFileName = OptionsUtil.getLatestOptionsFileName(localPath, Env.getDefault());

        DBOptions dbOptions = new DBOptions();
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        OptionsUtil.loadOptionsFromFile(localPath + "/" + optionsFileName, Env.getDefault(), dbOptions, columnFamilyDescriptors);

        Preconditions.checkArgument(columnFamilyDescriptors.size() > 0);
        columnFamilyDescriptors.get(0).getOptions().optimizeForPointLookup(blockCacheSize).setMemTableConfig(new HashSkipListMemTableConfig());
        dbOptions.setWalDir(localPath).setAllowConcurrentMemtableWrite(false);

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        RocksDB newDb = RocksDB.open(dbOptions, localPath, columnFamilyDescriptors, columnFamilyHandles);
        LOG.info(newDb.getProperty(STATS_KEY));
        return newDb;
    }

    public RocksDB getDB(final String namespace) {
        return rocksDBSnapshotMap.containsKey(namespace) ? rocksDBSnapshotMap.get(namespace).rocksDB : null;
    }

    @LifecycleStart
    public void start() throws IOException {
        RocksDB.loadLibrary();
        FileUtils.forceMkdir(new File(localStorageDirectory));
    }

    private void cleanup(String path) throws IOException {
        final File file = new File(path);
        if(file.exists()) {
            FileUtils.forceDelete(file);
            LOG.info("Cleaned up [%s]", path);
        }
    }

    @LifecycleStop
    public void stop() throws IOException, InterruptedException {
        kafkaExtractionManager.stop();
        for (String key : rocksDBSnapshotMap.keySet()) {
            RocksDBSnapshot rocksDBSnapshot = rocksDBSnapshotMap.get(key);
            OBJECT_MAPPER.writeValue(new File(rocksDBSnapshot.dbPath + SNAPSHOT_FILE_NAME), rocksDBSnapshot);
        }

        rocksDBSnapshotMap.entrySet().forEach(entry -> {
            RocksDBSnapshot snapshot = entry.getValue();
            snapshot.rocksDB.close();

            if (snapshot.isRandomLocalPathSuffixEnabled) {
                try {
                    cleanup(snapshot.dbPath);
                } catch (IOException e) {
                    LOG.error(e, "Exception while cleaning up %s", snapshot.dbPath);
                }
            }
        });

        if(fileSystem != null) {
            fileSystem.close();
        }
    }

    private boolean isRocksDBInstanceCreated(final String hdfsPath) {
        try {
            Path path = new Path(hdfsPath);
            if (fileSystem.exists(path)) {
                return true;
            }
        } catch (IOException e) {
            LOG.error(e, "IOException");
        }
        return false;
    }

    private boolean isFilePresentOnHdfs(final String successMarkerPath) {
        try {
            Path path = new Path(successMarkerPath);
            if (fileSystem.exists(path)) {
                return true;
            }
            return false;
        } catch (IOException e) {
            LOG.error(e, "IOException");
        }
        return false;
    }

    private void downloadRocksDBInstanceFromHDFS(final String hdfsPath,
                                                 final String localPath) throws IOException {

        LOG.info("Downloading RocksDB instance from [%s] to [%s]", hdfsPath, localPath);
        fileSystem.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
        LOG.info("Downloaded RocksDB instance from [%s] to [%s]", hdfsPath, localPath);
    }

    private void unzipFile(final String localZippedFileNameWithPath) throws IOException {
        LOG.info("Unzipping RocksDB instance [%s]", localZippedFileNameWithPath);
        ZipUtil.unpack(new File(localZippedFileNameWithPath),
                new File(FilenameUtils.removeExtension(localZippedFileNameWithPath)));
        LOG.info("Unzipped RocksDB instance [%s]", localZippedFileNameWithPath);
        cleanup(localZippedFileNameWithPath);
    }

    private void lookupAuditing(final String localZippedFileNameWithPath,
                                final RocksDBExtractionNamespace extractionNamespace, final String loadTime,
                                long sleepTime, int retryCount) {

        final String successMarkerPath = String.format("%s/load_time=%s/_SUCCESS",
                extractionNamespace.getLookupAuditingHDFSPath(), loadTime);

        LOG.info("Success Marker path for auditing : [%s]", successMarkerPath);

        if (retryCount < UPLOAD_LOOKUP_AUDIT_MAX_RETRY) {
            try {
                Thread.sleep(sleepTime);
                final File dirToZip = new File(FilenameUtils.removeExtension(localZippedFileNameWithPath));

                LOG.info("dirToZip: [%s], exists: [%s]", dirToZip, dirToZip.exists());

                if (dirToZip.exists() && !isFilePresentOnHdfs(successMarkerPath)) {

                    final File file = new File(String.format("%s/%s/%s", localStorageDirectory, "lookup_auditing", extractionNamespace.getNamespace()));
                    if (!file.exists()) {
                        FileUtils.forceMkdir(file);
                    }

                    final String localFileNameWithPath = String.format("%s/%s/%s/rocksdb.zip",
                            localStorageDirectory, "lookup_auditing", extractionNamespace.getNamespace());
                    LOG.info(String.format("localFileNameWithPath [%s]", localFileNameWithPath));

                    ZipUtil.pack(dirToZip, new File(localFileNameWithPath));
                    uploadFileForAuditing(extractionNamespace, loadTime,
                            successMarkerPath, localFileNameWithPath);
                    cleanup(String.format("%s/%s/%s", localStorageDirectory, "lookup_auditing", extractionNamespace.getNamespace()));

                    LOG.info("Uploaded lookup for auditing");
                    serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_UPLOAD_LOOKUP_FOR_AUDITING_SUCCESS, 1));
                }

            } catch (Exception e) {
                LOG.error(e, "Caught exception while uploading lookups to HDFS for auditing");
                try {
                    cleanup(String.format("%s/%s/%s", localStorageDirectory, "lookup_auditing", extractionNamespace.getNamespace()));
                    if (!isFilePresentOnHdfs(successMarkerPath)) {
                        fileSystem.delete(new Path(String.format("%s/load_time=%s/rocksdb.zip",
                                extractionNamespace.getLookupAuditingHDFSPath(), loadTime)), false);
                    }
                    sleepTime = 2 * sleepTime;
                } catch (Exception ex) {
                    LOG.error(e, "Exception while cleaning up");
                }

                lookupAuditing(localZippedFileNameWithPath, extractionNamespace, loadTime, sleepTime, ++retryCount);
            }
        } else {
            LOG.error(String.format("Giving up upload after [%s] retries", retryCount));
            serviceEmitter.emit(ServiceMetricEvent.builder().build(MonitoringConstants.MAHA_LOOKUP_UPLOAD_LOOKUP_FOR_AUDITING_FAILURE, 1));
        }
    }

    private void uploadFileForAuditing(RocksDBExtractionNamespace extractionNamespace,
                                       String loadTime, String successMarkerPath, String localFileNameWithPath)
            throws IOException {

        final String hdfsLookupAuditingPath = String.format("%s/load_time=%s/rocksdb.zip",
                extractionNamespace.getLookupAuditingHDFSPath(), loadTime);

        Path path = new Path(String.format("%s/load_time=%s",
                extractionNamespace.getLookupAuditingHDFSPath(), loadTime));
        if(!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
        }

        LOG.info(String.format("hdfsLookupAuditingPath [%s]", hdfsLookupAuditingPath));

        fileSystem.copyFromLocalFile(new Path(localFileNameWithPath), new Path(hdfsLookupAuditingPath));
        final String localSuccessPath = String.format("%s/%s/%s/_SUCCESS",
                localStorageDirectory, "lookup_auditing", extractionNamespace.getNamespace());
        File successFile = new File(localSuccessPath);
        if (!successFile.exists()) {
            new FileOutputStream(successFile).close();
        }
        fileSystem.copyFromLocalFile(new Path(localSuccessPath), new Path(successMarkerPath));

    }

}
