// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.metamx.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunner;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.Objects;

@JsonTypeName("maharocksdb")
public class RocksDBExtractionNamespace implements ExtractionNamespace {

    Logger LOG = new Logger(RocksDBExtractionNamespace.class);

    @JsonProperty
    private final String rocksDbInstanceHDFSPath;
    @JsonProperty
    private final String lookupAuditingHDFSPath;
    @JsonProperty
    private final String namespace;
    @JsonProperty
    private final Period pollPeriod;
    @JsonProperty
    private final String kafkaTopic;
    @JsonProperty
    private boolean cacheEnabled = true;
    @JsonProperty
    private boolean lookupAuditingEnabled = false;
    @JsonProperty
    private final String lookupName;
    @JsonProperty
    private String tsColumn = "last_updated";
    @JsonProperty
    private final MissingLookupConfig missingLookupConfig;

    private Long lastUpdatedTime = -1L;

    @JsonProperty
    public String cacheActionRunnerName = "";

    private CacheActionRunner cacheActionRunner = null;

    @JsonCreator
    public RocksDBExtractionNamespace(@NotNull @JsonProperty(value = "namespace", required = true)
                                              String namespace,
                                      @NotNull @JsonProperty(value = "rocksDbInstanceHDFSPath", required = true) final String rocksDbInstanceHDFSPath,
                                      @NotNull @JsonProperty(value = "lookupAuditingHDFSPath", required = true) final String lookupAuditingHDFSPath,
                                      @Min(0) @JsonProperty(value = "pollPeriod", required = true)
                                              Period pollPeriod,
                                      @NotNull @JsonProperty(value = "kafkaTopic", required = false) final String kafkaTopic,
                                      @JsonProperty(value = "cacheEnabled", required = false) final boolean cacheEnabled,
                                      @JsonProperty(value = "lookupAuditingEnabled", required = false) final boolean lookupAuditingEnabled,
                                      @NotNull @JsonProperty(value = "lookupName", required = true) final String lookupName,
                                      @Nullable @JsonProperty(value = "tsColumn", required = false) final String tsColumn,
                                      @NotNull @JsonProperty(value = "missingLookupConfig", required = false) final MissingLookupConfig missingLookupConfig,
                                      @JsonProperty(value = "cacheActionRunner", required = false) final String cacheActionRunnerName) {
        this.rocksDbInstanceHDFSPath = Preconditions.checkNotNull(rocksDbInstanceHDFSPath, "rocksDbInstanceHDFSPath");
        this.lookupAuditingHDFSPath = Preconditions.checkNotNull(lookupAuditingHDFSPath, "lookupAuditingHDFSPath");
        this.namespace = Preconditions.checkNotNull(namespace, "namespace");
        this.pollPeriod = Preconditions.checkNotNull(pollPeriod, "pollPeriod");
        this.kafkaTopic = kafkaTopic;
        this.missingLookupConfig = missingLookupConfig;
        this.cacheEnabled = cacheEnabled;
        this.lookupAuditingEnabled = lookupAuditingEnabled;
        this.lookupName = lookupName;
        this.tsColumn = tsColumn;

        //cacheActionRunner = "."
        if(StringUtils.isBlank(cacheActionRunnerName)) //no runner is passed, use Default Class String
            this.cacheActionRunnerName = CacheActionRunner.class.getName();
        else {
            try { //Check if the passed in runner is valid, else throw an exception to stop the program.
                Object actionRunner = Class.forName(cacheActionRunnerName).newInstance();
                Preconditions.checkArgument(actionRunner instanceof CacheActionRunner,
                        "Passed in runner should be a CacheActionRunner, but got a " + cacheActionRunnerName + " of class " + actionRunner.getClass().getName());
                this.cacheActionRunnerName =  cacheActionRunnerName;
            } catch (Throwable t) {
                LOG.error("Found a blank or invalid CacheActionRunner, logging error and throwing Runtime ", t);
                throw new RuntimeException("Found invalid passed in CacheActionRunner with String " + cacheActionRunner, t);
            }
        }
    }

    public String getRocksDbInstanceHDFSPath() {
        return rocksDbInstanceHDFSPath;
    }

    public String getLookupAuditingHDFSPath() {
        return lookupAuditingHDFSPath;
    }

    //@Override
    public String getNamespace() {
        return namespace;
    }

    @Override
    public long getPollMs() {
        return pollPeriod.toStandardDuration().getMillis();
    }

    @Override
    public String getLookupName() {
        return lookupName;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public MissingLookupConfig getMissingLookupConfig() {
        return missingLookupConfig;
    }

    //@Override
    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public boolean isLookupAuditingEnabled() {
        return lookupAuditingEnabled;
    }

    public Long getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void setLastUpdatedTime(Long lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public String getTsColumn() {
        return tsColumn;
    }

    public String getCacheActionRunnerName() { return cacheActionRunnerName; }

    public CacheActionRunner getCacheActionRunner() {
        try {
            if (cacheActionRunner == null) {
                cacheActionRunner = CacheActionRunner.class.cast(
                        Class.forName(cacheActionRunnerName).newInstance());
                LOG.debug("Populated a new CacheActionRunner with description " + cacheActionRunner.toString());
            } else {
                LOG.info("Runner is already defined.  Found " + cacheActionRunner.getClass().getName());
            }
        } catch(Exception e){
            LOG.error("Failed to get a valid cacheActionRunner.", e);
        }
        return cacheActionRunner;

    }

    @Override
    public String toString() {
        return "RocksDBExtractionNamespace{" +
                "rocksDbInstanceHDFSPath='" + rocksDbInstanceHDFSPath + '\'' +
                ", lookupAuditingHDFSPath='" + lookupAuditingHDFSPath + '\'' +
                ", namespace='" + namespace + '\'' +
                ", pollPeriod=" + pollPeriod +
                ", kafkaTopic='" + kafkaTopic + '\'' +
                ", cacheEnabled=" + cacheEnabled +
                ", lookupAuditingEnabled=" + lookupAuditingEnabled +
                ", lookupName='" + lookupName + '\'' +
                ", tsColumn='" + tsColumn + '\'' +
                ", missingLookupConfig=" + missingLookupConfig +
                ", lastUpdatedTime=" + lastUpdatedTime +
                ", cacheActionRunner=" + cacheActionRunner +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RocksDBExtractionNamespace that = (RocksDBExtractionNamespace) o;
        return cacheEnabled == that.cacheEnabled &&
                lookupAuditingEnabled == that.lookupAuditingEnabled &&
                Objects.equals(rocksDbInstanceHDFSPath, that.rocksDbInstanceHDFSPath) &&
                Objects.equals(lookupAuditingHDFSPath, that.lookupAuditingHDFSPath) &&
                Objects.equals(namespace, that.namespace) &&
                Objects.equals(pollPeriod, that.pollPeriod) &&
                Objects.equals(kafkaTopic, that.kafkaTopic) &&
                Objects.equals(lookupName, that.lookupName) &&
                Objects.equals(tsColumn, that.tsColumn) &&
                Objects.equals(missingLookupConfig, that.missingLookupConfig) &&
                Objects.equals(cacheActionRunner, that.cacheActionRunner);
    }

    @Override
    public int hashCode() {

        return Objects.hash(rocksDbInstanceHDFSPath,
                lookupAuditingHDFSPath,
                namespace,
                pollPeriod,
                kafkaTopic,
                cacheEnabled,
                lookupAuditingEnabled,
                lookupName,
                tsColumn,
                missingLookupConfig,
                cacheActionRunner);
    }
}
