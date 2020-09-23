// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.BaseCacheActionRunner;
import org.apache.druid.java.util.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.CacheActionRunner;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
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
    @JsonProperty
    private boolean randomLocalPathSuffixEnabled = false;

    private Long lastUpdatedTime = -1L;

    @JsonProperty
    public String cacheActionRunnerName = "";

    private BaseCacheActionRunner cacheActionRunner = null;

    @JsonProperty
    private String overrideLookupServiceHosts = null;
    private List<String> overrideLookupServiceHostsList = null;


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
                                      @JsonProperty(value = "cacheActionRunner", required = false) final String cacheActionRunnerName,
                                      @JsonProperty(value = "overrideLookupServiceHosts", required = false) final String overrideLookupServiceHosts,
                                      @JsonProperty(value = "randomLocalPathSuffixEnabled", required = false) final boolean randomLocalPathSuffixEnabled) {
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
        this.randomLocalPathSuffixEnabled = randomLocalPathSuffixEnabled;

        //cacheActionRunner = "."
        try {
            if(StringUtils.isBlank(cacheActionRunnerName)) //no runner is passed, use Default Class String
                this.cacheActionRunnerName = CacheActionRunner.class.getName();
            else {
                //Check if the passed in runner is valid, else throw an exception to stop the program.
                Object actionRunner = Class.forName(cacheActionRunnerName).newInstance();
                Preconditions.checkArgument(actionRunner instanceof BaseCacheActionRunner,
                        "Passed in runner should be a CacheActionRunner, but got a " + cacheActionRunnerName + " of class " + actionRunner.getClass().getName());
                this.cacheActionRunnerName =  cacheActionRunnerName;
            }
            this.cacheActionRunner = BaseCacheActionRunner.class.cast(
                Class.forName(this.cacheActionRunnerName).newInstance());
        } catch (Throwable t) {
            LOG.error(t, "Found a blank or invalid CacheActionRunner, logging error and throwing Runtime ");
            throw new RuntimeException("Found invalid passed in CacheActionRunner with String " + cacheActionRunner, t);
        }

        //overrideLookupServiceHosts
        if(StringUtils.isBlank(overrideLookupServiceHosts)) {
            LOG.info("no input from overrideLookupServiceHosts");
            this.overrideLookupServiceHostsList = Collections.emptyList();
            this.overrideLookupServiceHosts = this.overrideLookupServiceHostsList.toString();
        } else {
            LOG.info("input overrideLookupServiceHosts: " + overrideLookupServiceHosts);
            this.overrideLookupServiceHostsList = parseOverrideLookupServiceHostsList(overrideLookupServiceHosts);
            this.overrideLookupServiceHosts = overrideLookupServiceHostsList.toString().replaceAll("\\s+","");
            LOG.info("valid overrideLookupServiceHosts: " + this.overrideLookupServiceHosts);
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

    @Override
    public ExtractionNameSpaceSchemaType getSchemaType() {
        return this.cacheActionRunner.getSchemaType();
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

    public boolean isRandomLocalPathSuffixEnabled() {
        return randomLocalPathSuffixEnabled;
    }

    public String getCacheActionRunnerName() { return cacheActionRunnerName; }

    public BaseCacheActionRunner getCacheActionRunner() {
        return cacheActionRunner;
    }

    public String getOverrideLookupServiceHosts() {
        return overrideLookupServiceHosts;
    }

    @Override
    public List<String> getOverrideLookupServiceHostsList() {
        return overrideLookupServiceHostsList;
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
                ", overrideLookupServiceHosts=" + overrideLookupServiceHosts +
                ", randomLocalPathSuffixEnabled=" + randomLocalPathSuffixEnabled +
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
                Objects.equals(cacheActionRunnerName, that.cacheActionRunnerName) &&
                Objects.equals(overrideLookupServiceHosts, that.overrideLookupServiceHosts) &&
                Objects.equals(randomLocalPathSuffixEnabled, that.randomLocalPathSuffixEnabled);
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
                cacheActionRunnerName,
                overrideLookupServiceHosts,
                randomLocalPathSuffixEnabled);
    }
}
