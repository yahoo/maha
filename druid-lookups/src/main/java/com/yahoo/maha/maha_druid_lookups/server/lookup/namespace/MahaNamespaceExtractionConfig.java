// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Properties;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class MahaNamespaceExtractionConfig {
  @Nullable @JsonProperty(value = "kafka")
  private Properties kafkaProperties;

  @Nullable @JsonProperty(value = "service")
  private Properties lookupServiceProperties;

  @Nullable @JsonProperty(value = "rocksdb")
  private Properties rocksDBProperties;

  @NotNull @JsonProperty(value = "schemaFactory")
  private String protobufSchemaFactoryClass;

  @NotNull @JsonProperty(value = "authHeaderFactory")
  private String authHeaderFactoryClass;

  public Properties getKafkaProperties() {
    return kafkaProperties;
  }

  public Properties getLookupServiceProperties() {
    return lookupServiceProperties;
  }

  public Properties getRocksDBProperties() {
    return rocksDBProperties;
  }

  public String getProtobufSchemaFactoryClass() {
    return protobufSchemaFactoryClass;
  }

  public String getAuthHeaderFactoryClass() {
    return authHeaderFactoryClass;
  }
}
