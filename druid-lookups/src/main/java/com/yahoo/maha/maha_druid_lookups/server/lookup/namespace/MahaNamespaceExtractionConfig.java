// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.internal.cglib.proxy.$FixedValue;

import java.util.Properties;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class MahaNamespaceExtractionConfig {
  @Nullable @JsonProperty(value = "kafka")
  private Properties kafkaProperties = new Properties();

  @NotNull @JsonProperty(value = "lookupService")
  private Properties lookupServiceProperties;

  @NotNull @JsonProperty(value = "rocksdb")
  private Properties rocksDBProperties;

  @NotNull @JsonProperty(value = "schemaFactory")
  private String protobufSchemaFactoryClass;

  @NotNull @JsonProperty(value = "authHeaderFactory")
  private String authHeaderFactoryClass;

  @Nullable @JsonProperty(value="flatBufferSchemaFactory")
  private String flatBufferSchemaFactory;

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

  public String getFlatBufferSchemaFactory() { return flatBufferSchemaFactory; }

}
