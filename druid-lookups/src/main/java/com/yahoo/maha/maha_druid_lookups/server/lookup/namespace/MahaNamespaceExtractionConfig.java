// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import java.util.Properties;
import org.codehaus.jackson.annotate.JsonProperty;

public class MahaNamespaceExtractionConfig {
  @JsonProperty("kafka")
  private Properties kafkaProperties;

  @JsonProperty("service")
  private Properties lookupServiceProperties;

  @JsonProperty("rocksdb")
  private Properties rocksDBProperties;

  @JsonProperty("schemaFactory")
  private String protobufSchemaFactoryClass;

  @JsonProperty("authHeaderFactory")
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
