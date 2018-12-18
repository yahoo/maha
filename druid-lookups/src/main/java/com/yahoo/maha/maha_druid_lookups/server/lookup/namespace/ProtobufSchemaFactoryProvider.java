// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import org.apache.commons.lang.StringUtils;

public class ProtobufSchemaFactoryProvider implements Provider<ProtobufSchemaFactory> {

  private final ProtobufSchemaFactory schemaFactory;

  @Inject
  public ProtobufSchemaFactoryProvider(MahaNamespaceExtractionConfig config) {
    String schemaFactoryClass = config.getProtobufSchemaFactoryClass();
    Preconditions.checkArgument(StringUtils.isNotBlank(schemaFactoryClass), "schemaFactory cannot be blank!");
    Class clazz;

    try {
      clazz = Class.forName(schemaFactoryClass);
      if(ProtobufSchemaFactory.class.isAssignableFrom(clazz)) {
        schemaFactory = (ProtobufSchemaFactory) clazz.newInstance();
      } else {
        throw new IllegalArgumentException(String.format("%s must implement type ProtobufSchemaFactory!", schemaFactoryClass));
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }

  }

  @Override
  public ProtobufSchemaFactory get() {
    return schemaFactory;
  }
}
