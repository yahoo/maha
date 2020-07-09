// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.missing.lookup;

import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.PasswordProvider;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.schema.protobuf.ProtobufSchemaFactory;

import java.io.IOException;
import java.util.Properties;

public interface MissingLookupExtractionNamespaceFactory {

    void process(String dimension,
                 byte[] extractionNamespaceByteArray,
                 ProtobufSchemaFactory protobufSchemaFactory,
                 PasswordProvider passwordProvider,
                 Properties kafkaProperties,
                 String producerKafkaTopic) throws IOException;

    void stop();

}
