// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.missing.lookup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.InMemoryDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.PasswordProvider;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import io.druid.java.util.common.logger.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.DefaultMapper;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class MissingLookupInMemoryDBExtractionNamespaceFactory implements
        MissingLookupExtractionNamespaceFactory {

    private static final Logger LOGGER = new Logger(MissingLookupInMemoryDBExtractionNamespaceFactory.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private DBI dbi = null;
    private KafkaProducer<String, byte[]> kafkaProducer = null;

    public void process(String dimension,
                        byte[] extractionNamespaceByteArray,
                        ProtobufSchemaFactory protobufSchemaFactory,
                        PasswordProvider passwordProvider,
                        Properties kafkaProperties,
                        String producerKafkaTopic) throws IOException {

        InMemoryDBExtractionNamespace extractionNamespace = OBJECT_MAPPER.readValue(extractionNamespaceByteArray, InMemoryDBExtractionNamespace.class);
        dbi = ensureDBI(passwordProvider, extractionNamespace);
        kafkaProducer = ensureKafkaProducer(kafkaProperties);
        dbi.withHandle( handle -> {

            Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getLookupName());
            Message.Builder messageBuilder = protobufSchemaFactory.getProtobufMessageBuilder(extractionNamespace.getLookupName());

            String query = String.format("SELECT * FROM %s WHERE %s = :%s",
                    extractionNamespace.getMissingLookupConfig().getTable(),
                    extractionNamespace.getMissingLookupConfig().getPrimaryKeyColumn(),
                    extractionNamespace.getMissingLookupConfig().getPrimaryKeyColumn()
            );

            Map<String, Object> map = handle.createQuery(query)
                    .bind(extractionNamespace.getMissingLookupConfig().getPrimaryKeyColumn(), dimension)
                    .map(new DefaultMapper())
                    .first();

            descriptor.getFields()
                    .stream()
                    .forEach(fd -> messageBuilder.setField(fd, String.valueOf(map.get(fd.getName()))));

            Message message = messageBuilder.build();
            LOGGER.info("Producing key[%s] val[%s]", dimension, message);
            ProducerRecord<String, byte[]> producerRecord =
                    new ProducerRecord<>(producerKafkaTopic, dimension, message.toByteArray());
            kafkaProducer.send(producerRecord);
            return null;
        });
    }

    synchronized DBI ensureDBI(PasswordProvider passwordProvider, InMemoryDBExtractionNamespace namespace) {
        if (dbi == null) {
            dbi = new DBI(
                    namespace.getMissingLookupConfig().getConnectorConfig().getConnectURI(),
                    namespace.getMissingLookupConfig().getConnectorConfig().getUser(),
                    passwordProvider.getPassword(namespace.getMissingLookupConfig().getConnectorConfig().getPassword())
            );
        }
        return dbi;
    }

    synchronized KafkaProducer<String, byte[]> ensureKafkaProducer(Properties kafkaProperties) {
        if(kafkaProducer == null) {
            kafkaProducer = new KafkaProducer<>(kafkaProperties);
        }
        return kafkaProducer;
    }

    public void stop() {
        if(kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }
}
