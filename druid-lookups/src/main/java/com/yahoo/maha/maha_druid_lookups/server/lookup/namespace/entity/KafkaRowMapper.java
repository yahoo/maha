// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity;

import com.metamx.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespaceWithLeaderAndFollower;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.skife.jdbi.v2.DefaultMapper;
import org.skife.jdbi.v2.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class KafkaRowMapper extends RowMapper {

    private static final Logger LOG = new Logger(KafkaRowMapper.class);
    private JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace;
    private Map<String, List<String>> cache;
    private Producer<String, byte[]> kafkaProducer;
    private String producerKafkaTopic;

    public KafkaRowMapper(JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace, Map<String, List<String>> cache ) {
        super(extractionNamespace, cache);
        this.extractionNamespace = extractionNamespace;
        this.cache = cache;
        this.producerKafkaTopic = extractionNamespace.getKafkaTopic();
    }

    @Override
    public Void map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
        List<String> strings = new ArrayList<>(extractionNamespace.getColumnList().size());
        for(String columnName: extractionNamespace.getColumnList()) {
            strings.add(resultSet.getString(columnName));
        }

        String rowPkCol = resultSet.getString(extractionNamespace.getPrimaryKeyColumn());

        if(Objects.nonNull(cache))
            cache.put(rowPkCol, strings);

        Map<String, Object> row = new DefaultMapper().map(i, resultSet, statementContext);
        if(Objects.nonNull(row)) {

                kafkaProducer = ensureKafkaProducer(extractionNamespace.getKafkaProperties());
                ProducerRecord<String, byte[]> producerRecord =
                        new ProducerRecord<>(producerKafkaTopic, extractionNamespace.getTable(), row.toString().getBytes());
                kafkaProducer.send(producerRecord);
        } else {
            LOG.info("No query results to return.");
        }

        return null;
    }



    /**
     * Safe KafkaProducer create/call.
     * @param kafkaProperties
     * @return
     */
    private synchronized Producer<String, byte[]> ensureKafkaProducer(Properties kafkaProperties) {

        if(kafkaProducer == null) {
            kafkaProducer = new KafkaProducer<>(kafkaProperties, new StringSerializer(), new ByteArraySerializer());
        }
        return kafkaProducer;
    }
}
