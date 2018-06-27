package com.yahoo.maha.maha_druid_lookups.missing.lookup;

import com.metamx.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.PasswordProvider;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MissingLookupManager<T extends MissingLookupExtractionNamespaceFactory,
        U extends ProtobufSchemaFactory, V extends PasswordProvider> {

    private static final Logger LOGGER = new Logger(MissingLookupManager.class);
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    public void start(T missingLookupExtractionNamespaceFactory,
                      U protobufSchemaFactory,
                      V passwordProvider,
                      Properties kafkaProperties,
                      String consumerKafkaTopic,
                      int consumerCount,
                      String producerKafkaTopic) {

        for(int i=0; i<consumerCount; i++) {
            executorService.submit(() -> {
                try {
                    final KafkaConsumer<String, byte[]> consumer = createKafkaConsumer(kafkaProperties);
                    consumer.subscribe(Arrays.asList(consumerKafkaTopic));

                    LOGGER.info("Listening to topic [%s]", consumerKafkaTopic);
                    long recordCount = 0;
                    while (!Thread.currentThread().isInterrupted()) {
                        final ConsumerRecords<String, byte[]> records = consumer.poll(5000);
                        for (ConsumerRecord<String, byte[]> record : records) {
                            final String dimension = record.key();

                            final byte[] extractionNamespaceByteArray = record.value();
                            if (dimension == null || extractionNamespaceByteArray == null) {
                                LOGGER.error("Bad key/message from topic [%s]", consumerKafkaTopic);
                                continue;
                            }
                            LOGGER.debug("Read key[%s] val[%s]", dimension, new String(extractionNamespaceByteArray));
                            try {
                                missingLookupExtractionNamespaceFactory.process(dimension,
                                        extractionNamespaceByteArray,
                                        protobufSchemaFactory,
                                        passwordProvider,
                                        kafkaProperties,
                                        producerKafkaTopic);
                            } catch (Exception e) {
                                LOGGER.error(e, "Caught exception while reading from kafka");
                            }
                            recordCount++;
                            if ((recordCount % 100) == 0) {
                                LOGGER.info("Consumed [%s] records from [%s] topic", recordCount, consumerKafkaTopic);
                            }
                        }
                    }
                    consumer.close();
                } catch (Exception e) {
                    LOGGER.error(e, e.getMessage());
                }
            });
        }
    }

    KafkaConsumer<String, byte[]> createKafkaConsumer(Properties kafkaProperties) {
        return new KafkaConsumer<>(kafkaProperties);
    }

    public void stop(T missingLookupExtractionNamespaceFactory) {
        missingLookupExtractionNamespaceFactory.stop();
        executorService.shutdown();
    }

}
