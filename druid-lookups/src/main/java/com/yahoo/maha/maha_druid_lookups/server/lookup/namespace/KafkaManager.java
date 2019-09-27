// Copyright 2017, Yahoo Holdings Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.RocksDBExtractionNamespace;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.cache.MahaNamespaceExtractionCacheManager;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.ProtobufSchemaFactory;
import io.druid.guice.ManageLifecycle;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

@ManageLifecycle
public class KafkaManager {

    private static final Logger log = new Logger(KafkaManager.class);
    private final Properties kafkaProperties = new Properties();
    private static final int CONSUMER_COUNT = 5;
    private static final ConcurrentMap<String, Future> futureMap = new ConcurrentHashMap<>();
    private final ExecutorService customFutureReturningExecutorInitial = new CustomFutureReturningExecutor(new ThreadFactoryBuilder()
            .setNameFormat("kafka-consumer-initial-%d")
            .setDaemon(true)
            .setPriority(Thread.MIN_PRIORITY)
            .build());

    private final ExecutorService customFutureReturningExecutor = new CustomFutureReturningExecutor(new ThreadFactoryBuilder()
            .setNameFormat("kafka-consumer-%d")
            .setDaemon(true)
            .setPriority(Thread.MIN_PRIORITY)
            .build());

    private final Provider<MahaNamespaceExtractionCacheManager> namespaceExtractionCacheManager;

    private final ProtobufSchemaFactory protobufSchemaFactory;

    private KafkaProducer<String, byte[]> kafkaProducer;

    @Inject
    public KafkaManager(Provider<MahaNamespaceExtractionCacheManager> namespaceExtractionCacheManager,
                        final MahaNamespaceExtractionConfig mahaNamespaceExtractionConfig,
                        ProtobufSchemaFactory protobufSchemaFactory) {
        //this.kafkaProperties.putAll(mahaNamespaceExtractionConfig.getKafkaProperties());
        String bootstrapServers = mahaNamespaceExtractionConfig.getKafkaProperties().getProperty("bootstrap_servers", "");
        log.info("bootstrap.servers : [%s]", bootstrapServers);
        this.kafkaProperties.put("bootstrap.servers", bootstrapServers);
        this.namespaceExtractionCacheManager = namespaceExtractionCacheManager;
        this.protobufSchemaFactory = protobufSchemaFactory;
    }

    private void updateRocksDB(final Parser<Message> parser, final Descriptors.Descriptor descriptor,
                               final Descriptors.FieldDescriptor tsField, final RocksDB rocksDB, final String key,
                               final byte[] value) throws RocksDBException,
            InvalidProtocolBufferException {
        byte[] cacheValue = rocksDB.get(key.getBytes());
        if(cacheValue != null) {

            Message messageInDB = parser.parseFrom(cacheValue);
            Message newMessage = parser.parseFrom(value);
            Long lastUpdatedInDB, newLastUpdated;

            lastUpdatedInDB = Long.valueOf(messageInDB.getField(tsField).toString());
            newLastUpdated = Long.valueOf(newMessage.getField(tsField).toString());

            if(newLastUpdated > lastUpdatedInDB) {
                rocksDB.put(key.getBytes(), value);
            }
        } else {
            rocksDB.put(key.getBytes(), value);
        }
    }

    public void applyChangesSinceBeginning(final RocksDBExtractionNamespace extractionNamespace,
                                           final String groupId, final RocksDB rocksDB, final ConcurrentMap<Integer, Long> kafkaPartitionOffset) {

        final String topic = extractionNamespace.getKafkaTopic();
        final String namespace = extractionNamespace.getNamespace();
        final Properties properties = new Properties();
        properties.putAll(kafkaProperties);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "20000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("auto.offset.reset", "earliest");
        properties.put("max.poll.records", "10000");

        final Parser<Message> parser = protobufSchemaFactory.getProtobufParser(extractionNamespace.getNamespace());
        final Descriptors.Descriptor descriptor = protobufSchemaFactory.getProtobufDescriptor(extractionNamespace.getNamespace());
        final Descriptors.FieldDescriptor tsField = descriptor.findFieldByName(extractionNamespace.getTsColumn());

        final List<Future> futureList = new ArrayList<>(CONSUMER_COUNT);
        final CountDownLatch countDownLatch = new CountDownLatch(CONSUMER_COUNT);
        for (int count = 0; count < CONSUMER_COUNT; count++) {

            futureList.add(customFutureReturningExecutorInitial.submit(new Task() {
                @Override
                public Boolean call() {
                    final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
                    consumer.subscribe(Arrays.asList(topic));
                    log.info("Listening to topic [%s] with groupid [%s] for namespace [%s] to apply changes since the beginning", topic, groupId, namespace);
                    long recordCount = 0;
                    while (!cancelled && !Thread.currentThread().isInterrupted()) {
                        final ConsumerRecords<String, byte[]> records = consumer.poll(20000);
                        if (records.isEmpty()) {
                            log.info("Nothing else to consume for this consumer and topic [%s]", topic);
                            break;
                        }
                        for (ConsumerRecord<String, byte[]> record : records) {
                            if(cancelled) {
                                break;
                            }
                            final String key = record.key();
                            final byte[] message = record.value();
                            if (key == null || message == null) {
                                log.error("Bad key/message from topic [%s]", topic);
                                continue;
                            }
                            try {
                                updateRocksDB(parser, descriptor, tsField, rocksDB, key, message);
                                kafkaPartitionOffset.put(record.partition(), record.offset());
                            } catch (RocksDBException | InvalidProtocolBufferException e) {
                                log.error("Caught exception while applying changes to RocksDB", e);
                            }
                            log.debug("Placed key[%s] val[%s]", key, message);
                            recordCount++;
                            if ((recordCount % 1000000) == 0) {
                                log.info("Consumed [%s] records from [%s] topic since the beginning", recordCount, topic);
                            }
                        }
                    }
                    consumer.close();
                    log.info("Applied all the changes since the beginning for this consumer and topic [%s]", topic);
                    countDownLatch.countDown();
                    return true;
                }
            }));
        }
       try {
            countDownLatch.await(30, TimeUnit.MINUTES);
            futureList.forEach(future -> {
                if(!future.isDone()) {
                    future.cancel(true);
                }
            });
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            log.info("Done with waiting for all consumers [%s]", topic);
        }
        log.info("Applied all the changes since the beginning [%s]", topic);
    }

    public void addListener(final RocksDBExtractionNamespace kafkaNamespace, final String groupId,
                            final ConcurrentMap<Integer, Long> kafkaPartitionOffset, final boolean seekOffsetToPreviousSnapshot) {

        final String topic = kafkaNamespace.getKafkaTopic();
        final String namespace = kafkaNamespace.getNamespace();
        final Properties properties = new Properties();
        properties.putAll(kafkaProperties);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "5000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("max.poll.records", "10000");

        if(futureMap.containsKey(namespace)) {
            removeListener(namespace);
        }

        log.debug("Going to Listen to topic [%s] for namespace [%s]", topic, namespace);
        final Future future = customFutureReturningExecutor.submit(
                new Task() {
                    @Override
                    public Boolean call() {

                        final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
                        consumer.subscribe(Arrays.asList(topic));

                        if(seekOffsetToPreviousSnapshot) {
                            kafkaPartitionOffset.entrySet().forEach(partitionOffset -> {
                                log.info("topic = [%s], seek partition = [%s], seek offset = [%s]", topic, partitionOffset.getKey(), partitionOffset.getValue());
                                consumer.poll(0);
                                consumer.seek(new TopicPartition(topic, partitionOffset.getKey()), partitionOffset.getValue());
                            });
                        }

                        log.info("Listening to topic [%s] for namespace [%s]", topic, namespace);
                        long recordCount = 0;
                        while (!cancelled && !Thread.currentThread().isInterrupted()) {
                            final ConsumerRecords<String, byte[]> records = consumer.poll(1000);

                            for (ConsumerRecord<String, byte[]> record : records) {
                                if(cancelled) {
                                    break;
                                }
                                final String key = record.key();
                                final byte[] message = record.value();

                                if (key == null || message == null) {
                                    log.error("Bad key/message from topic [%s]", topic);
                                    continue;
                                }
                                ExtractionNamespaceCacheFactory namespaceFunctionFactory =
                                        namespaceExtractionCacheManager.get()
                                                .getExtractionNamespaceFunctionFactory(kafkaNamespace.getClass());
                                namespaceFunctionFactory.updateCache(kafkaNamespace,
                                        namespaceExtractionCacheManager.get().getCacheMap(namespace), key, message);
                                log.debug("Placed key[%s] val[%s]", key, message);
                                kafkaPartitionOffset.put(record.partition(), record.offset());
                                recordCount++;
                                if((recordCount % 100000) == 0) {
                                    log.info("Consumed [%s] records from [%s] topic", recordCount, topic);
                                }
                            }

                        }
                        consumer.close();
                        log.info("Running Task is cancelled for topic [%s]", topic);
                        return true;
                    }
                }
        );

        futureMap.put(namespace, future);
    }

    private void removeListener(final String namesapce) {
        log.info("Cancelling the running task for [%s]", namesapce);
        futureMap.get(namesapce).cancel(true);
    }

    @LifecycleStart
    public void start() {
        // NO-OP
    }

    @LifecycleStop
    public void stop() {
        customFutureReturningExecutorInitial.shutdown();
        customFutureReturningExecutor.shutdown();
        for(String namespace: futureMap.keySet()) {
            futureMap.get(namespace).cancel(true);
        }
    }

    private class CustomFutureReturningExecutor extends ThreadPoolExecutor {

        public CustomFutureReturningExecutor(ThreadFactory threadFactory) {
            super(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>(), threadFactory);
        }

        @Override
        protected RunnableFuture newTaskFor(Callable callable) {
            if (callable instanceof IdentifiableCallable) {
                return ((IdentifiableCallable) callable).newTask();
            } else {
                return super.newTaskFor(callable);
            }
        }
    }

    private interface IdentifiableCallable<T> extends Callable<T> {
        RunnableFuture newTask();
    }

    private abstract class FutureTaskWrapper<T> extends FutureTask<T> {
        public FutureTaskWrapper(Callable<T> callable) {
            super(callable);
        }
    }

    private class Task implements IdentifiableCallable<Boolean> {

        volatile boolean cancelled;

        @Override
        public RunnableFuture<Boolean> newTask() {
            return new FutureTaskWrapper<Boolean>(this) {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    log.info("Cancelling the task");
                    Task.this.cancelTask();
                    return super.cancel(mayInterruptIfRunning);
                }
            };
        }

        public synchronized void cancelTask() {
            cancelled = true;
        }

        @Override
        public Boolean call() {
            return true;
        }
    }

    public void handleMissingLookup(byte[] extractionNamespace, String topic, String dimension) {
        try {
            ProducerRecord<String, byte[]> producerRecord =
                    new ProducerRecord<>(topic, dimension, extractionNamespace);
            KafkaProducer<String, byte[]> kafkaProducer = ensureKafkaProducer();
            kafkaProducer.send(producerRecord);
        } catch (Exception e) {
            log.error(e, "caught exception while writing dimension: [%s] to missingLookupTopic: [%s]",  dimension, topic);
        }
    }

    private synchronized KafkaProducer<String, byte[]> ensureKafkaProducer() {
        if(kafkaProducer == null) {
            final Properties properties = new Properties();
            properties.putAll(kafkaProperties);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            properties.put(ProducerConfig.ACKS_CONFIG, kafkaProperties.getProperty("retries", "0"));
            properties.put(ProducerConfig.RETRIES_CONFIG, kafkaProperties.getProperty("acks", "0"));
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaProperties.getProperty("buffer.memory", "1048576"));
            properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaProperties.getProperty("max.block.ms", "1000"));
            Thread.currentThread().setContextClassLoader(null);
            this.kafkaProducer = new KafkaProducer<>(properties);
        }
        return kafkaProducer;
    }
}
