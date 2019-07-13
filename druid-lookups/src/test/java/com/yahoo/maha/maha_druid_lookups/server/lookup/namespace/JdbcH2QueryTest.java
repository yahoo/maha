package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.service.ServiceEmitter;
import com.yahoo.maha.jdbc.JdbcConnection;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespaceWithLeaderAndFollower;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doCallRealMethod;

/**
 * Created:         ryanwagner
 * Date:            2019/06/13
 *
 * Class Purpose:   Testing Druid lookup functionality to convert a lookup committed by all nodes
 *                  into a leader-follower set.
 *
 * Leader Role:     Reads data from Oracle & writes to a Kafka topic.
 * Follower Role:   Reads from Kafka topic & writes back to Druid.
 *
 * Progress:        setIsLeader ensures leader functionality, but consumer currently does nothing with data
 *                  parsed from its Topic except to return an updated TS (no cache writeback yet).
 */
public class JdbcH2QueryTest {

    private String h2dbId = UUID.randomUUID().toString().replace("-", "");
    private HikariDataSource ds;
    private JdbcConnection jdbcConnection;

    private JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower jdbcEncFactory = new JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower();
    private String jdbcUrl;
    private String userName;
    private String passWord;
    private String jdbcConnectorConfig;
    private Properties kafkaProperties;

    private Date currentMoment = new Date();
    private DateTime currentDateTime = new DateTime(currentMoment);
    private String toDatePlusOneHour = (new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")).format(currentDateTime.plusHours(1).toDate());

    //Mock classes, currently unused in this test suite.
    @Mock
    ServiceEmitter serviceEmitter;

    @Mock
    LookupService lookupService;

    private void setKafkaProperties() {
        kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put("group.id", "test-consumer-group");
    }

    private void initJdbcToH2() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(userName);
        config.setPassword(passWord);
        config.setMaximumPoolSize(2);
        ds = new HikariDataSource(config);
        Assert.assertTrue(ds.isRunning(), "Expect a running HikariDataSource.");
        jdbcConnection = new JdbcConnection(ds, 10);
        scala.util.Try testQuery = jdbcConnection.execute("SELECT * from DUAL;");
        Assert.assertTrue(testQuery.isSuccess(), "Expect JDBC connection to succeed with basic queries.");
    }

    private void buildJdbcTablesToQuery() {
        scala.util.Try createResult = jdbcConnection.execute(
                "CREATE TABLE ad (name VARCHAR2(255), id BIGINT, gpa DECIMAL, date TIMESTAMP, last_updated TIMESTAMP, title VARCHAR2(255), status VARCHAR2(255));");
        Assert.assertTrue(createResult.isSuccess(),"Should not fail to create a table in H2.");
    }

    private void insertIntoStudentTable() {
        scala.util.Try insertResult = jdbcConnection.execute("INSERT INTO ad values ('Bobbert', 1234, 3.1, ts '" + toDatePlusOneHour + "', ts '" + toDatePlusOneHour + "', 'Good Title', 'DELETED')");
        scala.util.Try insertResult2 = jdbcConnection.execute("INSERT INTO ad values ('Bobbert', 4444, 1.1, ts '" + toDatePlusOneHour + "', ts '" + toDatePlusOneHour + "', 'Gooder Title', 'ON')");
        Assert.assertTrue(insertResult.isSuccess(), "Should be able to insert data into the new table.");
        Assert.assertTrue(insertResult2.isSuccess(), "Should be able to insert data into the new table.");
    }

    @BeforeTest
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        jdbcEncFactory.emitter = serviceEmitter;
        jdbcEncFactory.lookupService = lookupService;
    }


    @BeforeClass
    public void init() {
        jdbcUrl = "jdbc:h2:mem:" + h2dbId + ";MODE=Oracle;DB_CLOSE_DELAY=-1";
        userName = "testUser";
        passWord = "h2.test.database.password";
        jdbcConnectorConfig = "{\"connectURI\":\"" + jdbcUrl + "\", \"user\":\"" + userName + "\", \"password\":\"" + passWord + "\"}";

        initJdbcToH2();
        buildJdbcTablesToQuery();
        insertIntoStudentTable();
        setKafkaProperties();
    }

    @AfterClass
    public void shutDown() {
        ds.close();
    }

    /**
     * Validate JDBC Connection works in test with a dummy table & values.
     */
    @Test
    public void testConnectJdbcToH2Lookup() {
        scala.util.Try queryResult = jdbcConnection.queryForObject("SELECT * from ad where NAME = 'Bobbert'", (
                rs -> {
                    try {
                        if(rs.isBeforeFirst())
                            rs.next();

                        String name = rs.getString("name");
                        long id = rs.getLong("id");
                        Float gpa = rs.getFloat("gpa");
                        Timestamp ts = rs.getTimestamp("last_updated");
                        Assert.assertEquals(name, "Bobbert", "");
                        Assert.assertEquals((Object)id, 1234L, "");
                        Assert.assertEquals(gpa, 3.1F, "");
                        return null;
                    } catch (SQLException e) {
                        return new scala.util.Failure(e);
                    }
                }));
        Assert.assertTrue(queryResult.isSuccess(), "Should be able to query the advertiser table." + queryResult.toString());
    }

    private MockConsumer<String, byte[]> createAndPopulateNewConsumer(
            List<ConsumerRecord<String, byte[]>> recordsToUse,
            String lookupName) {
        MockConsumer<String, byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        TopicPartition adPartition = new TopicPartition(lookupName, 0);
        List<TopicPartition> partitions = Collections.singletonList(adPartition);
        List<String> topics = Collections.singletonList(lookupName);
        Map<TopicPartition, Long> beginOffsets = new HashMap<TopicPartition, Long>(){
            {
                put(adPartition, 0L);
            }
        };
        Map<TopicPartition, Long> endOffsets = new HashMap<TopicPartition, Long>(){
            {
                put(adPartition, 1L);
            }
        };
        mockConsumer.subscribe(topics);
        mockConsumer.rebalance(partitions);
        mockConsumer.updateBeginningOffsets(beginOffsets);
        mockConsumer.updateEndOffsets(endOffsets);
        for(ConsumerRecord<String, byte[]> record: recordsToUse) {
            mockConsumer.addRecord(record);
        }

        return mockConsumer;
    }

    private MockProducer<String, byte[]> createAndPopulateNewProducer(

    ) {
        return new MockProducer<>(true, new StringSerializer(), new ByteArraySerializer());
    }

    private JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower createMockCacheFactory(
            MockConsumer<String, byte[]> mockConsumer,
            MockProducer<String, byte[]> mockProducer
    ) {
        JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower myJdbcEncFactory = mock(
                JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower.class);

        doCallRealMethod().when(myJdbcEncFactory).getCachePopulator(any(), any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).doFollowerOperations(any(), any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).doLeaderOperations(any(), any(), any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).populateRowListFromJDBC(any(), any(), any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).lastUpdates(any(), any());
        doCallRealMethod().when(myJdbcEncFactory).ensureDBI(any(), any());
        doCallRealMethod().when(myJdbcEncFactory).updateLocalCache(any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).getCacheValue(any(), any(), any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).populateLastUpdatedTime(any(), any());

        Whitebox.setInternalState(myJdbcEncFactory, "dbiCache", new ConcurrentHashMap<>());

        myJdbcEncFactory.emitter = serviceEmitter;
        myJdbcEncFactory.lookupService = lookupService;

        when(myJdbcEncFactory.ensureKafkaProducer(any())).thenReturn(mockProducer);
        when(myJdbcEncFactory.ensureKafkaConsumer(any())).thenReturn(mockConsumer);

        return myJdbcEncFactory;
    }

    public byte[] createInputRow() throws Exception{
        Map<String, Object> row = new HashMap<>();
        row.put("date", new Timestamp(new DateTime().getMillis()));
        row.put("last_updated", new Timestamp(new DateTime().getMillis()));
        row.put("name", "Bobbert");
        row.put("gpa", null);
        row.put("id", 1234L);
        row.put("title", "Good Ad");
        row.put("status", "ON");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(row);
        oos.close();
        return baos.toByteArray();
    }

    /**
     * Test getCachePopulator in JDBC EN Cache Factory using non-leader logic
     * Ensures follower logic can read and insert into cache.
     */
    @Test
    public void testCreateJdbcLookupOnH2() throws Exception {

        String lookupName = "ad_lookup";
        TopicPartition adPartition = new TopicPartition(lookupName, 0);
        ConsumerRecord<String, byte[]> adRecord = new ConsumerRecord<>(
                lookupName,
                0,
                179,
                new DateTime().getMillis(),
                TimestampType.CREATE_TIME,
                3065736663L,
                2,
                120,
                "ad",
                createInputRow());

        MockConsumer<String, byte[]> mockConsumer = createAndPopulateNewConsumer(Collections.singletonList(adRecord), "ad_lookup");
        MockProducer<String, byte[]> mockProducer = createAndPopulateNewProducer();


        JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower myJdbcEncFactory =
                createMockCacheFactory(mockConsumer, mockProducer);

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new ObjectMapper()
        .readerFor(MetadataStorageConnectorConfig.class)
        .readValue(jdbcConnectorConfig);

        JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace =
                new JDBCExtractionNamespaceWithLeaderAndFollower(
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date", "last_updated")),
                        "id", "last_updated", new Period(), true,
                        "ad_lookup", "ad_test", false, kafkaProperties);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "3.1", toDatePlusOneHour));
        Callable<String> populator = myJdbcEncFactory.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, "0", map);
        System.err.println("Callable Result Timestamp (long): " + populator.call());

        //Populator has been called, assertions here.
        Assert.assertTrue(mockConsumer.position(adPartition) >= 110L, "Expected >= 120 offset (1 message) but got " + mockConsumer.position(adPartition));
        String cachedName = new String(myJdbcEncFactory.getCacheValue(extractionNamespace, map, "1234", "name", Optional.empty()));
        Assert.assertEquals(cachedName, "Bobbert");
        String cachedGpa = new String(myJdbcEncFactory.getCacheValue(extractionNamespace, map, "1234", "gpa", Optional.empty()));
        Assert.assertEquals(cachedGpa, "null");
    }

    /**
     * Test getCachePopulator in JDBC EN Cache Factory using Leader logic
     * Should create and write to a kafka topic.
     */
    @Test
    public void testCreateJdbcLookupOnH2asLeader() throws Exception {

        String lookupName = "ad_lookup";
        ConsumerRecord<String, byte[]> adRecord = new ConsumerRecord<>(
                lookupName,
                0,
                179,
                new DateTime().getMillis(),
                TimestampType.CREATE_TIME,
                3065736663L,
                2,
                120,
                "ad",
                createInputRow());

        MockConsumer<String, byte[]> mockConsumer = createAndPopulateNewConsumer(Collections.singletonList(adRecord), "ad_lookup");
        MockProducer<String, byte[]> mockProducer = createAndPopulateNewProducer();

        JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower myJdbcEncFactory =
                createMockCacheFactory(mockConsumer, mockProducer);

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new ObjectMapper()
                .readerFor(MetadataStorageConnectorConfig.class)
                .readValue(jdbcConnectorConfig);

        JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace =
                new JDBCExtractionNamespaceWithLeaderAndFollower(
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date", "last_updated", "title", "status")),
                        "id", "last_updated", new Period(), false,
                        "ad_lookup", "ad_test", true, kafkaProperties);

        extractionNamespace.setFirstTimeCaching(false);
        extractionNamespace.setPreviousLastUpdateTimestamp(new Timestamp(currentDateTime.getMillis()));

        Map<String, List<String>> map = new HashMap<>();
        Callable<String> populator = myJdbcEncFactory.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, "0", map);
        System.err.println("Callable Result Timestamp (long): " + populator.call());

        //Populator has been called, assertions here.
        Assert.assertEquals(mockProducer.history().size(), 2, "Expect to see 2 producerRecords sent.");
        for(Object record: mockProducer.history()) {
            Assert.assertEquals(ProducerRecord.class, record.getClass());
            ProducerRecord rc = (ProducerRecord) record;
            Assert.assertSame("ad", rc.key());
            Assert.assertSame("ad_test", rc.topic());
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream((byte[])rc.value()));
            Map<String, Object> allColumnsMap = (Map<String, Object>)ois.readObject();
            String recordedValueAsString = allColumnsMap.toString();
            Assert.assertTrue(recordedValueAsString.contains("1234") || recordedValueAsString.contains(("4444")));
        }
    }
}
