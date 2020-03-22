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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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

import java.io.*;
import java.net.ServerSocket;
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

    private String jdbcUrl;
    private String userName;
    private String passWord;
    private String jdbcConnectorConfig;
    private Properties kafkaProperties;
    private ServerSocket freeSocket;

    private Date currentMoment = new Date();
    private DateTime currentDateTime = new DateTime(currentMoment);
    private DateTime currentDateTimeMinusOneHour = new DateTime(currentMoment).minusHours(1);
    private String toDatePlusOneHour = (new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")).format(currentDateTime.plusHours(1).toDate());
    private String toDateMinusOneHour = (new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")).format(currentDateTime.minusHours(1).toDate());

    //Mock classes, currently unused in this test suite.
    @Mock
    ServiceEmitter serviceEmitter;

    @Mock
    LookupService lookupService;

    /**
     * Set up Kafka for testing using suggested parameters for Producer & Consumer,
     * though actual behavior will be mocked.
     * @throws IOException
     */
    private void setKafkaProperties() throws IOException {
        kafkaProperties = new Properties();
        freeSocket = new ServerSocket(0);
        int freePort = freeSocket.getLocalPort();
        kafkaProperties.put("bootstrap.servers", "localhost:" + freePort);
        kafkaProperties.put("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        kafkaProperties.put("key.serializer", StringSerializer.class.getName());
        kafkaProperties.put("value.serializer", ByteArraySerializer.class.getName());
        kafkaProperties.put("group.id", "test-consumer-group");
    }

    /**
     * Open a JDBC Connection to local H2 DB.
     */
    private void initJdbcToH2() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(userName);
        config.setPassword(passWord);
        config.setMaximumPoolSize(2);
        ds = new HikariDataSource(config);
        Assert.assertTrue(ds.isRunning(), "Expect a running HikariDataSource.");
        jdbcConnection = new JdbcConnection(ds, 10);
        scala.util.Try testQuery = jdbcConnection.execute("SELECT 1 from DUAL;");
        Assert.assertTrue(testQuery.isSuccess(), "Expect JDBC connection to succeed with basic queries.");
    }

    /**
     * Create queries to be passed in the lookup configs.
     */
    private void buildJdbcTablesToQuery() {
        scala.util.Try createResult = jdbcConnection.execute(
                "CREATE TABLE ad (name VARCHAR2(255), id BIGINT, gpa DECIMAL, date TIMESTAMP, last_updated TIMESTAMP, title VARCHAR2(255), status VARCHAR2(255));");
        Assert.assertTrue(createResult.isSuccess(),"Should not fail to create a table in H2.");
    }

    /**
     * Insert a few old & new rows into the Ad table to test against.
     */
    private void insertIntoAdTable() {
        scala.util.Try insertResult = jdbcConnection.execute("INSERT INTO ad values ('Bobbert', 1234, 3.1, ts '" + toDatePlusOneHour + "', ts '" + toDatePlusOneHour + "', 'Good Title', 'DELETED')");
        scala.util.Try insertResult2 = jdbcConnection.execute("INSERT INTO ad values ('Bobbert', 4444, 1.1, ts '" + toDatePlusOneHour + "', ts '" + toDatePlusOneHour + "', 'Gooder Title', 'ON')");
        scala.util.Try insertResult3 = jdbcConnection.execute("INSERT INTO ad values ('Before_Bob', 4444, 1.1, ts '" + toDateMinusOneHour + "', ts '" + toDateMinusOneHour + "', 'Gooder Title', 'ON')");
        Assert.assertTrue(insertResult.isSuccess(), "Should be able to insert data into the new table.");
        Assert.assertTrue(insertResult2.isSuccess(), "Should be able to insert data into the new table.");
        Assert.assertTrue(insertResult3.isSuccess(), "Should be able to insert data into the new table.");
    }

    @BeforeTest
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }


    @BeforeClass
    public void init() throws IOException {
        jdbcUrl = "jdbc:h2:mem:" + h2dbId + ";MODE=Oracle;DB_CLOSE_DELAY=-1";
        userName = "testUser";
        passWord = "h2.test.database.password";
        jdbcConnectorConfig = "{\"connectURI\":\"" + jdbcUrl + "\", \"user\":\"" + userName + "\", \"password\":\"" + passWord + "\"}";

        initJdbcToH2();
        buildJdbcTablesToQuery();
        insertIntoAdTable();
        setKafkaProperties();
    }

    @AfterClass
    public void shutDown() throws IOException {
        freeSocket.close();
        ds.close();
        serviceEmitter = null;
        lookupService = null;

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

    /**
     * Create a false consumer with the ad_lookup partition name to Mock topic reading.
     * The consumerRecords are created mainly using the createInputRow logic.
     * @param recordsToUse
     * @param lookupName
     * @return
     */
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

    /**
     * Create a mocked factory with Producer & Consumer, in order to Mock any behavior that would
     * otherwise require actual Kafka behavior.
     * This will test SerDe & other reading/writing logic.
     * @param mockConsumer
     * @param mockProducer
     * @return
     */
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
        doCallRealMethod().when(myJdbcEncFactory).lastUpdates(any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).ensureDBI(any(), any());
        doCallRealMethod().when(myJdbcEncFactory).updateLocalCache(any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).getCacheValue(any(), any(), any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).populateLastUpdatedTime(any(), any());
        doCallRealMethod().when(myJdbcEncFactory).getBaseWhereClause(any(), any());
        doCallRealMethod().when(myJdbcEncFactory).populateRowListFromJDBC(any(), any(), any(), any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).getSecondaryTsWhereClause(any(), any(), any());
        doCallRealMethod().when(myJdbcEncFactory).getMaxValFromColumn(any(), any(), any(), any(), any());

        Whitebox.setInternalState(myJdbcEncFactory, "dbiCache", new ConcurrentHashMap<>());

        myJdbcEncFactory.emitter = serviceEmitter;
        myJdbcEncFactory.lookupService = lookupService;

        ConcurrentHashMap<String, Consumer<String, byte[]>> mockConsumerWrapper = new ConcurrentHashMap<>();
        mockConsumerWrapper.put("ad_test", mockConsumer);

        myJdbcEncFactory.lookupConsumerMap = mockConsumerWrapper;
        when(myJdbcEncFactory.ensureKafkaProducer(any())).thenReturn(mockProducer);

        return myJdbcEncFactory;
    }

    /**
     * Create a row, testing a null input & future Timestamp.
     * @return
     * @throws Exception
     */
    private byte[] createInputRow() throws Exception{
        Map<String, Object> row = new HashMap<>();
        row.put("date", new Timestamp(new DateTime().getMillis()));
        row.put("last_updated", new Timestamp(new DateTime().plusHours(1).getMillis()));
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
     * Create a row with an old timestamp to parse.
     * @return
     * @throws Exception
     */
    private byte[] createInputRowOldTime() throws Exception{
        Map<String, Object> row = new HashMap<>();
        row.put("date", new Timestamp(new DateTime().getMillis()));
        row.put("last_updated", new Timestamp(currentDateTimeMinusOneHour.getMillis()));
        row.put("name", "Old Bobbert");
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
     * This assumes it isn't the first time caching.
     */
    @Test
    public void testCreateJdbcLookupOnH2AsFollower() throws Exception {

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
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date")),
                        "id", "last_updated", new Period(3000L), true,
                        "ad_lookup", "ad_test", false, kafkaProperties);
        extractionNamespace.setFirstTimeCaching(false);
        extractionNamespace.setPreviousLastUpdateTimestamp(new Timestamp(currentDateTimeMinusOneHour.getMillis()));
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
     * Test getCachePopulator in JDBC EN Cache Factory using non-leader logic
     * Ensures follower logic can read and insert into cache.
     * This assumes it isn't the first time caching.
     * Use a record older than current last updated date,
     * testing out-of-order Kafka processing
     */
    @Test
    public void testCreateJdbcLookupOnH2AsFollowerBadRecord() throws Exception {

        String lookupName = "ad_lookup";
        TopicPartition adPartition = new TopicPartition(lookupName, 0);
        ConsumerRecord<String, byte[]> adRecord = new ConsumerRecord<>(
                lookupName,
                0,
                179,
                currentDateTimeMinusOneHour.getMillis(),
                TimestampType.CREATE_TIME,
                3065736663L,
                2,
                120,
                "ad",
                createInputRowOldTime());

        MockConsumer<String, byte[]> mockConsumer = createAndPopulateNewConsumer(Collections.singletonList(adRecord), "ad_lookup");
        MockProducer<String, byte[]> mockProducer = createAndPopulateNewProducer();


        JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower myJdbcEncFactory =
                createMockCacheFactory(mockConsumer, mockProducer);

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new ObjectMapper()
                .readerFor(MetadataStorageConnectorConfig.class)
                .readValue(jdbcConnectorConfig);

        JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace =
                new JDBCExtractionNamespaceWithLeaderAndFollower(
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date")),
                        "id", "last_updated", new Period(3000L), true,
                        "ad_lookup", "ad_test", false, kafkaProperties);
        extractionNamespace.setFirstTimeCaching(false);
        extractionNamespace.setPreviousLastUpdateTimestamp(new Timestamp(currentDateTime.getMillis()));
        Map<String, List<String>> map = new HashMap<>();


        map.put("1234", Arrays.asList("1234", "my name", "3.1", toDatePlusOneHour, toDatePlusOneHour));
        Callable<String> populator = myJdbcEncFactory.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, "0", map);
        System.err.println("Callable Result Timestamp (long): " + populator.call());

        //Populator has been called, assertions here.
        Assert.assertTrue(mockConsumer.position(adPartition) >= 110L, "Expected >= 120 offset (1 message) but got " + mockConsumer.position(adPartition));
        String cachedName = new String(myJdbcEncFactory.getCacheValue(extractionNamespace, map, "1234", "name", Optional.empty()));
        Assert.assertEquals(cachedName, "my name");
        String cachedGpa = new String(myJdbcEncFactory.getCacheValue(extractionNamespace, map, "1234", "gpa", Optional.empty()));
        Assert.assertEquals(cachedGpa, "3.1");
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
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date", "title", "status")),
                        "id", "last_updated", new Period(3000L), true,
                        "ad_lookup", "ad_test", true, kafkaProperties);

        extractionNamespace.setFirstTimeCaching(false);
        extractionNamespace.setPreviousLastUpdateTimestamp(new Timestamp(currentDateTime.getMillis()));

        //Should see:
        //Leader finished loading 2 values giving final cache size of [4] for extractionNamespace [ad_lookup]
        //on debug
        Map<String, List<String>> map = new HashMap<>();
        map.put("232323", Arrays.asList("232323", "another name", "3.1", toDatePlusOneHour, toDatePlusOneHour));
        map.put("2323234", Arrays.asList("2323234", "another name", "3.1", toDatePlusOneHour, toDatePlusOneHour));
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

    /**
     * Test getCachePopulator in JDBC EN Cache Factory using Leader logic
     * Should create and write to a kafka topic.
     */
    @Test
    public void testCreateJdbcLookupOnH2asLeaderFirstTime() throws Exception {

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
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date", "title", "status")),
                        "id", "last_updated", new Period(3000L), true,
                        "ad_lookup", "ad_test", true, kafkaProperties);

        extractionNamespace.setFirstTimeCaching(true);
        extractionNamespace.setPreviousLastUpdateTimestamp(new Timestamp(currentDateTime.getMillis()));

        Map<String, List<String>> map = new HashMap<>();
        Callable<String> populator = myJdbcEncFactory.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, "0", map);
        System.err.println("Callable Result Timestamp (long): " + populator.call());

        //Populator has been called, assertions here.
        Assert.assertEquals(mockProducer.history().size(), 0, "Expect to see 0 producerRecords sent, since KafkaProducer should usr JDBC on bootstrap.");
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
