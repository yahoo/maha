package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.service.ServiceEmitter;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespaceWithLeaderAndFollower;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.TestProtobufSchemaFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Ignore;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created:         ryanwagner
 * Date:            2019/06/13
 *
 * Class Purpose:   Testing Druid lookup functionality to convert a lookup committed by all nodes
 *                  into a leader-follower set.
 *
 * Leader Role:     Reads data from Oracle & writes to a Kafka topic.
 * Follower Role:   Reads from Kafka topic & writes back to local Cache.
 *
 * Purpose:         These tests, marked as @Ignored, are to be implemented locally to demonstrate full lookup
 *                  functionality.
 *
 * Requirements:    Local instance of mySQL DB, running on the Port noted in the connection String.
 *                  Local instance of Kafka running on the same port as bootstrap.servers.
 *                  TODO: Local instance of Druid to load these instances into.
 *
 * TODOS:           TODO: Above
 *                  TODO: Ensure KafkaConsumer runs properly by creating a 1-minute test that periodically
 *                        writes to an active Kafka topic.
 */
public class JdbcSqlLocalQueryTest {

    private HikariConfig config;
    private String h2dbId = UUID.randomUUID().toString().replace("-", "");
    private HikariDataSource ds;
    private Statement jdbcConnection;

    private JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower jdbcEncFactory = new JDBCExtractionNamespaceCacheFactoryWithLeaderAndFollower();
    private String jdbcUrl;
    private String userName;
    private String passWord;
    private String jdbcConnectorConfig;
    private Properties kafkaProperties;

    Date currentMoment = new Date();
    DateTime currentDateTime = new DateTime(currentMoment);
    String toDatePlusOneHour = (new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")).format(currentDateTime.plusHours(1).toDate());

    //Mock classes, currently unused in this test suite.
    @Mock
    ServiceEmitter serviceEmitter;

    @Mock
    LookupService lookupService;

    /**
     * Set up local Kafka Producer and Comsumer, which will share properties.
     */
    void setKafkaProperties() {
        kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
    }

    /**
     * Create a JDBC Connection to your local mySQL database.
     * @throws Exception
     */
    void initJdbcToMySql() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/test?"
                        + "user=root&password=password&useSSL=false&serverTimezone=UTC");
        jdbcConnection = connection.createStatement();
        ResultSet testQuery = jdbcConnection.executeQuery("SELECT * from AD;");
        testQuery.next();
        String p = testQuery.getString(1);
        Assert.assertTrue(testQuery.isFirst());
    }

    /**
     * Create tables in your mySQL instance.
     */
    void buildJdbcTablesToQuery() {
        //scala.util.Try createResult = jdbcConnection.execute("CREATE TABLE ad (name VARCHAR2(255), id BIGINT, gpa DECIMAL, date TIMESTAMP, last_updated TIMESTAMP);");
        //Assert.assertTrue(createResult.isSuccess(),"Should not fail to create a table in H2.");
    }

    /**
     * Insert into the mySQL table created.
     */
    void insertIntoStudentTable() {
        String insertDate = toDatePlusOneHour;
        //scala.util.Try insertResult = jdbcConnection.execute("INSERT INTO ad values ('Bobbert', 1234, 3.1, ts '" + toDatePlusOneHour + "', ts '" + toDatePlusOneHour + "')");//CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)");
        //Assert.assertTrue(insertResult.isSuccess(), "Should be able to insert data into the new table.");
    }

    /**
     * Create mocks as necessary.
     */
    @BeforeTest
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        jdbcEncFactory.emitter = serviceEmitter;
        jdbcEncFactory.lookupService = lookupService;
    }


    /**
     * Set all variables used in your local mySQL and Kafka instances, and initialize both.
     * @throws Exception
     */
    @BeforeClass
    public void init() throws Exception {
        jdbcUrl = "jdbc:mysql://localhost:3306/test?serverTimezone=UTC";
        userName = "root";
        passWord = "password";
        jdbcConnectorConfig = "{\"connectURI\":\"" + jdbcUrl + "\", \"user\":\"" + userName + "\", \"password\":\"" + passWord + "\"}";

        initJdbcToMySql();
        buildJdbcTablesToQuery();
        insertIntoStudentTable();
        setKafkaProperties();
    }

    @AfterClass
    public void shutDown() {
    }

    /**
     * Test Consumer Logic is able to parse a message from a Kafka topic of the format generated
     * by the Producer & convert it into a correct DruidLookup cache.
     */

    @Test
    public void testCreateJdbcLookupOnMySqlAsFollower() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new ObjectMapper()
        .readerFor(MetadataStorageConnectorConfig.class)
        .readValue(jdbcConnectorConfig);

        //new MetadataStorageConnectorConfig();
        JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace =
                new JDBCExtractionNamespaceWithLeaderAndFollower(
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date", "last_updated", "title", "status")),
                        "id", "date", new Period(), true, "ad_lookup", "ad_test", false, kafkaProperties);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "3.1", toDatePlusOneHour, toDatePlusOneHour));
        Callable<String> populator = jdbcEncFactory.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, "0", map);//, kafkaProperties, new TestProtobufSchemaFactory(), "topic");
        System.err.println("Callable Result: " + populator.call());
    }

    /**
     * Test Producer logic is able to ingest a full Oracle table & write that message back to a Kafka
     * topic.
     */
    @Test
    public void testCreateJdbcLookupOnMySqlAsLeader() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new ObjectMapper()
                .readerFor(MetadataStorageConnectorConfig.class)
                .readValue(jdbcConnectorConfig);

        String currentDate = (new SimpleDateFormat("hh:mm:ss")).format(new Date());

        //new MetadataStorageConnectorConfig();
        JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace =
                new JDBCExtractionNamespaceWithLeaderAndFollower(
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date", "last_updated", "title", "status")),
                        "id", "date", new Period(), true, "ad_lookup", "ad_test", true, kafkaProperties);
        extractionNamespace.setFirstTimeCaching(true);
        extractionNamespace.setPreviousLastUpdateTimestamp(new Timestamp(currentDateTime.getMillis()));
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "3.1", currentDate, currentDate));
        Callable<String> populator = jdbcEncFactory.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, "0", map);//, kafkaProperties, new TestProtobufSchemaFactory(), "topic");
        System.err.println("Callable Result: " + populator.call());
    }
}
