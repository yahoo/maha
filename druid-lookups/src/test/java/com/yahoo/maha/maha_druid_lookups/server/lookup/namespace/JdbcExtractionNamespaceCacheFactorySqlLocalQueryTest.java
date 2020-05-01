package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespace;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Ignore;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.*;

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
public class JdbcExtractionNamespaceCacheFactorySqlLocalQueryTest {

    private HikariConfig config;
    private String h2dbId = UUID.randomUUID().toString().replace("-", "");
    private HikariDataSource ds;
    private Statement jdbcConnection;

    private JDBCExtractionNamespaceCacheFactory jdbcEncFactory = new JDBCExtractionNamespaceCacheFactory();
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
     * Set up local Kafka Producer and Consumer, which will share properties.
     * Bootstrap Kafka Config using default quickstart local port for local testing.
     */
    void setKafkaProperties() {
        kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        kafkaProperties.put("key.serializer", StringSerializer.class.getName());
        kafkaProperties.put("value.serializer", ByteArraySerializer.class.getName());
        kafkaProperties.put("group.id", "test-consumer-group");
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
        ResultSet testQuery = jdbcConnection.executeQuery("SELECT 1 from DUAL;");
        testQuery.next();
        Assert.assertTrue(testQuery.isFirst());
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
        jdbcUrl = "jdbc:mysql://localhost:3306/sys?serverTimezone=UTC";
        userName = "root";
        passWord = "password";
        jdbcConnectorConfig = "{\"connectURI\":\"" + jdbcUrl + "\", \"user\":\"" + userName + "\", \"password\":\"" + passWord + "\"}";

        initJdbcToMySql();
        setKafkaProperties();
    }

    @AfterClass
    public void shutDown() {
        serviceEmitter = null;
        lookupService = null;
    }

    /**
     * Test Consumer Logic is able to parse a message from a Kafka topic of the format generated
     * by the Producer & convert it into a correct DruidLookup cache.
     */

    @Ignore
    public void testCreateJdbcLookupOnMySqlAsFollower() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new ObjectMapper()
        .readerFor(MetadataStorageConnectorConfig.class)
        .readValue(jdbcConnectorConfig);

        //new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date", "last_updated", "title", "status")),
                        "id", "date", new Period(3000L), true, "ad_lookup");
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "3.1", toDatePlusOneHour, toDatePlusOneHour));
        Callable<String> populator = jdbcEncFactory.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, "0", map);//, kafkaProperties, new TestProtobufSchemaFactory(), "topic");
        System.err.println("Callable Result: " + populator.call());
    }

    /**
     * Test Producer logic is able to ingest a full Oracle table & write that message back to a Kafka
     * topic.
     */
    @Ignore
    public void testCreateJdbcLookupOnMySqlAsLeader() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new ObjectMapper()
                .readerFor(MetadataStorageConnectorConfig.class)
                .readValue(jdbcConnectorConfig);

        String currentDate = (new SimpleDateFormat("hh:mm:ss")).format(new Date());

        //new MetadataStorageConnectorConfig();
        JDBCExtractionNamespace extractionNamespace =
                new JDBCExtractionNamespace(
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date", "last_updated", "title", "status")),
                        "id", "date", new Period(3000L), true, "ad_lookup");
        extractionNamespace.setFirstTimeCaching(true);
        extractionNamespace.setPreviousLastUpdateTimestamp(new Timestamp(currentDateTime.getMillis()));
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "3.1", currentDate, currentDate));
        Callable<String> populator = jdbcEncFactory.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, "0", map);//, kafkaProperties, new TestProtobufSchemaFactory(), "topic");
        System.err.println("Callable Result: " + populator.call());
    }
}
