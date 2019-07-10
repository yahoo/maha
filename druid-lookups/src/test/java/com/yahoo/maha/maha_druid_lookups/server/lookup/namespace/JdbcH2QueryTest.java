package com.yahoo.maha.maha_druid_lookups.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.emitter.service.ServiceEmitter;
import com.yahoo.maha.jdbc.JdbcConnection;
import com.yahoo.maha.maha_druid_lookups.query.lookup.namespace.JDBCExtractionNamespaceWithLeaderAndFollower;
import com.yahoo.maha.maha_druid_lookups.server.lookup.namespace.entity.TestProtobufSchemaFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
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
 * Follower Role:   Reads from Kafka topic & writes back to Druid.
 *
 * Progress:        setIsLeader ensures leader functionality, but consumer currently does nothing with data
 *                  parsed from its Topic except to return an updated TS (no cache writeback yet).
 */
public class JdbcH2QueryTest {

    private HikariConfig config;
    private String h2dbId = UUID.randomUUID().toString().replace("-", "");
    private HikariDataSource ds;
    private JdbcConnection jdbcConnection;

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

    void setKafkaProperties() {
        kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put("group.id", "test-consumer-group");
    }

    void initJdbcToH2() {
        config = new HikariConfig();
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

    void buildJdbcTablesToQuery() {
        scala.util.Try createResult = jdbcConnection.execute(
                "CREATE TABLE ad (name VARCHAR2(255), id BIGINT, gpa DECIMAL, date TIMESTAMP, last_updated TIMESTAMP, title VARCHAR2(255), status VARCHAR2(255));");
        Assert.assertTrue(createResult.isSuccess(),"Should not fail to create a table in H2.");
    }

    void insertIntoStudentTable() {
        String insertDate = toDatePlusOneHour;
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
        //jdbcEncFactory.protobufSchemaFactory = new TestProtobufSchemaFactory();
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
                        System.err.println("Inserted data TS is: " + ts.toString());
                        return null;
                    } catch (SQLException e) {
                        return new scala.util.Failure(e);
                    }
                }));
        Assert.assertTrue(queryResult.isSuccess(), "Should be able to query the advertiser table." + queryResult.toString());
    }

    /**
     * Test getCachePopulator in JDBC EN Cache Factory using non-leader logic
     * (basic lookup & write-back).
     */
    @Test
    public void testCreateJdbcLookupOnH2() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new ObjectMapper()
        .readerFor(MetadataStorageConnectorConfig.class)
        .readValue(jdbcConnectorConfig);

        //new MetadataStorageConnectorConfig();
        JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace =
                new JDBCExtractionNamespaceWithLeaderAndFollower(
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date", "last_updated")),
                        "id", "last_updated", new Period(), true,
                        "ad_lookup", "ad_test", false, kafkaProperties);
        Map<String, List<String>> map = new HashMap<>();
        map.put("12345", Arrays.asList("12345", "my name", "3.1", toDatePlusOneHour));
        Callable<String> populator = jdbcEncFactory.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, "0", map);
        System.err.println("Callable Result: " + populator.call());
    }

    /**
     * Test getCachePopulator in JDBC EN Cache Factory using Leader logic
     * Should create and write to a kafka topic.
     */
    @Test
    public void testCreateJdbcLookupOnH2asLeader() throws Exception {

        MetadataStorageConnectorConfig metadataStorageConnectorConfig = new ObjectMapper()
                .readerFor(MetadataStorageConnectorConfig.class)
                .readValue(jdbcConnectorConfig);

        String currentDate = (new SimpleDateFormat("hh:mm:ss")).format(new Date());

        //new MetadataStorageConnectorConfig();
        JDBCExtractionNamespaceWithLeaderAndFollower extractionNamespace =
                new JDBCExtractionNamespaceWithLeaderAndFollower(
                        metadataStorageConnectorConfig, "ad", new ArrayList<>(Arrays.asList("id","name","gpa","date", "last_updated", "title", "status")),
                        "id", "last_updated", new Period(), false,
                        "ad_lookup", "ad_test", true, kafkaProperties);

        extractionNamespace.setFirstTimeCaching(false);
        extractionNamespace.setPreviousLastUpdateTimestamp(new Timestamp(currentDateTime.getMillis()));

        Map<String, List<String>> map = new HashMap<>();
        Callable<String> populator = jdbcEncFactory.getCachePopulator(extractionNamespace.getLookupName(), extractionNamespace, "0", map);
        System.err.println("Callable Result: " + populator.call());
    }
}
