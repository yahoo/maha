package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.joda.deser.DurationDeserializer;
import com.fasterxml.jackson.datatype.joda.deser.PeriodDeserializer;
import com.fasterxml.jackson.datatype.joda.deser.key.DateTimeKeyDeserializer;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class MongoExtractionNamespaceTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();


    @BeforeClass
    void setup() {
        SimpleModule module = new SimpleModule();
        module.addKeyDeserializer(DateTime.class, new DateTimeKeyDeserializer());
        module.addSerializer(DateTime.class, ToStringSerializer.instance);
        module.addSerializer(Interval.class, ToStringSerializer.instance);
        JsonDeserializer<?> periodDeserializer = new PeriodDeserializer();
        module.addDeserializer(Period.class, (JsonDeserializer<Period>) periodDeserializer);
        module.addSerializer(Period.class, ToStringSerializer.instance);
        module.addDeserializer(Duration.class, new DurationDeserializer());
        module.addSerializer(Duration.class, ToStringSerializer.instance);

        objectMapper.registerModule(module);

        Period period = Period.seconds(30);
        //System.out.println(period);
    }

    @Test
    public void successfullyDeserializeFullNamespaceFromJSON() throws Exception {
        MongoExtractionNamespace namespace = objectMapper
                .readValue(ClassLoader.getSystemClassLoader().getResourceAsStream("mongo_extraction_namespace.json")
                        , MongoExtractionNamespace.class);
        assertEquals(namespace.getConnectorConfig().getHosts(), "localhost:51240");
        assertEquals(namespace.getConnectorConfig().getDbName(), "advertiser");
        assertEquals(namespace.getConnectorConfig().getUser(), "test-user");
        assertEquals(namespace.getConnectorConfig().getPassword(), "mypassword");
        assertEquals(namespace.getConnectorConfig().getClientProperties().getProperty("socketTimeout").toString(), "30000");
        assertEquals(namespace.getCollectionName(), "advertiser");
        assertEquals(namespace.getTsColumn(), "updated_at");
        assertEquals(namespace.isTsColumnEpochInteger(), true);
        assertEquals(namespace.getPollMs(), 30000);
        assertEquals(namespace.isCacheEnabled(), true);
        assertEquals(namespace.getLookupName(), "advertiser_lookup");
        assertEquals(namespace.getDocumentProcessor().getColumnList(), Lists.newArrayList("name", "currency", "status"));
        assertEquals(namespace.getDocumentProcessor().getPrimaryKeyColumn(), "_id");
        assertEquals(namespace.getMongoClientRetryCount(), 3);

    }
}
