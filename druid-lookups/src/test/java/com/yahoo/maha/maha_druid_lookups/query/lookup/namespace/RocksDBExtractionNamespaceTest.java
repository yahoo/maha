package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.joda.deser.DurationDeserializer;
import com.fasterxml.jackson.datatype.joda.deser.PeriodDeserializer;
import com.fasterxml.jackson.datatype.joda.deser.key.DateTimeKeyDeserializer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Note: these classes are testing Jackson version 2.9.9, and druid-server
 * 0.11.0 uses Jackson 2.4.*
 * Multiple bindings (ExtractionNamespace) are not supported before Jackson 2.6.*,
 * so whichever Binding is mentioned first is used by Druid.
 */
public class RocksDBExtractionNamespaceTest {
    private static final ObjectMapper objectMapper = new DefaultObjectMapper();


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
        System.out.println(period);
    }

    @Test
    public void successfullyDeserializeFullNamespaceFromJSON() throws Exception {
        RocksDBExtractionNamespace namespace = objectMapper
                .readValue(ClassLoader.getSystemClassLoader().getResourceAsStream("rocksdb_extraction_namespace.json")
                        , RocksDBExtractionNamespace.class);
        assertEquals(namespace.getNamespace(), "advertiser");
        assertEquals(namespace.getRocksDbInstanceHDFSPath(), "/data/druid/lookups/snapshots/advertiser");
        assertEquals(namespace.getLookupAuditingHDFSPath(), "/data/druid/lookups/audits/advertiser");
        assertEquals(namespace.getPollMs(), 30000);
        assertEquals(namespace.isCacheEnabled(), true);
        assertEquals(namespace.getLookupName(), "advertiser_lookup");

    }

    @Test
    public void successfullyDeserializeFullNamespaceFromInMemoryDbJSON() throws Exception {
        RocksDBExtractionNamespace namespace = objectMapper
                .readValue(ClassLoader.getSystemClassLoader().getResourceAsStream("inmemorydb_extraction_namespace.json")
                        , RocksDBExtractionNamespace.class);
        assertEquals(namespace.getNamespace(), "advertiser");
        assertEquals(namespace.getRocksDbInstanceHDFSPath(), "/data/druid/lookups/snapshots/advertiser");
        assertEquals(namespace.getLookupAuditingHDFSPath(), "/data/druid/lookups/audits/advertiser");
        assertEquals(namespace.getPollMs(), 30000);
        assertEquals(namespace.isCacheEnabled(), true);
        assertEquals(namespace.getLookupName(), "advertiser_lookup");
        assertEquals(namespace.getOverrideLookupServiceHosts(), "[]");
        assertEquals(namespace.getOverrideLookupServiceHostsList().size(), 0);
    }

    @Test
    public void testOverrideLookupHosts() throws Exception{
        RocksDBExtractionNamespace namespace = objectMapper
                .readValue(ClassLoader.getSystemClassLoader().getResourceAsStream("rocksdb_extraction_namespace_w_serviceHostList.json")
                        , RocksDBExtractionNamespace.class);
        assertEquals(namespace.getOverrideLookupServiceHosts(),"[http://test.com:1234,http://localhost:8090,https://test.com:8090,http://127.0.0.1:9999]");
        assertEquals(namespace.getOverrideLookupServiceHostsList().size(), 4);
    }
}
