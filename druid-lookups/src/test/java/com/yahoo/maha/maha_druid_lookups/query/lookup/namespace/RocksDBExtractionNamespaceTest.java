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

public class RocksDBExtractionNamespaceTest {
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

    }
}
