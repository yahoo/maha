package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.mongodb.MongoClientOptions;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoStorageConnectorConfigTest {

    @Test
    public void successFullyDeserializeFullConfig() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Integer> integerMap = new HashMap<>();
        Map<String, Boolean> boolMap = new HashMap<>();

        int i = 2000;
        for (String prop : MongoStorageConnectorConfig.INT_PROPERTIES) {
            integerMap.put(prop, i--);
        }

        for (String prop : MongoStorageConnectorConfig.BOOL_PROPERTIES) {
            boolMap.put(prop, true);
        }

        Joiner commaJoiner = Joiner.on(",");

        List<String> props = new ArrayList<>();
        integerMap.entrySet().stream().forEach(entry -> props.add(String.format("\"%s\":\"%s\"", entry.getKey(), entry.getValue())));
        boolMap.entrySet().stream().forEach(entry -> props.add(String.format("\"%s\":\"%s\"", entry.getKey(), entry.getValue())));

        String clientOptionsJson = commaJoiner.join(props);

        String jsonConfig = String.format("{\n" +
                "\t\"hosts\": \"host1:2001,host2:2002,host3:2003\",\n" +
                "\t\"dbName\": \"mydb\",\n" +
                "\t\"user\": \"test-user\",\n" +
                "\t\"password\": {\n" +
                "\t\t\"type\": \"default\",\n" +
                "\t\t\"password\": \"mypassword\"\n" +
                "\t},\n" +
                "\t\"clientOptions\": {\n%s" +
                "\t}\n" +
                "}", clientOptionsJson);

        MongoStorageConnectorConfig read = objectMapper.readValue(jsonConfig, MongoStorageConnectorConfig.class);
        assertEquals(read.getHosts(), "host1:2001,host2:2002,host3:2003");
        assertEquals(read.getDbName(), "mydb");
        assertEquals(read.getUser(), "test-user");
        assertEquals(read.getPassword(), "mypassword");
        MongoClientOptions options = read.getMongoClientOptions();

        Class<MongoClientOptions> clazz = MongoClientOptions.class;
        for (String p : MongoStorageConnectorConfig.INT_PROPERTIES) {
            Integer value = integerMap.get(p);
            if (value != null) {
                try {
                    String methodName = String.format("get%s%s", p.toUpperCase().charAt(0), p.substring(1));
                    Method m = clazz.getMethod(methodName);
                    assertEquals(m.invoke(options), value);
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Failed to parse int for property : %s=%s", p, value), e);
                }
            }
        }
        for (String p : MongoStorageConnectorConfig.BOOL_PROPERTIES) {
            Boolean value = boolMap.get(p);
            if (value != null) {
                try {
                    String methodName = String.format("is%s%s", p.toUpperCase().charAt(0), p.substring(1));
                    Method m = clazz.getMethod(methodName);
                    assertEquals(m.invoke(options), value);
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Failed to parse boolean for property : %s=%s", p, value), e);
                }
            }
        }
    }
}
