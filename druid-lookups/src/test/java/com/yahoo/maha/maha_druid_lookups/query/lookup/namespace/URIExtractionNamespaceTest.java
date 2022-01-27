package com.yahoo.maha.maha_druid_lookups.query.lookup.namespace;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Module;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.guice.GuiceInjectableValues;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Note: these classes are testing Jackson version 2.9.9, and druid-server
 * 0.11.0 uses Jackson 2.4.*
 * Multiple bindings (ExtractionNamespace) are not supported before Jackson 2.6.*,
 * so whichever Binding is mentioned first is used by Druid.
 */
public class URIExtractionNamespaceTest {
    static {
        NullHandling.initializeForTests();
    }

    public static ObjectMapper registerTypes(
            final ObjectMapper mapper
    )
    {
        mapper.setInjectableValues(
                new GuiceInjectableValues(
                        Guice.createInjector(
                                ImmutableList.of(
                                        new Module()
                                        {
                                            @Override
                                            public void configure(Binder binder)
                                            {
                                                binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(mapper);
                                                binder.bind(ObjectMapper.class).toInstance(mapper);
                                            }
                                        }
                                )
                        )
                )
        ).registerSubtypes(URIExtractionNamespace.class, URIExtractionNamespace.FlatDataParser.class);

        final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
        mapper.setAnnotationIntrospectors(
                new AnnotationIntrospectorPair(
                        guiceIntrospector,
                        mapper.getSerializationConfig().getAnnotationIntrospector()
                ),
                new AnnotationIntrospectorPair(
                        guiceIntrospector,
                        mapper.getDeserializationConfig().getAnnotationIntrospector()
                )
        );
        return mapper;
    }

    @Test
    public void testCSV()
    {
        URIExtractionNamespace.CSVFlatDataParser parser = new URIExtractionNamespace.CSVFlatDataParser(
                ImmutableList.of(
                        "col1",
                        "col2",
                        "col3"
                ), "col2", "col3"
        );
        Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A,B,C"));
    }
    @Test
    public void testCSVWithHeader()
    {
        URIExtractionNamespace.CSVFlatDataParser parser = new URIExtractionNamespace.CSVFlatDataParser(
                ImmutableList.of("col1", "col2", "col3"),
                "col2",
                "col3",
                true,
                1
        );
        // parser return empyt list as the 1 row header need to be skipped.
        Assert.assertEquals(ImmutableMap.of(), parser.getParser().parseToMap("row to skip "));
        //Header also need to be skipped.
        Assert.assertEquals(ImmutableMap.of(), parser.getParser().parseToMap("col1,col2,col3"));
        // test the header is parsed
        Assert.assertEquals(ImmutableList.of("col1", "col2", "col3"), parser.getParser().getFieldNames());
        // The third row will parse to data
        Assert.assertEquals(ImmutableMap.of("val2", "val3"), parser.getParser().parseToMap("val1,val2,val3"));
    }
    @Test(expected = IllegalArgumentException.class)
    public void testBadCSV()
    {
        URIExtractionNamespace.CSVFlatDataParser parser = new URIExtractionNamespace.CSVFlatDataParser(
                ImmutableList.of(
                        "col1",
                        "col2",
                        "col3"
                ), "col2", "col3ADFSDF"
        );
        Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A,B,C"));
    }

    @Test(expected = NullPointerException.class)
    public void testBadCSV2()
    {
        URIExtractionNamespace.CSVFlatDataParser parser = new URIExtractionNamespace.CSVFlatDataParser(
                ImmutableList.of(
                        "col1",
                        "col2",
                        "col3"
                ), "col2", "col3"
        );
        Map<String, String> map = parser.getParser().parseToMap("A");
    }

    @Test
    public void testTSV()
    {
        URIExtractionNamespace.TSVFlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
                ImmutableList.of("col1", "col2", "col3"),
                "|",
                null, "col2",
                "col3"
        );
        Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A|B|C"));
    }

    @Test
    public void testWithListDelimiterTSV()
    {
        URIExtractionNamespace.TSVFlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
                ImmutableList.of("col1", "col2", "col3"),
                "\\u0001",
                "\\u0002", "col2",
                "col3"
        );
        Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A\\u0001B\\u0001C"));
    }
    @Test
    public void testWithHeaderAndListDelimiterTSV()
    {
        URIExtractionNamespace.TSVFlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
                ImmutableList.of("col1", "col2", "col3"),
                "\\u0001",
                "\\u0002", "col2",
                "col3",
                true,
                1
        );
        // skipping one row
        Assert.assertEquals(ImmutableMap.of(), parser.getParser().parseToMap("Skipping some rows"));
        // skip the header as well
        Assert.assertEquals(ImmutableMap.of(), parser.getParser().parseToMap("col1\\u0001col2\\u0001col3"));
        // test if the headers are parsed well.
        Assert.assertEquals(ImmutableList.of("col1", "col2", "col3"), parser.getParser().getFieldNames());
        // test if the data row is parsed correctly
        Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A\\u0001B\\u0001C"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadTSV()
    {
        URIExtractionNamespace.TSVFlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
                ImmutableList.of("col1", "col2", "col3fdsfds"),
                ",",
                null, "col2",
                "col3"
        );
        Map<String, String> map = parser.getParser().parseToMap("A,B,C");
        Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A,B,C"));
    }


    @Test(expected = NullPointerException.class)
    public void testBadTSV2()
    {
        URIExtractionNamespace.TSVFlatDataParser parser = new URIExtractionNamespace.TSVFlatDataParser(
                ImmutableList.of("col1", "col2", "col3"),
                ",",
                null, "col2",
                "col3"
        );
        Map<String, String> map = parser.getParser().parseToMap("A");
        Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("A,B,C"));
    }

    @Test
    public void testJSONFlatDataParser()
    {
        final String keyField = "keyField";
        final String valueField = "valueField";
        URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
                new ObjectMapper(),
                keyField,
                valueField
        );
        Assert.assertEquals(
                ImmutableMap.of("B", "C"),
                parser.getParser()
                        .parseToMap(
                                StringUtils.format(
                                        "{\"%s\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                                        keyField,
                                        valueField
                                )
                        )
        );
    }


    @Test(expected = NullPointerException.class)
    public void testJSONFlatDataParserBad()
    {
        final String keyField = "keyField";
        final String valueField = "valueField";
        URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
                new ObjectMapper(),
                keyField,
                valueField
        );
        Assert.assertEquals(
                ImmutableMap.of("B", "C"),
                parser.getParser()
                        .parseToMap(
                                StringUtils.format(
                                        "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                                        keyField,
                                        valueField
                                )
                        )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJSONFlatDataParserBad2()
    {
        final String keyField = "keyField";
        final String valueField = "valueField";
        URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
                registerTypes(new ObjectMapper()),
                null,
                valueField
        );
        Assert.assertEquals(
                ImmutableMap.of("B", "C"),
                parser.getParser()
                        .parseToMap(
                                StringUtils.format(
                                        "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                                        keyField,
                                        valueField
                                )
                        )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJSONFlatDataParserBad3()
    {
        final String keyField = "keyField";
        final String valueField = "valueField";
        URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
                registerTypes(new ObjectMapper()),
                keyField,
                null
        );
        Assert.assertEquals(
                ImmutableMap.of("B", "C"),
                parser.getParser()
                        .parseToMap(
                                StringUtils.format(
                                        "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                                        keyField,
                                        valueField
                                )
                        )
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJSONFlatDataParserBad4()
    {
        final String keyField = "keyField";
        final String valueField = "valueField";
        URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
                registerTypes(new ObjectMapper()),
                "",
                ""
        );
        Assert.assertEquals(
                ImmutableMap.of("B", "C"),
                parser.getParser()
                        .parseToMap(
                                StringUtils.format(
                                        "{\"%sDFSDFDS\":\"B\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                                        keyField,
                                        valueField
                                )
                        )
        );
    }

    @Test
    public void testObjectMapperFlatDataParser()
    {
        URIExtractionNamespace.ObjectMapperFlatDataParser parser = new URIExtractionNamespace.ObjectMapperFlatDataParser(
                registerTypes(new ObjectMapper())
        );
        Assert.assertEquals(ImmutableMap.of("B", "C"), parser.getParser().parseToMap("{\"B\":\"C\"}"));
    }

    @Test
    public void testSimpleJSONSerDe() throws IOException
    {
        final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
        for (URIExtractionNamespace.FlatDataParser parser : ImmutableList.of(
                new URIExtractionNamespace.CSVFlatDataParser(
                        ImmutableList.of(
                                "col1",
                                "col2",
                                "col3"
                        ), "col2", "col3"
                ),
                new URIExtractionNamespace.ObjectMapperFlatDataParser(mapper),
                new URIExtractionNamespace.JSONFlatDataParser(mapper, "keyField", "valueField"),
                new URIExtractionNamespace.TSVFlatDataParser(ImmutableList.of("A", "B"), ",", null, "A", "B")
        )) {
            final String str = mapper.writeValueAsString(parser);
            final URIExtractionNamespace.FlatDataParser parser2 = mapper.readValue(
                    str,
                    URIExtractionNamespace.FlatDataParser.class
            );
            Assert.assertEquals(str, mapper.writeValueAsString(parser2));
        }
    }

    @Test
    public void testSimpleToString()
    {
        final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
        for (URIExtractionNamespace.FlatDataParser parser : ImmutableList.of(
                new URIExtractionNamespace.CSVFlatDataParser(
                        ImmutableList.of(
                                "col1",
                                "col2",
                                "col3"
                        ), "col2", "col3"
                ),
                new URIExtractionNamespace.ObjectMapperFlatDataParser(mapper),
                new URIExtractionNamespace.JSONFlatDataParser(mapper, "keyField", "valueField"),
                new URIExtractionNamespace.TSVFlatDataParser(ImmutableList.of("A", "B"), ",", null, "A", "B")
        )) {
            Assert.assertFalse(parser.toString().contains("@"));
        }
    }

    @Test
    public void testMatchedJson() throws IOException
    {
        final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
        URIExtractionNamespace namespace = mapper.readValue(
                "{\"type\":\"uri\", \"uriPrefix\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\", \"versionRegex\":\"a.b.c\", \"namespace\":\"testNamespace\"}",
                URIExtractionNamespace.class
        );

        Assert.assertEquals(
                URIExtractionNamespace.ObjectMapperFlatDataParser.class.getName(),
                namespace.getNamespaceParseSpec().getClass().getName()
        );
        Assert.assertEquals("file:/foo", namespace.getUriPrefix().toString());
        Assert.assertEquals("a.b.c", namespace.getFileRegex());
        Assert.assertEquals(5L * 60_000L, namespace.getPollMs());
    }

    @Test
    public void testExplicitJson() throws IOException
    {
        final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
        URIExtractionNamespace namespace = mapper.readValue(
                "{\"type\":\"uri\", \"uri\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\"}",
                URIExtractionNamespace.class
        );

        Assert.assertEquals(
                URIExtractionNamespace.ObjectMapperFlatDataParser.class.getName(),
                namespace.getNamespaceParseSpec().getClass().getName()
        );
        Assert.assertEquals("file:/foo", namespace.getUri().toString());
        Assert.assertEquals(5L * 60_000L, namespace.getPollMs());
    }

    @Test(expected = JsonMappingException.class)
    public void testExplicitJsonException() throws IOException
    {
        final ObjectMapper mapper = registerTypes(new DefaultObjectMapper());
        mapper.readValue(
                "{\"type\":\"uri\", \"uri\":\"file:/foo\", \"namespaceParseSpec\":{\"format\":\"simpleJson\"}, \"pollPeriod\":\"PT5M\", \"versionRegex\":\"a.b.c\", \"namespace\":\"testNamespace\"}",
                URIExtractionNamespace.class
        );
    }

    @Test
    public void testFlatDataNumeric()
    {
        final String keyField = "keyField";
        final String valueField = "valueField";
        final int n = 341879;
        final String nString = StringUtils.format("%d", n);
        URIExtractionNamespace.JSONFlatDataParser parser = new URIExtractionNamespace.JSONFlatDataParser(
                new ObjectMapper(),
                keyField,
                valueField
        );
        Assert.assertEquals(
                "num string value",
                ImmutableMap.of("B", nString),
                parser.getParser()
                        .parseToMap(
                                StringUtils.format(
                                        "{\"%s\":\"B\", \"%s\":\"%d\", \"FOO\":\"BAR\"}",
                                        keyField,
                                        valueField,
                                        n
                                )
                        )
        );
        Assert.assertEquals(
                "num string key",
                ImmutableMap.of(nString, "C"),
                parser.getParser()
                        .parseToMap(
                                StringUtils.format(
                                        "{\"%s\":\"%d\", \"%s\":\"C\", \"FOO\":\"BAR\"}",
                                        keyField,
                                        n,
                                        valueField
                                )
                        )
        );
        Assert.assertEquals(
                "num value",
                ImmutableMap.of("B", nString),
                parser.getParser()
                        .parseToMap(
                                StringUtils.format(
                                        "{\"%s\":\"B\", \"%s\":%d, \"FOO\":\"BAR\"}",
                                        keyField,
                                        valueField,
                                        n
                                )
                        )
        );
        Assert.assertEquals(
                "num key",
                ImmutableMap.of(nString, "C"),
                parser.getParser()
                        .parseToMap(
                                StringUtils.format(
                                        "{\"%s\":%d, \"%s\":\"C\", \"FOO\":\"BAR\"}",
                                        keyField,
                                        n,
                                        valueField
                                )
                        )
        );
    }

    @Test
    public void testSimpleJsonNumeric()
    {
        final URIExtractionNamespace.ObjectMapperFlatDataParser parser = new URIExtractionNamespace.ObjectMapperFlatDataParser(
                registerTypes(new DefaultObjectMapper())
        );
        final int n = 341879;
        final String nString = StringUtils.format("%d", n);
        Assert.assertEquals(
                ImmutableMap.of("key", nString),
                parser.getParser().parseToMap(StringUtils.format("{\"key\":%d}", n))
        );
    }

}
