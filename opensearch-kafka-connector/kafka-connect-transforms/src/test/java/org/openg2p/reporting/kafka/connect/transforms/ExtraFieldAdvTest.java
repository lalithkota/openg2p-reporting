package org.openg2p.reporting.kafka.connect.transforms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExtraFieldAdvTest {
    private final ExtractFieldAdv<SourceRecord> extractNewFieldAdvTransform = new ExtractFieldAdv.Value<>();
    private final JsonConverter jsonConverter = new JsonConverter();

    @AfterEach
    public void teardown() {
        extractNewFieldAdvTransform.close();
        jsonConverter.close();
    }

    @Test
    public void testSimpleExtractSchemaless() {
        Map<String, String> config = new HashMap<>();
        config.put(ExtractFieldAdv.FIELD_CONFIG, "after");
        
        extractNewFieldAdvTransform.configure(config);
        
        Map<String, String> jsonConverterConfig = new HashMap<>();
        jsonConverterConfig.put("schemas.enable", "false");
        jsonConverterConfig.put("converter.type", "value");
        jsonConverter.configure(jsonConverterConfig);

        SchemaAndValue original = jsonConverter.toConnectData(
            "topic", 
            "{\"after\":{\"finalize\":123,\"ok_value\":\"abcd\"}}".getBytes()
        );

        SourceRecord transformed = extractNewFieldAdvTransform.apply(createTestRecord(original));

        assertEquals("abcd", ((Map<String, Object>) transformed.value()).get("ok_value"));
        assertEquals(123l, ((Map<String, Object>) transformed.value()).get("finalize"));
    }

    @Test
    public void testSingleRenameMergeSchemaless() {
        Map<String, String> config = new HashMap<>();
        config.put(ExtractFieldAdv.FIELD_CONFIG, "source.ts_ms->source_ts_ms,payload.after");

        extractNewFieldAdvTransform.configure(config);

        Map<String, String> jsonConverterConfig = new HashMap<>();
        jsonConverterConfig.put("schemas.enable", "false");
        jsonConverterConfig.put("converter.type", "value");
        jsonConverter.configure(jsonConverterConfig);

        SchemaAndValue original = jsonConverter.toConnectData(
            "topic",
            "{\"payload\":{\"after\": {\"finalize\":123}}, \"source\": {\"ts_ms\":\"1234\"}}".getBytes()
        );

        SourceRecord transformed = extractNewFieldAdvTransform.apply(createTestRecord(original));

        assertEquals("1234", ((Map<String, Object>) transformed.value()).get("source_ts_ms"));
        assertEquals(123l, ((Map<String, Object>) transformed.value()).get("finalize"));
    }

    @Test
    public void testSingleMergeSchemaless() {
        Map<String, String> config = new HashMap<>();
        config.put(ExtractFieldAdv.FIELD_CONFIG, "source,after");

        extractNewFieldAdvTransform.configure(config);

        Map<String, String> jsonConverterConfig = new HashMap<>();
        jsonConverterConfig.put("schemas.enable", "false");
        jsonConverterConfig.put("converter.type", "value");
        jsonConverter.configure(jsonConverterConfig);

        SchemaAndValue original = jsonConverter.toConnectData(
            "topic",
            "{\"after\": {\"finalize\":123}, \"source\": {\"ts_ms\":\"1234\"}}".getBytes()
        );

        SourceRecord transformed = extractNewFieldAdvTransform.apply(createTestRecord(original));

        assertEquals("1234", ((Map<String, Object>) transformed.value()).get("ts_ms"));
        assertEquals(123l, ((Map<String, Object>) transformed.value()).get("finalize"));
    }

    @Test
    public void testMultiMergeNestedSchemaless() {
        Map<String, String> config = new HashMap<>();
        config.put(ExtractFieldAdv.FIELD_CONFIG, "source,after,report");

        extractNewFieldAdvTransform.configure(config);

        Map<String, String> jsonConverterConfig = new HashMap<>();
        jsonConverterConfig.put("schemas.enable", "false");
        jsonConverterConfig.put("converter.type", "value");
        jsonConverter.configure(jsonConverterConfig);

        SchemaAndValue original = jsonConverter.toConnectData(
            "topic",
            ("{" +
            "    \"after\": {" +
            "        \"finalize\": {" +
            "            \"test2\":\"val2\"," +
            "            \"preamble\":null," +
            "            \"array\":[\"list2\"]" +
            "        }" +
            "    }," +
            "    \"source\": {" +
            "        \"finalize\":{" +
            "            \"test1\":\"val1\"," +
            "            \"preamble\":\"fine\"," +
            "            \"array\":[\"list1\"]" +
            "        }" +
            "    }," +
            "    \"report\": {" +
            "        \"finalize\":{" +
            "            \"test3\":\"val3\"," +
            "            \"test2\":\"present\"," +
            "            \"array\":[\"list3\"]" +
            "        }" +
            "    }" +
            "}").getBytes()
        );

        SourceRecord transformed = extractNewFieldAdvTransform.apply(createTestRecord(original));

        Map<String, Object> transformedFinalized = (Map<String, Object>)((Map<String, Object>)transformed.value()).get("finalize");
        List<String> transformedFinalizedArray = (List<String>)transformedFinalized.get("array");
        assertNull(transformedFinalized.get("preamble"));
        assertEquals("val1", transformedFinalized.get("test1"));
        assertEquals("present", transformedFinalized.get("test2"));
        assertEquals("val3", transformedFinalized.get("test3"));
        assertEquals("list1", transformedFinalizedArray.get(0));
        assertEquals("list2", transformedFinalizedArray.get(1));
        assertEquals("list3", transformedFinalizedArray.get(2));
    }

    @Test
    public void testMergeExceptionSchemaless() {
        Map<String, String> config = new HashMap<>();
        config.put(ExtractFieldAdv.FIELD_CONFIG, "source,after");

        extractNewFieldAdvTransform.configure(config);

        Map<String, String> jsonConverterConfig = new HashMap<>();
        jsonConverterConfig.put("schemas.enable", "false");
        jsonConverterConfig.put("converter.type", "value");
        jsonConverter.configure(jsonConverterConfig);

        SchemaAndValue original = jsonConverter.toConnectData(
            "topic",
            "{\"after\": 123, \"source\": {\"ts_ms\":\"1234\"}}".getBytes()
        );

        DataException exception = assertThrows(DataException.class, () -> extractNewFieldAdvTransform.apply(createTestRecord(original)));
        assertEquals("ExtractNewFieldAdv: One of the maps trying to be merged is a primitive.", exception.getMessage());
    }

    @Test
    public void testMultiMergeNestedWithSchema() {
        Map<String, String> config = new HashMap<>();
        config.put(ExtractFieldAdv.FIELD_CONFIG, "source,after,report");

        extractNewFieldAdvTransform.configure(config);

        Map<String, String> jsonConverterConfig = new HashMap<>();
        jsonConverterConfig.put("schemas.enable", "true");
        jsonConverterConfig.put("converter.type", "value");
        jsonConverter.configure(jsonConverterConfig);

        // TODO: Restructure
        // SchemaAndValue original = jsonConverter.toConnectData(
        //     "topic",
        //     ("{" +
        //     "    \"after\": {" +
        //     "        \"finalize\": {" +
        //     "            \"test2\":\"val2\"," +
        //     "            \"preamble\":null," +
        //     "            \"array\":[\"list2\"]" +
        //     "        }" +
        //     "    }," +
        //     "    \"source\": {" +
        //     "        \"finalize\":{" +
        //     "            \"test1\":\"val1\"," +
        //     "            \"preamble\":\"fine\"," +
        //     "            \"array\":[\"list1\"]" +
        //     "        }" +
        //     "    }," +
        //     "    \"report\": {" +
        //     "        \"finalize\":{" +
        //     "            \"test3\":\"val3\"," +
        //     "            \"test2\":\"present\"," +
        //     "            \"array\":[\"list3\"]" +
        //     "        }" +
        //     "    }" +
        //     "}").getBytes()
        // );

        // SourceRecord transformed = extractNewFieldAdvTransform.apply(createTestRecord(original));

        // Map<String, Object> transformedFinalized = (Map<String, Object>)((Map<String, Object>)transformed.value()).get("finalize");
        // List<String> transformedFinalizedArray = (List<String>)transformedFinalized.get("array");
        // assertNull(transformedFinalized.get("preamble"));
        // assertEquals("val1", transformedFinalized.get("test1"));
        // assertEquals("present", transformedFinalized.get("test2"));
        // assertEquals("val3", transformedFinalized.get("test3"));
        // assertEquals("list1", transformedFinalizedArray.get(0));
        // assertEquals("list2", transformedFinalizedArray.get(1));
        // assertEquals("list3", transformedFinalizedArray.get(2));
    }

    private SourceRecord createTestRecord(SchemaAndValue schemaAndValue) {
        return new SourceRecord(null, null, "topic", 0, schemaAndValue.schema(), schemaAndValue.value());
    }
}
