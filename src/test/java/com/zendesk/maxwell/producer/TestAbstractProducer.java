package com.zendesk.maxwell.producer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.BinlogPosition;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;

public class TestAbstractProducer {
    public static final TypeReference<Map<String, Object>> MAP_STRING_OBJECT_REF =
            new TypeReference<Map<String, Object>>() {
            };

    private RowMap rowMap;
    private ColumnDef idCol;
    private ObjectMapper mapper = new ObjectMapper();

    private Map<String, Object> parseJson(String json) throws Exception {
        return mapper.readValue(json, MAP_STRING_OBJECT_REF);
    }

    private class TestProducer extends AbstractProducer {
        TestProducer(Format format) {
            super(new MaxwellContext(new MaxwellConfig()), EnumSet.allOf(Format.class), format);
        }

        @Override
        public void push(RowMap r) throws Exception {
        }
    }


    @Before
    public void setup() {
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

        rowMap = new RowMap("insert", "testdb", "test_t", 0L, Arrays.asList("id"),
                            new BinlogPosition(0, "binlogpos.test"));

        idCol = new IntColumnDef("test_t", "id", "int", 0, false);

        rowMap.putData("id", idCol, 2);
        rowMap.putOldData("id", idCol, 1);
    }

    @Test
    public void testJsonSerialize() throws Exception {
        byte[] rowBytes = new TestProducer(AbstractProducer.Format.JSON).serialize(rowMap, null);
        Map<String, Object> outputMap = parseJson(new String(rowBytes, "UTF-8"));
        assertThat(outputMap,
                   is(parseJson("{\"type\":\"insert\",\"database\":\"testdb\"" +
                                ",\"table\":\"test_t\",\"ts\":0" +
                                ",\"data\": {\"id\": 2}, \"old\":{\"id\":1}}")));
    }

    @Test
    public void testAvroJsonSerialize() throws Exception {
        byte[] rowBytes = new TestProducer(AbstractProducer.Format.AVRO_JSON)
                .serialize(rowMap, "test");

        String schemaStr = rowMap.generateAvroSchema("test", null).toString().replace("\"", "\\\"");

        Map<String, Object> outputMap = parseJson(new String(rowBytes, "UTF-8"));
        Map<String, Object> expectedMap =
                parseJson("{\"schema\":\"" + schemaStr + "\"" +
                          ",\"database\": {\"string\":\"testdb\"}" +
                          ",\"table\": {\"string\":\"test_t\"}" +
                          ",\"type\": {\"string\":\"insert\"}" +
                          ",\"ts\": {\"long\":0}" +
                          ",\"xid\": null" +
                          ",\"commit\": null" +
                          ",\"data\": {\"test.columns\": {\"id\": {\"long\":2}}}" +
                          ",\"old\": {\"test.columns\": {\"id\": {\"long\":1}}}}");


        assertThat(outputMap, is(expectedMap));
    }
}
