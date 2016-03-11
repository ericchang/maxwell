package com.zendesk.maxwell;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.google.common.base.Charsets;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.io.JsonEncoder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MaxwellAvroIntegrationTest extends AbstractIntegrationTest {
    private static final String SCHEMA_NAME = "schema";
    private static final String DATA_NAME = "data";
    private static final String OLD_DATA_NAME = "old";
    private static final String COLUMNS_NAME = "columns";
    private static final String NAMESPACE = "maxwell.test";
    private static final String DOC = "testdoc";
    // because JsonDecoder.CHARSET (used in JsonEncoder.writeByteArray) is not publicly accessible
    private static final String BYTES_ENCODING = "ISO-8859-1";

    /** Create a new field builder that is seeded with common maxwell fields */
    private SchemaBuilder.FieldAssembler<Schema> maxwellFields(String tableName) {
        return SchemaBuilder.record(tableName).namespace(NAMESPACE).doc(DOC).fields()
                .name(SCHEMA_NAME).type().stringType().noDefault()
                .name("database").type().optional().stringType()
                .name("table").type().optional().stringType()
                .name("type").type().optional().stringType()
                .name("ts").type().optional().longType()
                .name("xid").type().optional().longType()
                .name("commit").type().optional().booleanType();
    }

    /** Create a new field builder to which table columns can be added */
    private SchemaBuilder.FieldAssembler<Schema> columnFields() {
        return SchemaBuilder.record(COLUMNS_NAME).namespace(NAMESPACE).fields();
    }

    private Schema timestampSchema() {
        Schema timestampSchema = Schema.create(Schema.Type.LONG);
        LogicalTypes.timestampMillis().addToSchema(timestampSchema);
        return timestampSchema;
    }

    private Schema timeSchema() {
        Schema timeSchema = Schema.create(Schema.Type.INT);
        LogicalTypes.timeMillis().addToSchema(timeSchema);
        return timeSchema;
    }

    private Schema decimalSchema(int precision, int scale) {
        Schema decimalSchema = Schema.create(Schema.Type.BYTES);
        LogicalTypes.decimal(precision, scale).addToSchema(decimalSchema);
        return decimalSchema;
    }

    private String schemaToString(Schema schema) {
        return schema.toString().replaceAll("\"", "\\\\\"");
    }

    /** Generates a Schema for the table name, assigning the maxwell fields, 'data', 'old' fields */
    private Schema rowSchema(String tableName, Schema columnsSchema) {
        return maxwellFields(tableName)
                .name(DATA_NAME).type().optional().type(columnsSchema)
                .name(OLD_DATA_NAME).type().optional().type(columnsSchema)
                .endRecord();
    }

    /** Use Avro-generated json */
    @Override
    protected String rowToJson(RowMap rowMap) throws Exception {
        // create a record whose name matches the table in the RowMap
        // and with the given namespace and doc
        Schema rowSchema = rowMap.generateAvroSchema(NAMESPACE, DOC);
        return rowMap.toAvroJSON(rowSchema);
    }

    @Override
    /** Convert the "schema" element to an Avro {@link Schema} */
    protected Map<String, Object> sanitizeMap(Map<String, Object> jsonMap) throws Exception {
        Map<String, Object> sanitized = super.sanitizeMap(jsonMap);
        assertThat("schema is null", sanitized.get(SCHEMA_NAME), notNullValue());
        assertThat("schema not string", sanitized.get(SCHEMA_NAME), instanceOf(String.class));
        String schemaJson = (String) sanitized.get(SCHEMA_NAME);
        assertThat("schema is empty", schemaJson.trim().length(), not(0));
        sanitized.put(SCHEMA_NAME, new Schema.Parser().parse(schemaJson));
        return sanitized;
    }

    @Test
    public void testRunMinimalBinlog() throws Exception {
        if ( server.getVersion().equals("5.5") )
            return;

        try {
            server.getConnection().createStatement().execute("set global binlog_row_image='minimal'");
            server.resetConnection(); // only new connections pick up the binlog setting

            Map<String, Object> scopes = new HashMap<>();

            // test_table(id, a) columns, since minimal binlog is being used
            Schema testTableColsIdA = columnFields()
                    .name("id").type().optional().longType()
                    .name("a").type().optional().stringType()
                    .endRecord();
            Schema testTableColsIdASchema = rowSchema("test_table", testTableColsIdA);
            // set the mustache variable schema_test_table_cols_a
            scopes.put("schema_test_table_cols_id_a", schemaToString(testTableColsIdASchema));

            // test_table(id, a, b) columns
            Schema testTableColsIdAB = columnFields()
                    .name("id").type().optional().longType()
                    .name("a").type().optional().stringType()
                    .name("b").type().optional().stringType()
                    .endRecord();
            Schema testTableColsIdABSchema = rowSchema("test_table", testTableColsIdAB);
            scopes.put("schema_test_table_cols_id_a_b", schemaToString(testTableColsIdABSchema));

            // test_table_2(id, i1, i2) columns
            Schema testTable2ColsIdI1I2 = columnFields()
                    .name("id").type().optional().longType()
                    .name("i1").type().optional().longType()
                    .name("i2").type().optional().longType()
                    .endRecord();
            Schema testTable2ColsIdI1I2Schema = rowSchema("test_table_2", testTable2ColsIdI1I2);
            scopes.put("schema_test_table_2_cols_id_i1_i2",
                       schemaToString(testTable2ColsIdI1I2Schema));

            // test_table_2(id, i3) columns
            Schema testTable2ColsIdI3 = columnFields()
                    .name("id").type().optional().longType()
                    .name("i3").type().optional().longType()
                    .endRecord();
            Schema testTable2ColsIdI3Schema = rowSchema("test_table_2", testTable2ColsIdI3);
            scopes.put("schema_test_table_2_cols_id_i3",
                       schemaToString(testTable2ColsIdI3Schema));

            runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_minimal", scopes);
        } finally {
            server.getConnection().createStatement().execute("set global binlog_row_image='full'");
            server.resetConnection();
        }
    }

    @Test
    public void testRunMainJSONTest() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        // (id, textcol) columns
        Schema idAndTextCols = columnFields()
                .name("id").type().optional().type(decimalSchema(20, 0))
                .name("textcol").type().optional().stringType()
                .endRecord();
        Schema rowWithTextColumns = rowSchema("test_table", idAndTextCols);
        // set the mustache variable schema_with_text_col
        scopes.put("schema_with_text_col", schemaToString(rowWithTextColumns));

        // (id, datecol, textcol) columns
        Schema idTextDateCols = columnFields()
                .name("id").type().optional().type(decimalSchema(20, 0))
                .name("datecol").type().optional().type(timestampSchema())
                .name("textcol").type().optional().stringType()
                .endRecord();
        Schema rowWithTextAndDateColumns = rowSchema("test_table", idTextDateCols);
        scopes.put("schema_with_text_and_date_cols", schemaToString(rowWithTextAndDateColumns));


        // (id, dummycol, datecol, textcol) columns
        Schema idTextDateDummyCols = columnFields()
                .name("id").type().optional().type(decimalSchema(20, 0))
                .name("dummycol").type().optional().longType()
                .name("datecol").type().optional().type(timestampSchema())
                .name("textcol").type().optional().stringType()
                .endRecord();
        Schema rowWithTextAndDateAndDummyColumns = rowSchema("test_table", idTextDateDummyCols);
        scopes.put("schema_with_text_and_date_and_dummy_cols",
                   schemaToString(rowWithTextAndDateAndDummyColumns));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_1j", scopes);
    }

    @Test
    public void testCreateLikeJSON() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        Schema columns = columnFields()
                .name("id").type().optional().longType()
                .name("flt").type().optional().doubleType()
                .endRecord();
        Schema columnsSchema = rowSchema("test_alike2", columns);
        scopes.put("schema_test_alike2", schemaToString(columnsSchema));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_create_like", scopes);
    }

    @Test
    public void testCreateSelectJSON() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        class AlikeBuilder {
            public Schema build(String tableName) {
                Schema alikeColumns = columnFields()
                        .name("id").type().optional().longType()
                        .name("flt").type().optional().doubleType()
                        .endRecord();
                return rowSchema(tableName, alikeColumns);
            }
        }

        scopes.put("schema_test_alike", schemaToString(new AlikeBuilder().build("test_alike")));
        scopes.put("schema_test_alike2", schemaToString(new AlikeBuilder().build("test_alike2")));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_create_select", scopes);
    }

    @Test
    public void testEnumJSON() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        Schema columns = columnFields()
                .name("id").type().optional().longType()
                .name("enum_field").type().optional().stringType()
                .endRecord();
        Schema columnsSchema = rowSchema("test_enums", columns);
        scopes.put("schema_test_enums", schemaToString(columnsSchema));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_enum", scopes);
    }

    @Test
    public void testLatin1JSON() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        Schema columns = columnFields()
                .name("textcol").type().optional().stringType()
                .endRecord();
        Schema columnsSchema = rowSchema("latin1", columns);
        scopes.put("schema_test_latin1", schemaToString(columnsSchema));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_latin1", scopes);
    }

    @Test
    public void testSetJSON() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        Schema columns = columnFields()
                .name("id").type().optional().longType()
                .name("set_field").type().optional().array().items().stringType()
                .endRecord();
        Schema columnsSchema = rowSchema("test_sets", columns);
        scopes.put("schema_test_sets", schemaToString(columnsSchema));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_set", scopes);
    }

    @Test
    public void testZeroCreatedAtJSON() throws Exception {
        if ( server.getVersion().equals("5.5") ) { // 5.6 not yet supported for this test
            Map<String, Object> scopes = new HashMap<>();

            Schema columns = columnFields()
                    .name("id").type().optional().longType()
                    .name("datecol").type().optional().type(timestampSchema())
                    .name("ch").type().optional().stringType()
                    .endRecord();
            Schema columnsSchema = rowSchema("ca", columns);
            scopes.put("schema_ca", schemaToString(columnsSchema));

            runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_zero_created_at", scopes);
        }
    }

    @Test
    public void testBlob() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        Schema blobColumns = columnFields()
                .name("id").type().optional().longType()
                .name("blob_field").type().optional().stringType()
                .endRecord();
        Schema blobColumnsSchema = rowSchema("test_blob", blobColumns);
        scopes.put("schema_test_blob", schemaToString(blobColumnsSchema));

        Schema binaryColumns = columnFields()
                .name("id").type().optional().longType()
                .name("binary_field").type().optional().stringType()
                .endRecord();
        Schema binaryColumnsSchema = rowSchema("test_binary", binaryColumns);
        scopes.put("schema_test_binary", schemaToString(binaryColumnsSchema));

        Schema varbinaryColumns = columnFields()
                .name("id").type().optional().longType()
                .name("varbinary_field").type().optional().stringType()
                .endRecord();
        Schema varbinaryColumnsSchema = rowSchema("test_varbinary", varbinaryColumns);
        scopes.put("schema_test_varbinary", schemaToString(varbinaryColumnsSchema));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_blob", scopes);
    }

    /** Replicates {@link JsonEncoder#writeByteArray} functionality */
    private String jsonEncodeBytes(byte[] bytes) throws Exception {
        byte[] utf8Quoted = new JsonStringEncoder().quoteAsUTF8(new String(bytes, BYTES_ENCODING));
        return new String(utf8Quoted, Charsets.UTF_8);
    }

    @Test
    public void testBit() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        class TestBitColumnsBuilder {
            Schema build() {
                return columnFields()
                        .name("id").type().optional().longType()
                        .name("bit_field").type().optional().bytesType()
                        .endRecord();
            }
        }

        Schema testBit = rowSchema("test_bit", new TestBitColumnsBuilder().build());
        scopes.put("schema_test_bit", schemaToString(testBit));

        Schema testBit2 = rowSchema("test_bit2", new TestBitColumnsBuilder().build());
        scopes.put("schema_test_bit2", schemaToString(testBit2));

        Schema testBit3 = rowSchema("test_bit3", new TestBitColumnsBuilder().build());
        scopes.put("schema_test_bit3", schemaToString(testBit3));

        Schema testBit4 = rowSchema("test_bit4", new TestBitColumnsBuilder().build());
        scopes.put("schema_test_bit4", schemaToString(testBit4));

        scopes.put("testbit_00011", jsonEncodeBytes(new byte[] { 0, 0, 0, 0, 0, 0, 0, 3 }));

        scopes.put("testbit_111111", jsonEncodeBytes(new byte[] { 0, 0, 0, 0, 0, 0, 0, 63 }));

        scopes.put("testbit_409832983", jsonEncodeBytes(new byte[] { 0, 0, 0, 0, 24, 109, -114, 23 }));

        scopes.put("testbit_18446744073709551615",
                   jsonEncodeBytes(new byte[] { -1, -1, -1, -1, -1, -1, -1, -1 }));

        scopes.put("testbit_5461", jsonEncodeBytes(new byte[] { 0, 0, 0, 0, 0, 0, 21, 85 }));

        scopes.put("testbit2_00011", jsonEncodeBytes(new byte[] { 3 }));

        scopes.put("testbit3_13929", jsonEncodeBytes(new byte[] { 54, 105 }));

        scopes.put("testbit4_111000111000111001010110", jsonEncodeBytes(new byte[] { -29, -114, 86 }));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_bit", scopes);
    }

    @Test
    public void testBignum() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        Schema bignumCols = columnFields()
                .name("id").type().optional().longType()
                .name("long_field").type().optional().type(decimalSchema(20, 0))
                .endRecord();
        Schema bignumColsSchema = rowSchema("test_long", bignumCols);
        scopes.put("schema_test_long_db", schemaToString(bignumColsSchema));

        scopes.put("long_field_bytes", jsonEncodeBytes(new byte[] { 0, -1, -1, -1, -1, -1, -1, -1, -1 }));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_bignum", scopes);
    }

    @Test
    public void testTime() throws Exception {
        if ( server.getVersion().equals("5.6") ) {
            Map<String, Object> scopes = new HashMap<>();

            Schema timeCols = columnFields()
                    .name("t").type().optional().type(timeSchema())
                    .name("dt").type().optional().type(timestampSchema())
                    .name("ts").type().optional().type(timestampSchema())
                    .endRecord();
            Schema timeColsSchema = rowSchema("tt", timeCols);
            scopes.put("schema_tt", schemaToString(timeColsSchema));

            runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_time", scopes);
        }
    }

    @Test
    public void testUCS2() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        Schema ucs2Cols = columnFields()
                .name("textcol").type().optional().stringType()
                .endRecord();
        Schema ucs2ColsSchema = rowSchema("ucs2", ucs2Cols);
        scopes.put("schema_ucs2", schemaToString(ucs2ColsSchema));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_ucs2", scopes);
    }

    @Test
    public void testGIS() throws Exception {
        Map<String, Object> scopes = new HashMap<>();

        Schema ucs2Cols = columnFields()
                .name("id").type().optional().longType()
                .name("g").type().optional().stringType()
                .endRecord();
        Schema ucs2ColsSchema = rowSchema("test_gis", ucs2Cols);
        scopes.put("schema_test_gis", schemaToString(ucs2ColsSchema));

        runJSONMustacheTemplateFile(getSQLDir() + "/avro/test_gis", scopes);
    }
}
