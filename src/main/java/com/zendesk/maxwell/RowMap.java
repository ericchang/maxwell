package com.zendesk.maxwell;

import static org.apache.avro.Schema.Field;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class RowMap implements Serializable {
	static final Logger LOGGER = LoggerFactory.getLogger(RowMap.class);
	private static final String dataFieldName = "data";
	private static final String oldDataFieldName = "old";

	private final String rowType;
	private final String database;
	private final String table;
	private final Long timestamp;
	private final BinlogPosition nextPosition;

	private Long xid;
	private boolean txCommit;

	private final HashMap<String, Object> data;
	private final HashMap<String, Object> oldData;
	private final HashMap<String, ColumnDef> columnDefs;
	private final List<String> pkColumns;

	private static final JsonFactory jsonFactory = new JsonFactory();

	private static final ThreadLocal<ByteArrayOutputStream> byteArrayThreadLocal =
			new ThreadLocal<ByteArrayOutputStream>(){
				@Override
				protected ByteArrayOutputStream initialValue() {
					return new ByteArrayOutputStream();
				}
			};

	private static final ThreadLocal<JsonGenerator> jsonGeneratorThreadLocal =
			new ThreadLocal<JsonGenerator>() {
				@Override
				protected JsonGenerator initialValue() {
					JsonGenerator g = null;
					try {
						g = jsonFactory.createGenerator(byteArrayThreadLocal.get());
					} catch (IOException e) {
						LOGGER.error("error initializing jsonGenerator", e);
						return null;
					}
					g.setRootValueSeparator(null);
					return g;
				}
			};

	private static Comparator<ColumnDef> columnDefComparator = new Comparator<ColumnDef>() {
		@Override
		public int compare(ColumnDef c1, ColumnDef c2) {
			int compare = Integer.compare(c1.getPos(), c2.getPos());
			return compare == 0 ? c1.getName().compareTo(c2.getName()) : compare;
		}
	};

	public RowMap(String type, String database, String table, Long timestamp, List<String> pkColumns, BinlogPosition nextPosition) {
		this.rowType = type;
		this.database = database;
		this.table = table;
		this.timestamp = timestamp;
		this.data = new HashMap<>();
		this.oldData = new HashMap<>();
		this.columnDefs = new HashMap<>();
		this.nextPosition = nextPosition;
		this.pkColumns = pkColumns;
	}

	public String pkToJson() throws IOException {
		JsonGenerator g = jsonGeneratorThreadLocal.get();

		g.writeStartObject(); // start of row {

		g.writeStringField("database", database);
		g.writeStringField("table", table);

		if (pkColumns.isEmpty()) {
			g.writeStringField("_uuid", UUID.randomUUID().toString());
		} else {
			for (String pk : pkColumns) {
				Object pkValue = null;
				if ( data.containsKey(pk) )
					pkValue = getJsonData(pk, data);

				g.writeObjectField("pk." + pk, pkValue);
			}
		}

		g.writeEndObject(); // end of 'data: { }'
		g.flush();
		return jsonFromStream();
	}

	public String pkAsConcatString() {
		if (pkColumns.isEmpty()) {
			return database + table;
		}
		String keys="";
		for (String pk : pkColumns) {
			Object pkValue = null;
			if (data.containsKey(pk))
				pkValue = getJsonData(pk, data);
			if (pkValue != null)
				keys += pkValue.toString();
		}
		if (keys.isEmpty())
			return "None";
		return keys;
	}

	private void writeMapToJSON(String jsonMapName, Map<String, Object> columnVals, boolean includeNullField) throws IOException {
		JsonGenerator generator = jsonGeneratorThreadLocal.get();
		generator.writeObjectFieldStart(jsonMapName); // start of jsonMapName: {

		/* TODO: maintain ordering of fields in column order */
		for ( String key: columnVals.keySet() ) {
			Object value = getJsonData(key, columnVals);

			if ( value == null && !includeNullField)
				continue;

			if ( value instanceof List) { // sets come back from .asJSON as lists, and jackson can't deal with lists natively.
				List stringList = (List) value;

				generator.writeArrayFieldStart(key);
				for ( Object s : stringList )  {
					generator.writeObject(s);
				}
				generator.writeEndArray();
			} else {
				generator.writeObjectField(key, value);
			}
		}

		generator.writeEndObject(); // end of 'jsonMapName: { }'
		return;
	}

	public String toJSON() throws IOException {
		JsonGenerator g = jsonGeneratorThreadLocal.get();

		g.writeStartObject(); // start of row {

		g.writeStringField("database", this.database);
		g.writeStringField("table", this.table);
		g.writeStringField("type", this.rowType);
		g.writeNumberField("ts", this.timestamp);

		/* TODO: allow xid and commit to be configurable in the output */
		if ( this.xid != null )
			g.writeNumberField("xid", this.xid);

		if ( this.txCommit )
			g.writeBooleanField("commit", true);

		writeMapToJSON(dataFieldName, this.data, false);

		if ( !this.oldData.isEmpty() ) {
			writeMapToJSON(oldDataFieldName, this.oldData, true);
		}

		g.writeEndObject(); // end of row
		g.flush();

		return jsonFromStream();
	}

	/**
	 * Builds an Avro {@link Schema} from the {@link ColumnDef}s that have been added to this
	 * RowMap by the {@link #putData(String, ColumnDef, Object)} and
	 * {@link #putOldData(String, ColumnDef, Object)} methods.   This includes envelope fields
	 * generated by Maxwell:
	 * <ol>
	 *     <li>database</li>
	 *     <li>table</li>
	 *     <li>type (insert, update, delete)</li>
	 *     <li>ts (timestamp)</li>
	 *     <li>xid</li>
	 *     <li>commit</li>
	 * </ol>
	 *
	 * Each Avro {@link Field} is modeled as nullable, with a default value of null.
	 * This supports forwards compatibility with future schemas that make non-nullable fields
	 * nullable.
	 *
	 * @param namespace Avro namespace to assign schema
	 * @return the Avro Schema
	 */
	public Schema generateAvroSchema(String namespace, String doc) {
		LOGGER.info(String.format("Generating schema for table %s.%s", database, table));

		if ( doc == null ) {
			doc = String.format("Maxwell-generated schema for table %s.%s using namespace %s",
								database, table, namespace);
		}

		SchemaBuilder.FieldAssembler<Schema> fields =
				SchemaBuilder.record(table).namespace(namespace).doc(doc).fields()
						.name("schema").type().stringType().noDefault()
						.name("database").type().optional().stringType()
						.name("table").type().optional().stringType()
						.name("type").type().optional().stringType()
						.name("ts").type().optional().longType()
						.name("xid").type().optional().longType()
						.name("commit").type().optional().booleanType();

		Schema columnsSchema = generateAvroColumnsSchema(namespace);
		fields.name("data").type().optional().type(columnsSchema);
		fields.name("old").type().optional().type(columnsSchema);

		return fields.endRecord();
	}

	/**
	 * Sorts {@link ColumnDef}s in {@link #columnDefs} by {@link ColumnDef#pos} for consistent
	 * field declaration order in resulting schema
	 */
	private Schema generateAvroColumnsSchema(String namespace) {
		List<Field> columnFields = new ArrayList<>();
		List<ColumnDef> columns = getColumns();
		Collections.sort(columns, columnDefComparator);
		for ( ColumnDef columnDef : columns ) {
			Field columnField = columnDef.getAvroField();
			columnFields.add(columnField);
		}
		return Schema.createRecord("columns", null, namespace, false, columnFields);
	}

	/**
	 * Builds a {@link GenericRecord} that conforms to the specified {@link Schema}.  The
	 * specified Schema is assumed to be the value returned from
	 * {@link #generateAvroSchema(String, String)} (String)}. Specifically, it must have nullable
	 * {@link Field}s named {@link #dataFieldName} and {@link #oldDataFieldName} whose schemas
	 * will be used to create embedded GenericRecords from the {@link #data} and {@link #oldData},
	 * respectively.
	 *
	 * The resulting `old` field will be the merge of {@link #data} and {@link #oldData} maps.
	 * Avro's json representation of null values requires null elements to be rendered.
	 * This makes it impossible to distinguish in the json whether the {@link #oldData} value was
	 * truly null or just unspecified.
	 */
	private GenericRecord buildGenericRecord(Schema schema) {
		GenericRecordBuilder builder = new GenericRecordBuilder(schema);
		builder.set("schema", schema.toString());
		builder.set("database", this.database);
		builder.set("table", this.table);
		builder.set("type", this.rowType);
		builder.set("ts", this.timestamp);

		/* TODO: allow xid and commit to be configurable in the output */
		if ( this.xid != null ) {
			builder.set("xid", this.xid);
		}

		if ( this.txCommit ) {
			builder.set("commit", true);
		}

		Field dataField = schema.getField(dataFieldName);
		assert dataField != null;
		builder.set(dataFieldName, buildRowRecord(dataField, this.data));

		// Make a merged map from data + oldData which represents the previous state of the row
		if ( !this.oldData.isEmpty() ) {
			HashMap<String, Object> merged = new HashMap<>(this.data);
			merged.putAll(this.oldData);

			Field oldDataField = schema.getField(oldDataFieldName);
			assert oldDataField != null;

			GenericRecord rowRecord = buildRowRecord(oldDataField, merged);
			builder.set(oldDataFieldName, rowRecord);
		}

		return builder.build();
	}

	private GenericRecord buildRowRecord(Field rowField, Map<String, Object> rowData) {
		List<Schema> rowFieldTypes = rowField.schema().getTypes();
		assert rowFieldTypes.size() >= 2;
		assert rowFieldTypes.get(1).getType() == Schema.Type.RECORD;
		Schema columnsSchema = rowFieldTypes.get(1);
		rowField.schema().getTypes().get(1);
		GenericRecordBuilder builder = new GenericRecordBuilder(columnsSchema);

		for ( Field field : columnsSchema.getFields() ) {
            String name = field.name();
            Object value = rowData.get(name);
            ColumnDef columnDef = columnDefs.get(name);
            if ( value != null ) {
				builder.set(name, columnDef.asAvro(value));
            }
        }

		return builder.build();
	}

	public String toAvroJSON(Schema schema) throws IOException {
		GenericRecord record = buildGenericRecord(schema);

		ByteArrayOutputStream avroJsonOut = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> datumWriter = new ReflectDatumWriter<>(schema);

		Encoder encoder = EncoderFactory.get().jsonEncoder(schema, avroJsonOut);
		datumWriter.write(record, encoder);
		encoder.flush();

		return avroJsonOut.toString();
	}

	private String jsonFromStream() {
		ByteArrayOutputStream b = byteArrayThreadLocal.get();
		String s = b.toString();
		b.reset();
		return s;
	}

	private Object getJsonData(String key, Map<String,Object> columnVals) {
		Object colValue = columnVals.get(key);
		if ( colValue == null ) {
			return null;
		} else {
			ColumnDef columnDef = columnDefs.get(key);
			return columnDef.asJSON(colValue);
		}
	}

	public Object getData(String key) {
		return getJsonData(key, data);
	}

	public boolean containsValue(String key, Object value) {
		return Objects.deepEquals(data.get(key), value);
	}

	public void putData(String key, ColumnDef columnDef, Object value) {
		columnDefs.put(key, columnDef);
		data.put(key, value);
	}

	public void putOldData(String key, ColumnDef columnDef, Object value) {
		columnDefs.put(key, columnDef);
		oldData.put(key, value);
	}

	public BinlogPosition getPosition() {
		return nextPosition;
	}

	public Long getXid() {
		return xid;
	}

	public void setXid(Long xid) {
		this.xid = xid;
	}

	public void setTXCommit() {
		this.txCommit = true;
	}

	public boolean isTXCommit() {
		return this.txCommit;
	}

	public String getDatabase() {
		return database;
	}

	public String getTable() {
		return table;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public boolean hasData(String name) {
		return this.data.containsKey(name);
	}

	/**
	 * @return the current list of {@link ColumnDef} as populated by calls to
	 * {@link #putData(String, ColumnDef, Object)} and
	 * {@link #putOldData(String, ColumnDef, Object)}.
     */
	public List<ColumnDef> getColumns() {
		List<ColumnDef> columns = new ArrayList<>(columnDefs.values());
		return columns;
	}
}
