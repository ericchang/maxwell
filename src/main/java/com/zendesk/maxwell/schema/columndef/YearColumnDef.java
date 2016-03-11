package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class YearColumnDef extends ColumnDef {
	private final Schema.Field fieldSchema;

	public YearColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);

		Schema union = SchemaBuilder.unionOf().nullType().and().intType().endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_YEAR;
	}

	@Override
	public Object jsonValue(Object value) {
		if ( value instanceof Date ) {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(( java.sql.Date ) value);
			return calendar.get(Calendar.YEAR);
		}
		return value;
	}

	@Override
	public String toSQL(Object value) {
		return ((Integer)value).toString();
	}

	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	@Override
	public Object avroValue(Object value) {
		Object jsonValue = asJSON(value);

		if ( jsonValue instanceof Integer ) {
			return jsonValue;
		} else {
			throw new RuntimeException(String.format("Cannot convert value to Avro %s", value));
		}
	}
}
