package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;

public class FloatColumnDef extends ColumnDef {
	private final Schema.Field fieldSchema;

	public FloatColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);

		Schema union = SchemaBuilder.unionOf().nullType().and().doubleType().endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		if ( getType().equals("float") )
			return type == MySQLConstants.TYPE_FLOAT;
		else
			return type == MySQLConstants.TYPE_DOUBLE;
	}

	@Override
	public String toSQL(Object value) {
		return value.toString();
	}

	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	@Override
	public Object avroValue(Object value) {
		if ( getType().equals("float") ) {
			return Double.valueOf((Float) value);
		} else if ( value instanceof Double ){
			return value;
		} else {
			throw new RuntimeException(String.format("Cannot convert value to Avro %s", value));
		}
	}
}
