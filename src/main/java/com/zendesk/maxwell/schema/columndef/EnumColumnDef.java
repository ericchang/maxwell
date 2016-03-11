package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;

public class EnumColumnDef extends ColumnDef {
	private final Schema.Field fieldSchema;

	public EnumColumnDef(String tableName, String name, String type, int pos, String[] enumValues) {
		super(tableName, name, type, pos);
		this.enumValues = enumValues;

		Schema union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_ENUM;
	}

	@Override
	public String toSQL(Object value) {
		return "'" + asString(value) + "'";
	}

	@Override
	public String jsonValue(Object value) {
		return asString(value);
	}

	private String asString(Object value) {
		if ( value instanceof String ) {
			return ( String ) value;
		}
		Integer i = (Integer) value;

		if ( i == 0 )
			return null;
		else
			return enumValues[((Integer) value) - 1];
	}

	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	/**
	 * Keeping things simple and representing mysql enumerations as strings in Avro.  This
	 * skirts thorny issues like evolution when enum values are removed and the fact that mysql
	 * allows empty enum values but Avro does not.
     */
	@Override
	public Object avroValue(Object value) {
		return asString(value);
	}
}
