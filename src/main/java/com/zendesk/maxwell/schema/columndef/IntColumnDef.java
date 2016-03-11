package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;


public class IntColumnDef extends ColumnDef {
	private final int bits;
	private final Schema.Field fieldSchema;

	public IntColumnDef(String tableName, String name, String type, int pos, boolean signed) {
		super(tableName, name, type, pos);
		this.signed = signed;
		this.bits = bitsFromType(type);

		Schema union = SchemaBuilder.unionOf().nullType().and().longType().endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}


	private long castUnsigned(Integer i, long max_value) {
		if ( i < 0 )
			return max_value + i;
		else
			return i;
	}

	private Long toLong(Object value) {

		if ( value instanceof Long ) {
			return ( Long ) value;
		}

		if ( value instanceof Boolean ) {
			return ( Boolean ) value ? 1l: 0l;
		}

		Integer i = (Integer) value;

		if (signed)
			return Long.valueOf(i);

		long res = castUnsigned(i, 1L << this.bits);
		return Long.valueOf(res);

	}
	@Override
	public String toSQL(Object value) {
		return toLong(value).toString();
	}

	@Override
	public Object jsonValue(Object value) {
		return toLong(value);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		switch(this.bits) {
		case 8:
			return type == MySQLConstants.TYPE_TINY;
		case 16:
			return type == MySQLConstants.TYPE_SHORT;
		case 24:
			return type == MySQLConstants.TYPE_INT24;
		case 32:
			return type == MySQLConstants.TYPE_LONG;
		default:
			return false;
		}
	}

	private final static int bitsFromType(String type) {
		switch(type) {
		case "tinyint":
			return 8;
		case "smallint":
			return 16;
		case "mediumint":
			return 24;
		case "int":
			return 32;
		default:
			return 0;
		}
	}


	@Override
	public boolean getSigned() {
		return signed;
	}

	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	@Override
	public Object avroValue(Object value) {
		return toLong(value);
	}
}
