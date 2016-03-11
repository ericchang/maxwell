package com.zendesk.maxwell.schema.columndef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.StringUtils;

import com.google.code.or.common.util.MySQLConstants;

public class SetColumnDef extends ColumnDef {
	private final Schema arraySchema;
	private final Schema.Field fieldSchema;

	public SetColumnDef(String tableName, String name, String type, int pos, String[] enumValues) {
		super(tableName, name, type, pos);
		this.enumValues = enumValues;

		arraySchema = SchemaBuilder.array().items().stringType();

		Schema union = SchemaBuilder.unionOf().nullType().and().type(arraySchema).endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_SET;
	}

	@Override
	public String toSQL(Object value) {
		return "'" + StringUtils.join(asList(value), "'") + "'";
	}

	@Override
	public Object jsonValue(Object value) {
		return asList(value);
	}

	private ArrayList<String> asList(Object value) {
		if ( value instanceof String ) {
			return new ArrayList<>(Arrays.asList((( String ) value).split(",")));
		}
		ArrayList<String> values = new ArrayList<>();
		long v = (Long) value;
		for(int i = 0; i < enumValues.length; i++) {
			if ( ((v >> i) & 1) == 1 ) {
				values.add(enumValues[i]);
			}
		}
		return values;
	}

	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	@Override
	public Object avroValue(Object value) {
		return new GenericData.Array<>(arraySchema, asList(value));
	}
}
