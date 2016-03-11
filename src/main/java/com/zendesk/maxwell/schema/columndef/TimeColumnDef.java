package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import org.apache.avro.*;
import org.apache.avro.data.TimeConversions;
import org.joda.time.LocalTime;

import java.sql.Time;

public class TimeColumnDef extends ColumnDef {
	private final Schema timeSchema;
	private final LogicalType timeType;
	private final Schema.Field fieldSchema;
	private final Conversion<LocalTime> timeConversion = new TimeConversions.TimeConversion();

	public TimeColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);

		timeSchema = Schema.create(Schema.Type.INT);
		timeType = LogicalTypes.timeMillis();
		timeType.addToSchema(timeSchema);

		Schema union = SchemaBuilder.unionOf().nullType().and().type(timeSchema).endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_TIME;
	}

	@Override
	public String toSQL(Object value) {
		Time t = (Time) value;
		return "'" + String.valueOf(t) + "'";
	}

	@Override
	public Object jsonValue(Object value) {
		return String.valueOf((Time) value);
	}

	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	@Override
	public Object avroValue(Object value) {
		if ( value instanceof Time ) {
			LocalTime localTime = new LocalTime(((Time) value).getTime());
			return timeConversion.toInt(localTime, timeSchema, timeType);
		} else {
			throw new RuntimeException(String.format("Cannot convert value to Avro %s", value));
		}
	}
}
