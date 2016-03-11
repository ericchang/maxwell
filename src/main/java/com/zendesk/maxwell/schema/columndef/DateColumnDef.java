package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import org.apache.avro.*;
import org.apache.avro.data.TimeConversions;
import org.joda.time.LocalDate;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DateColumnDef extends ColumnDef {
	private final Schema dateSchema;
	private final LogicalType dateType;
	private final Schema.Field fieldSchema;
	private final Conversion<LocalDate> dateConversion = new TimeConversions.DateConversion();

	public DateColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);

		dateSchema = Schema.create(Schema.Type.INT);
		dateType = LogicalTypes.date();
		dateType.addToSchema(dateSchema);

		Schema union = SchemaBuilder.unionOf().nullType().and().type(dateSchema).endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	private static SimpleDateFormat dateFormatter;

	private static SimpleDateFormat getDateFormatter() {
		if ( dateFormatter == null ) {
			dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
		}
		return dateFormatter;
	}


	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_DATE;
	}

	private String formatDate(Object value) {
		/* protect against multithreaded access of static dateFormatter */
		synchronized ( DateColumnDef.class ) {
			return getDateFormatter().format((Date) value);
		}
	}

	@Override
	public String toSQL(Object value) {
		return "'" + formatDate(value) + "'";
	}

	@Override
	public Object jsonValue(Object value) {
		return formatDate(value);
	}


	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	@Override
	public Object avroValue(Object value) {
		if ( value instanceof Date ) {
			LocalDate localDate = new LocalDate(((Date) value).getTime());
			return dateConversion.toInt(localDate, dateSchema, dateType);
		} else {
			throw new RuntimeException(String.format("Cannot convert value to Avro %s", value));
		}
	}
}
