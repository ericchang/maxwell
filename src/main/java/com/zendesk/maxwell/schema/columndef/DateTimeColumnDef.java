package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.google.code.or.common.util.MySQLConstants;
import org.apache.avro.*;
import org.apache.avro.data.TimeConversions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTimeColumnDef extends ColumnDef {
	private final Schema dateTimeSchema;
	private final LogicalType dateTimeType;
	private final Schema.Field fieldSchema;
	private final Conversion<DateTime> dateConversion = new TimeConversions.TimestampConversion();

	public DateTimeColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);

		dateTimeSchema = Schema.create(Schema.Type.LONG);
		dateTimeType = LogicalTypes.timestampMillis();
		dateTimeType.addToSchema(dateTimeSchema);

		Schema union = SchemaBuilder.unionOf().nullType().and().type(dateTimeSchema).endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	private static SimpleDateFormat dateTimeFormatter;

	private static SimpleDateFormat getDateTimeFormatter() {
		if ( dateTimeFormatter == null ) {
			dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
		return dateTimeFormatter;
	}


	@Override
	public boolean matchesMysqlType(int type) {
		if ( getType().equals("datetime") ) {
			return type == MySQLConstants.TYPE_DATETIME ||
				   type == MySQLConstants.TYPE_DATETIME2;
		} else {
			return type == MySQLConstants.TYPE_TIMESTAMP ||
				   type == MySQLConstants.TYPE_TIMESTAMP2;
		}
	}

	private String formatValue(Object value) {
		/* protect against multithreaded access of static dateTimeFormatter */
		synchronized ( DateTimeColumnDef.class ) {
			if ( value instanceof Long && getType().equals("datetime") )
				return formatLong(( Long ) value);
			else if ( value instanceof Timestamp )
				return getDateTimeFormatter().format(( Timestamp ) value);
			else if ( value instanceof Date )
				return getDateTimeFormatter().format(( Date ) value);
			else
				return "";
		}
	}

	private String formatLong(Long value) {
		final int second = (int)(value % 100); value /= 100;
		final int minute = (int)(value % 100); value /= 100;
		final int hour = (int)(value % 100); value /= 100;
		final int day = (int)(value % 100); value /= 100;
		final int month = (int)(value % 100);
		final int year = (int)(value / 100);

		return String.format("%04d-%02d-%02d %02d:%02d:%02d",  year, month, day, hour, minute, second);
	}

    /**
     * {@link com.google.code.or.common.util.MySQLUtils#toDatetime(long)} doesn't properly floor
	 * month and day values.  See {@link DatetimeColumn} constructor for usage.
     */
    private DateTime extractDateTime(long value) {
        final int second = (int) (value % 100);
        value /= 100;
        final int minute = (int) (value % 100);
        value /= 100;
        final int hour = (int) (value % 100);
        value /= 100;
        final int day = Math.max((int) (value % 100), 1);
        value /= 100;
        final int month = Math.max((int) (value % 100), 1);
        final int year = (int) (value / 100);

        return new DateTime(year, month, day, hour, minute, second, 0, DateTimeZone.UTC);
    }

	@Override
	public String toSQL(Object value) {
		return "'" + formatValue(value) + "'";
	}


	@Override
	public Object jsonValue(Object value) {
		return formatValue(value);
	}

	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	/**
	 * MySQL does not support time zones, so assume UTC (rather than JVM's default time zone)
	 * when parsing dates.
	 */
	@Override
	public Object avroValue(Object value) {
		DateTime dateTime = null;
		if ( value instanceof Long && getType().equals("datetime") ) {
            dateTime = extractDateTime((Long) value);
		} else if ( value instanceof Timestamp ) {
			dateTime = new DateTime(((Timestamp) value).getTime()).withZoneRetainFields(DateTimeZone.UTC);
		} else if ( value instanceof Date ) {
			dateTime = new DateTime(((Date) value).getTime()).withZoneRetainFields(DateTimeZone.UTC);
		} else {
			return null;
		}

		return dateConversion.toLong(dateTime, dateTimeSchema, dateTimeType);
	}
}
