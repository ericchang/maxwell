package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import org.apache.avro.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class DecimalColumnDef extends ColumnDef {
	private static final Logger LOGGER = LoggerFactory.getLogger(DecimalColumnDef.class);
	private static final int default_precision = 65; // max precision for DECIMAL
	private static final int default_scale = 30; // max scale for DECIMAL

	private Schema decimalSchema;
	private LogicalType decimalType;
	private Schema.Field fieldSchema;
	private final Conversion<BigDecimal> decimalConversion = new Conversions.DecimalConversion();

	public DecimalColumnDef(String tableName, String name, String type, Integer precision, Integer scale, int pos) {
		super(tableName, name, type, pos);

		if ( scale != null ) {
			this.scale = scale;
		} else {
			if ( precision == null ) {
				LOGGER.warn(String.format("Defaulting to %d scale for %s.%s", default_scale, tableName, name));
				this.scale = default_scale;
			} else {
				LOGGER.info(String.format("Only precision specified for %s.%s, scale to 0", tableName, name));
				this.scale = 0;
			}
		}

		if ( precision != null ) {
			this.precision = Math.max(precision, 1);
		} else {
			LOGGER.warn(String.format("No precision specified for %s.%s, defaulting to %s", tableName, name, default_precision));
			this.precision = default_precision;
		}

		decimalSchema = Schema.create(Schema.Type.BYTES);
		decimalType = LogicalTypes.decimal(this.precision, this.scale);
		decimalType.addToSchema(decimalSchema);

		Schema union = SchemaBuilder.unionOf().nullType().and().type(decimalSchema).endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_DECIMAL;
	}

	@Override
	public String toSQL(Object value) {
		BigDecimal d = (BigDecimal) value;

		return d.toEngineeringString();
	}

	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	@Override
	public Object avroValue(Object value) {
		if ( value instanceof BigDecimal ) {
			BigDecimal bd = (BigDecimal) value;
			return decimalConversion.toBytes(bd, decimalSchema, decimalType);
		} else {
			throw new RuntimeException(String.format("Cannot convert value to Avro %s", value));
		}
	}

}
