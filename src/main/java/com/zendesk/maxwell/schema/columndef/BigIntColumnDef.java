package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import org.apache.avro.*;

import java.math.BigDecimal;
import java.math.BigInteger;

public class BigIntColumnDef extends ColumnDef {
    private final int bigint_precision = 20;
	private final BigInteger longlong_max = BigInteger.ONE.shiftLeft(64);

    private final Schema decimalSchema;
    private final LogicalType decimalType;
    private final Schema.Field fieldSchema;
    private final Conversion<BigDecimal> decimalConversion = new Conversions.DecimalConversion();

	public BigIntColumnDef(String tableName, String name, String type, int pos, boolean signed) {
		super(tableName, name, type, pos);
        this.signed = signed;

        decimalSchema = Schema.create(Schema.Type.BYTES);
        decimalType = LogicalTypes.decimal(bigint_precision,0);
        decimalType.addToSchema(decimalSchema);

        Schema union = SchemaBuilder.unionOf().nullType().and().type(decimalSchema).endUnion();
        fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_LONGLONG;
	}

	private Object toNumeric(Object value) {
        if ( value instanceof BigInteger ) {
          return value;
        }
        Long l = (Long)value;
        if ( l < 0 && !signed )
        	return longlong_max.add(BigInteger.valueOf(l));
        else
            return Long.valueOf(l);
	}
	@Override
	public String toSQL(Object value) {
		return toNumeric(value).toString();
	}

	@Override
	public Object jsonValue(Object value) {
		return toNumeric(value);
	}

    /**
     * Returns a decimal {@link LogicalType} with a precision of 20 and a scale of 0 to support
     * an 8 byte unsigned long which exceeds the value that can be stored in Java (signed) long
     */
	@Override
	public Schema.Field buildAvroField() {
        return fieldSchema;
	}

    @Override
    public Object avroValue(Object value) {
        Object numeric = toNumeric(value);
        if ( numeric instanceof BigInteger ) {
            return decimalConversion.toBytes(new BigDecimal((BigInteger) numeric),
                                             decimalSchema, decimalType);
        } else if ( numeric instanceof Long ) {
            return decimalConversion.toBytes(BigDecimal.valueOf((Long) numeric),
                                             decimalSchema, decimalType);
        } else {
            throw new RuntimeException(String.format("Cannot convert value to Avro %s", value));
        }
    }
}
