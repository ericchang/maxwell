package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang.ArrayUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BitColumnDef extends ColumnDef {
	private final Schema.Field fieldSchema;

	public BitColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);

		List<Schema> unionSchemas = new ArrayList<>();
		unionSchemas.add(Schema.create(Schema.Type.NULL));
		unionSchemas.add(Schema.create(Schema.Type.BYTES));

		Schema union = SchemaBuilder.unionOf().nullType().and().bytesType().endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_BIT;
	}

	@Override
	public Object jsonValue(Object value) {
		byte[] bytes = (byte[]) value;
		if ( bytes.length == 8 && ((bytes[7] & 0xFF) > 127) ) {
			return bytesToBigInteger(bytes);
		} else {
			return bytesToLong(bytes);
		}
	}

	private BigInteger bytesToBigInteger(byte[] bytes) {
		BigInteger res = BigInteger.ZERO;

		for (int i = 0; i < bytes.length; i++) {
			res = res.add(BigInteger.valueOf(bytes[i] & 0xFF).shiftLeft(i * 8));
		}

		return res;
	}

	private Long bytesToLong(byte[] bytes) {
		long res = 0;

		for (int i = 0; i < bytes.length; i++)
			res += ((bytes[i] & 0xFF) << ( i * 8 ));

		return res;
	}

	@Override
	public String toSQL(Object value) {
		return asJSON(value).toString();
	}

	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	@Override
	public Object avroValue(Object value) {
		if (value instanceof byte[]) {
			byte[] bigEndianBytes = ArrayUtils.clone((byte[])value);
			ArrayUtils.reverse(bigEndianBytes);
			return ByteBuffer.wrap(bigEndianBytes);
		} else {
			throw new RuntimeException(String.format("Cannot convert value to Avro %s", value));
		}
	}
}
