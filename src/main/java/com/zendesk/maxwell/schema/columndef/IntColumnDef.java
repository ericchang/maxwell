package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;


public class IntColumnDef extends ColumnDef {
	public int bits;

	protected boolean signed;

	public IntColumnDef(String name, String type, int pos, boolean signed) {
		super(name, type, pos);
		this.signed = signed;
		this.bits = bitsFromType(type);
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
	public Object asJSON(Object value) {
		return toLong(value);
	}

	@Override
	public ColumnDef copy() {
		return new IntColumnDef(name, type, pos, signed);
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

	public boolean isSigned() {
		return signed;
	}

}
