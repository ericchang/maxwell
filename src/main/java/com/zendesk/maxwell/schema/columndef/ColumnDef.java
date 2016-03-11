package com.zendesk.maxwell.schema.columndef;

import org.apache.avro.Schema;

import java.util.regex.Pattern;

public abstract class ColumnDef {
	private static final Pattern avro_name_invalid = Pattern.compile("([^a-zA-Z0-9_]+)");
	private static final Pattern avro_name_valid = Pattern.compile("^[a-zA-Z_].*$");
	private static final String avro_replace_str = "_";

	protected final String tableName;
	protected final String name;
	protected final String type;
	protected String[] enumValues;
	protected Integer precision;
	protected Integer scale;
	private int pos;
	public boolean signed;
	public String encoding;

	public ColumnDef(String tableName, String name, String type, int pos) {
		this.tableName = tableName;
		this.name = name.toLowerCase();
		this.type = type;
		this.pos = pos;
		this.signed = false;
	}

	public abstract boolean matchesMysqlType(int type);
	public abstract String toSQL(Object value);

	public final Schema.Field getAvroField() {
		Schema.Field field = buildAvroField();
		return new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order());
	}

	/**
	 * @return an {@link org.apache.avro.Schema.Field} which will be copied and returned by
	 * {@link #getAvroField()}.  This allows a Field to be registered with multiple Schemas.
     */
	protected abstract Schema.Field buildAvroField();

	public final Object asAvro(Object value) {
		if ( value == null ) {
			return null;
		} else {
			return avroValue(value);
		}
	}

	protected abstract Object avroValue(Object value);

	public final Object asJSON(Object value) {
		if ( value == null ) {
			return null;
		} else {
			return jsonValue(value);
		}
	}

	protected Object jsonValue(Object value) {
		return value;
	}

	public ColumnDef copy() {
		return build(this.tableName, this.name, this.encoding, this.type, this.precision, this.scale, this.pos, this.signed, this.enumValues);
	}

	public static ColumnDef build(String tableName, String name, String encoding, String type, Integer precision, Integer scale, int pos, boolean signed, String enumValues[]) {
		switch(type) {
		case "tinyint":
		case "smallint":
		case "mediumint":
		case "int":
			return new IntColumnDef(tableName, name, type, pos, signed);
		case "bigint":
			return new BigIntColumnDef(tableName, name, type, pos, signed);
		case "tinytext":
		case "text":
		case "mediumtext":
		case "longtext":
		case "varchar":
		case "char":
			return new StringColumnDef(tableName, name, type, pos, encoding);
		case "tinyblob":
		case "blob":
		case "mediumblob":
		case "longblob":
		case "binary":
		case "varbinary":
			return new StringColumnDef(tableName, name, type, pos, "binary");
		case "geometry":
		case "geometrycollection":
		case "linestring":
		case "multilinestring":
		case "multipoint":
		case "multipolygon":
		case "polygon":
		case "point":
			return new GeometryColumnDef(tableName, name, type, pos);
		case "float":
		case "double":
			return new FloatColumnDef(tableName, name, type, pos);
		case "decimal":
			return new DecimalColumnDef(tableName, name, type, precision, scale, pos);
		case "date":
			return new DateColumnDef(tableName, name, type, pos);
		case "datetime":
		case "timestamp":
			return new DateTimeColumnDef(tableName, name, type, pos);
		case "year":
			return new YearColumnDef(tableName, name, type, pos);
		case "time":
			return new TimeColumnDef(tableName, name, type, pos);
		case "enum":
			return new EnumColumnDef(tableName, name, type, pos, enumValues);
		case "set":
			return new SetColumnDef(tableName, name, type, pos, enumValues);
		case "bit":
			return new BitColumnDef(tableName, name, type, pos);
		default:
			throw new IllegalArgumentException("unsupported column type " + type);
		}
	}

	static private String charToByteType(String type) {
		switch (type) {
			case "char":
			case "character":
				return "binary";
			case "varchar":
			case "varying":
				return "varbinary";
			case "tinytext":
				return "tinyblob";
			case "text":
				return "blob";
			case "mediumtext":
				return "mediumblob";
			case "longtext":
				return "longblob";
			case "long":
				return "mediumblob";
			default:
				throw new RuntimeException("Unknown type with BYTE flag: " + type);
		}
	}

	static public String unalias_type(String type, boolean longStringFlag, Long columnLength, boolean byteFlagToStringColumn) {
		if ( byteFlagToStringColumn )
			type = charToByteType(type);

		if ( longStringFlag ) {
			switch (type) {
				case "varchar":
					return "mediumtext";
				case "varbinary":
					return "mediumblob";
				case "binary":
					return "mediumtext";
			}
		}

		switch(type) {
			case "character":
			case "nchar":
				return "char";
			case "text":
			case "blob":
				if ( columnLength == null )
					return type;

				if ( columnLength < (1 << 8) )
					return "tiny" + type;
				else if ( columnLength < ( 1 << 16) )
					return type;
				else if ( columnLength < ( 1 << 24) )
					return "medium" + type;
				else
					return "long" + type;
			case "nvarchar":
			case "varying":
				return "varchar";
			case "bool":
			case "boolean":
			case "int1":
				return "tinyint";
			case "int2":
				return "smallint";
			case "int3":
				return "mediumint";
			case "int4":
			case "integer":
				return "int";
			case "int8":
				return "bigint";
			case "real":
			case "numeric":
				return "double";
			case "long":
				return "mediumtext";
			default:
				return type;
		}
	}

	public String getName() {
		return name;
	}

	public String getTableName() {
		return tableName;
	}

	public String getType() {
		return type;
	}

	public Integer getPrecision() {
		return precision;
	}

	public Integer getScale() {
		return scale;
	}

	public int getPos() {
		return pos;
	}

	public void setPos(int i) {
		this.pos = i;
	}

	public String getEncoding() {
		return this.encoding;
	}

	public boolean getSigned() {
		return this.signed;
	}

	public String[] getEnumValues() {
		return enumValues;
	}

	/**
	 * Replaces any sequence of non-alpha, non-numeric, and non-underscore characters with an
	 * underscore.
	 */
	protected String avroSanitize(String name) {
		String sanitized = avro_name_invalid.matcher(name).replaceAll(avro_replace_str);
		if ( !avro_name_valid.matcher(sanitized).matches() ) {
			return "_" + sanitized;
		} else {
			return sanitized;
		}
	}
}
