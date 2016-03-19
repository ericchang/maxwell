package com.zendesk.maxwell.schema.columndef;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringEscapeUtils;

import com.google.code.or.common.util.MySQLConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringColumnDef extends ColumnDef {
	static final Logger LOGGER = LoggerFactory.getLogger(StringColumnDef.class);

	private final Schema.Field fieldSchema;

	public StringColumnDef(String tableName, String name, String type, int pos, String encoding) {
		super(tableName, name, type, pos);
		this.encoding = encoding;

		Schema union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
		fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	public void setDefaultEncoding(String e) {
		if ( this.encoding == null )
		  this.encoding = e;
	}

	public void setCharset(String encoding) {
		this.encoding = encoding;
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_BLOB ||
			   type == MySQLConstants.TYPE_VARCHAR ||
			   type == MySQLConstants.TYPE_STRING;
	}

	@Override
	public String toSQL(Object value) {
		byte[] b = (byte[]) value;

		if ( getEncoding().equals("utf8") || getEncoding().equals("utf8mb4")) {
			return quoteString(new String(b));
		} else {
			return "x'" +  Hex.encodeHexString( b ) + "'";
		}
	}

	// this could obviously be more complete.
	private Charset charsetForEncoding() {
		switch(encoding.toLowerCase()) {
		case "utf8": case "utf8mb4":
			return Charset.forName("UTF-8");
		case "latin1": case "ascii":
			return Charset.forName("ISO-8859-1");
		case "ucs2":
			return Charset.forName("UTF-16");
		default:
			LOGGER.warn("warning: unhandled character set '" + encoding + "'");
			return null;
		}
	}
	@Override
	public Object jsonValue(Object value) {

		if ( value instanceof String ) {
			return value;
		}

		byte[] b = (byte[])value;
		if ( encoding.equals("binary") ) {
			return Base64.encodeBase64String(b);
		} else {
			return new String(b, charsetForEncoding());
		}
	}

	private String quoteString(String s) {
		String escaped = StringEscapeUtils.escapeSql(s);
		escaped = escaped.replaceAll("\n", "\\\\n");
		escaped = escaped.replaceAll("\r", "\\\\r");
		return "'" + escaped + "'";
	}

	@Override
	public Schema.Field buildAvroField() {
		return fieldSchema;
	}

	@Override
	public Object avroValue(Object value) {
		return asJSON(value);
	}
}
