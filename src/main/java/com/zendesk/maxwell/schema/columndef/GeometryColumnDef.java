package com.zendesk.maxwell.schema.columndef;

import com.google.code.or.common.util.MySQLConstants;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ben on 12/30/15.
 */
public class GeometryColumnDef extends ColumnDef {
	private final Schema.Field fieldSchema;

	public GeometryColumnDef(String tableName, String name, String type, int pos) {
		super(tableName, name, type, pos);

		Schema union = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
		this.fieldSchema = new Schema.Field(avroSanitize(name), union, null, JsonProperties.NULL_VALUE);
	}

	@Override
	public boolean matchesMysqlType(int type) {
		return type == MySQLConstants.TYPE_GEOMETRY;
	}

	@Override
	public Object jsonValue(Object value) {
		Geometry g = (Geometry) value;
		return g.toText();
	}

	@Override
	public String toSQL(Object value) {
		return null;
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
