package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.RowMap;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import org.apache.avro.Schema;

import java.util.*;

public abstract class AbstractProducer {
    public enum Format {
        JSON("json"), AVRO_JSON("avro-json");

        final String code;

        Format(String code) {
            this.code = code;
        }

        public static Format findByCode(String code) {
            for ( Format f : values() ) {
                if ( f.code.equals(code) ) {
                    return f;
                }
            }
            throw new RuntimeException(String.format("Could not find format for %s", code));
        }
    }

	protected final MaxwellContext context;
	protected Map<RowSchemaKey, Schema> rowShemaCache = new HashMap<>();
    protected EnumSet<Format> supportedFormats;
    protected final Format format;

	public AbstractProducer(MaxwellContext context,
                            EnumSet<Format> supportedFormats,
                            Format format) {
        if(!supportedFormats.contains(format)) {
            throw new RuntimeException(String.format("Unsupported format %s", format));
        }

		this.context = context;
        this.supportedFormats = supportedFormats;
        this.format = format;
	}

	abstract public void push(RowMap r) throws Exception;

    protected byte[] serialize(RowMap r, String namespace) throws Exception {
        switch ( format ) {
            case AVRO_JSON:
                return r.toAvroJSON(getRowSchema(r, namespace)).getBytes("UTF-8");
            case JSON:
                return r.toJSON().getBytes("UTF-8");
            default:
                throw new RuntimeException(String.format("unsupported format %s", format));
        }
    }

    class RowSchemaKey {
        final String db;
        final String table;
        final Set<String> columns;

        RowSchemaKey(RowMap r) {
            this.db = r.getDatabase();
            this.table = r.getTable();
            this.columns = new HashSet<>();
            for ( ColumnDef columnDef : r.getColumns() ) {
                columns.add(columnDef.getName());
            }
        }

        @Override
        public boolean equals(Object o) {
            if ( o == this ) {
                return true;
            }

            if ( o instanceof RowSchemaKey ) {
                RowSchemaKey rk = (RowSchemaKey) o;
                return Objects.equals(db, rk.db)
                       && Objects.equals(table, rk.table)
                       && Objects.equals(columns, rk.columns);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return 17 * Objects.hash(db, table, columns);
        }
    }

	/**
	 * Caches results of {@link RowMap#generateAvroSchema(String, String)}} using the key
	 * (database, table, set(column names)).  Adding the column names to the key handles the
     * case where binlog_row_image=minimal can result in different row schemas for the same table.
	 *
	 * @param rowMap RowMap for which to generate an Avro Schema
	 * @param namespace Avro namespace to assign schema
	 * @return the cached Avro Schema
	 */
	protected Schema getRowSchema(RowMap rowMap, String namespace) {
        RowSchemaKey cacheKey = new RowSchemaKey(rowMap);
		Schema rowSchema = rowShemaCache.get(cacheKey);
		if ( rowSchema == null ) {
			rowSchema = rowMap.generateAvroSchema(namespace, null);
			rowShemaCache.put(cacheKey, rowSchema);
            return rowSchema;
		} else {
			return rowSchema;
		}
	}
}
