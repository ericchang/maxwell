package com.zendesk.maxwell;

import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.DatetimeColumn;
import com.zendesk.maxwell.schema.columndef.ColumnDef;

public class ColumnWithDefinition {
	private Column column;
	private ColumnDef definition;

	public ColumnWithDefinition(Column column, ColumnDef definition) {
		this.column = column;
		this.definition = definition;
	}

	private Object mappedValue() {
		if (column instanceof DatetimeColumn)
			return ((DatetimeColumn) column).getLongValue();
		return column.getValue();
	}

	public Object getValue() {
		return mappedValue();
	}

	public String getName() {
		return definition.getName();
	}

	public ColumnDef getDefinition() {
		return definition;
	}
}

