package com.zendesk.maxwell;

import com.google.code.or.binlog.impl.event.UpdateRowsEvent;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.common.glossary.Pair;
import com.google.code.or.common.glossary.Row;
import com.google.code.or.common.glossary.column.BitColumn;
import com.zendesk.maxwell.schema.Table;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class MaxwellUpdateRowsEvent extends MaxwellAbstractRowsEvent {
	private final UpdateRowsEvent event;

	public MaxwellUpdateRowsEvent(UpdateRowsEvent e, Table t, MaxwellFilter f) {
		super(e, t, f);
		this.event = e;
	}

	public MaxwellUpdateRowsEvent(UpdateRowsEventV2 e2, Table table, MaxwellFilter filter) {
		super(e2, table, filter);
		UpdateRowsEvent e =  new UpdateRowsEvent(e2.getHeader());

		e.setBinlogFilename(e2.getBinlogFilename());
		e.setColumnCount(e2.getColumnCount());
		e.setRows(e2.getRows());
		e.setTableId(e2.getTableId());
		e.setUsedColumnsAfter(e2.getUsedColumnsAfter());
		e.setUsedColumnsBefore(e2.getUsedColumnsBefore());
		e.setReserved(e2.getReserved());
		this.event = e;
	}


	@Override
	public List<Row> getRows() { // only for filterRows() at the moment.  need to refactor that.
		ArrayList<Row> result = new ArrayList<Row>();
		for (Pair<Row> p : event.getRows()) {
			result.add(p.getAfter());
		}
		return result;
	}

	@Override
	public String sqlOperationString() {
		return "REPLACE INTO ";
	}
	@Override
	public String getType() {
		return "update";
	}

	private LinkedList<Pair<Row>> filteredRowsBeforeAndAfter;
	private boolean performedBeforeAndAfterFilter;

	private List<Pair<Row>> filteredRowsBeforeAndAfter() {
		if ( this.filter == null)
			return event.getRows();

		if ( performedBeforeAndAfterFilter )
			return filteredRowsBeforeAndAfter;

		filteredRowsBeforeAndAfter = new LinkedList<>();
		for ( Pair<Row> p : event.getRows()) {
			if ( this.filter.matchesRow(this, p.getAfter()) )
				filteredRowsBeforeAndAfter.add(p);
		}
		performedBeforeAndAfterFilter = true;
		return filteredRowsBeforeAndAfter;
	}

	@Override
	public List<RowMap> buildRowMaps() {
		ArrayList<RowMap> list = new ArrayList<>();
		for (Pair<Row> p : filteredRowsBeforeAndAfter() ) {
			Row after = p.getAfter();
			Row before = p.getBefore();

			RowMap rowMap = buildRowMap();

			for ( ColumnWithDefinition cd : new ColumnWithDefinitionList(table, after, event.getUsedColumnsAfter())) {
				rowMap.putData(cd.getName(), cd.getDefinition(), cd.getValue());
			}

			for ( ColumnWithDefinition cd : new ColumnWithDefinitionList(table, before, event.getUsedColumnsBefore())) {
				String name = cd.getName();
				Object beforeValue = cd.getValue();

				if (!rowMap.hasData(name)) {
					/*
					   If we find a column in the BEFORE image that's *not* present in the AFTER image,
					   we're running in binlog_row_image = MINIMAL.  In this case, the BEFORE image acts
					   as a sort of WHERE clause to update rows with the new values (present in the AFTER image).

					   In order to reconstruct as much of the row as possible, here we fill in
					   missing data in the rowMap with values from the BEFORE image
					 */
					rowMap.putData(name, cd.getDefinition(), beforeValue);
				} else {
					if ( !rowMap.containsValue(name, beforeValue) ) {
						rowMap.putOldData(name, cd.getDefinition(), beforeValue);
					}
				}
			}
			list.add(rowMap);
		}

		return list;
	}

	@Override
	protected BitColumn getUsedColumns() {
		return event.getUsedColumnsAfter(); // not actually used, since we override buildRowMaps()
	}
}
