package com.dask.sql.catalog.domain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CatalogTable {
	public CatalogTable() { this.tableColumns = new HashMap<String, CatalogColumn>(); }

	public CatalogTable(String name, CatalogDatabase db, List<CatalogColumn> columns) {
		this.name = name;
		this.database = db;
		this.tableColumns = new HashMap<String, CatalogColumn>();
		for(CatalogColumn column : columns) {
			column.setTable(this);
			this.tableColumns.put(column.getColumnName(), column);
		}
	}

	public CatalogTable(String name, CatalogDatabase db, List<String> columnNames, List<String> columnTypes) {
		this.name = name;
		this.database = db;
		this.tableColumns = new HashMap<String, CatalogColumn>();
		for(int i = 0; i < columnNames.size(); i++) {
			CatalogColumn column = new CatalogColumn();
			column.setColumnDataType(columnTypes.get(i));
			column.setTable(this);
			column.setColumnName(columnNames.get(i));
			column.setOrderValue(i);
			this.tableColumns.put(column.getColumnName(), column);
		}
	}
	private Long id;
	private String name;
	private Map<String, CatalogColumn> tableColumns;
	private CatalogDatabase database;

	public CatalogDatabase
	getDatabase() {
		return database;
	}

	public void
	setDatabase(CatalogDatabase database) {
		this.database = database;
	}

	public Long
	getId() {
		return this.id;
	}

	public void
	setId(Long id) {
		this.id = id;
	}

	public String
	getTableName() {
		return this.name;
	}

	public void
	setTableName(String name) {
		this.name = name;
	}

	/**
	 * Converts the columns map into a Set of columns
	 * @return the set of {@see CatalogColumn} that belong to this table
	 */
	public Set<CatalogColumn>
	getColumns() {
		List<CatalogColumn> cols = new ArrayList<CatalogColumn>();

		for(CatalogColumn col : this.tableColumns.values()) {
			cols.add(col);
		}
		Collections.sort(cols);

		Set<CatalogColumn> tempColumns = new LinkedHashSet<CatalogColumn>();

		for(CatalogColumn col : cols) {
			tempColumns.add(col);
		}

		return tempColumns;
	}

	public Map<String, CatalogColumn>
	getTableColumns() {
		return tableColumns;
	}

	public void
	setTableColumns(Map<String, CatalogColumn> columns) {
		this.tableColumns = columns;
	}
}
