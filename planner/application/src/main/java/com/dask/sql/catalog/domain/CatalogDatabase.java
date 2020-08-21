package com.dask.sql.catalog.domain;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class CatalogDatabase {
	private Long id;

	private String name;

	private Map<String, CatalogTable> databaseTables;

	public CatalogDatabase() { this.databaseTables = new HashMap<String, CatalogTable>(); }

	public CatalogDatabase(String name) {
		this.name = name;
		this.databaseTables = new HashMap<String, CatalogTable>();
	}

	public Long
	getId() {
		return id;
	}

	public void
	setId(Long id) {
		this.id = id;
	}

	public String
	getDatabaseName() {
		return this.name;
	}

	public void
	setDatabaseName(String name) {
		this.name = name;
	}

	public Set<CatalogTable>
	getTables() {
		Set<CatalogTable> tempTables = new LinkedHashSet<CatalogTable>();
		tempTables.addAll(this.databaseTables.values());
		return tempTables;
	}

	public Map<String, CatalogTable>
	getDatabaseTables() {
		return this.databaseTables;
	}

	public void
	setDatabaseTables(Map<String, CatalogTable> tables) {
		this.databaseTables = tables;
	}

	public CatalogTable
	getTable(String tableName) {
		return this.databaseTables.get(tableName);
	}

	public Set<String>
	getTableNames() {
		Set<String> tableNames = new LinkedHashSet<String>();
		tableNames.addAll(this.databaseTables.keySet());
		return tableNames;
	}

	public void
	addTable(CatalogTable table) {
		this.databaseTables.put(table.getTableName(), table);
	}

	public void
	removeTable(CatalogTable table) {
		this.databaseTables.remove(table.getTableName());
	}
	public void
	removeTable(String tableName) {
		this.databaseTables.remove(tableName);
	}
}
