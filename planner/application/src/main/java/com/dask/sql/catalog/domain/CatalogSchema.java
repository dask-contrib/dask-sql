package com.dask.sql.catalog.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CatalogSchema {
	private Long id;

	private String name;

	private Map<String, CatalogDatabase> schemaDatabases;


	public Long
	getId() {
		return id;
	}

	public void
	setId(Long id) {
		this.id = id;
	}

	public String
	getSchemaName() {
		return this.name;
	}

	public void
	getSchemaName(String name) {
		this.name = name;
	}

	public Set<CatalogDatabase>
	getDatabases() {
		Set<CatalogDatabase> tempDatabases = new LinkedHashSet<CatalogDatabase>();
		tempDatabases.addAll(this.schemaDatabases.values());
		return tempDatabases;
	}

	public Map<String, CatalogDatabase>
	getSchemaDatabases() {
		return this.schemaDatabases;
	}

	public void
	setDatabases(Map<String, CatalogDatabase> databases) {
		this.schemaDatabases = databases;
	}

	public CatalogDatabase
	getDatabaseByName(String databaseName) {
		return schemaDatabases.get(databaseName);
	}
}
