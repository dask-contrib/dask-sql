/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

package com.dask.sql.schema;

import com.dask.sql.catalog.domain.CatalogDatabase;
import com.dask.sql.catalog.domain.CatalogSchema;
import com.dask.sql.catalog.domain.CatalogTable;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class DaskSchema implements Schema {
	final static Logger LOGGER = LoggerFactory.getLogger(DaskSchema.class);

	final private CatalogSchema catalogSchema;
	final private CatalogDatabase catalogDatabase;

	public DaskSchema(CatalogSchema catalogSchema) {
		this.catalogSchema = catalogSchema;
		this.catalogDatabase = null;
	}

	public DaskSchema(CatalogDatabase catalogDatabase) {
		this.catalogSchema = null;
		this.catalogDatabase = catalogDatabase;
	}

	@Override
	public Table
	getTable(String name) {
		if(isDatabase()) {
			final CatalogTable catalogTable = this.catalogDatabase.getTable(name);
			return new DaskTable(catalogTable);
		}

		// TODO percy raise unsupported operation for schema
		LOGGER.debug("was NOT found to be a database!");
		return null;
	}

	public String
	getName() {
		return this.catalogDatabase.getDatabaseName();
	}

	@Override
	public Set<String>
	getTableNames() {
		LOGGER.debug("getting table names");
		return this.catalogDatabase.getTableNames();
	}

	@Override
	public Collection<Function>
	getFunctions(String string) {
		Collection<Function> functionCollection = new HashSet<Function>();
		return functionCollection;
	}

	@Override
	public Set<String>
	getFunctionNames() {
		Set<String> functionSet = new HashSet<String>();
		return functionSet;
	}

	@Override
	public Schema
	getSubSchema(String string) {
		return null;
	}

	@Override
	public Set<String>
	getSubSchemaNames() {
		Set<String> hs = new HashSet<String>();
		return hs;
	}

	@Override
	public Set<String>
	getTypeNames() {
		Set<String> hs = new HashSet<String>();
		return hs;
	}

	@Override
	public RelProtoDataType
	getType(String name) {
		return null;
	}

	@Override
	public Expression
	getExpression(SchemaPlus sp, String string) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public boolean
	isMutable() {
		return true;
	}

	// void updateMetaData(String schema, String table) {
	// metaConnect.updateMetaData(schema, table);
	// }

	@Override
	public Schema
	snapshot(SchemaVersion sv) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	private boolean
	isDatabase() {
		return (this.catalogSchema == null && this.catalogDatabase != null);
	}
}
