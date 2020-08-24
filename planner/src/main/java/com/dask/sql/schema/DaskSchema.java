/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

package com.dask.sql.schema;

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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

public class DaskSchema implements Schema {
	final static Logger LOGGER = LoggerFactory.getLogger(DaskSchema.class);

	private String name;
	private Map<String, DaskTable> databaseTables;

	public DaskSchema(String name) {
		this.databaseTables = new HashMap<String, DaskTable>();
		this.name = name;
	}

	public void
	addTable(DaskTable table) {
		this.databaseTables.put(table.getTableName(), table);
	}

	public void
	removeTable(DaskTable table) {
		this.databaseTables.remove(table.getTableName());
	}

	public void
	removeTable(String tableName) {
		this.databaseTables.remove(tableName);
	}

	public String
	getName() {
		return this.name;
	}

	@Override
	public Table
	getTable(String name) {
		return this.databaseTables.get(name);
	}

	@Override
	public Set<String>
	getTableNames() {
		Set<String> tableNames = new LinkedHashSet<String>();
		tableNames.addAll(this.databaseTables.keySet());
		return tableNames;
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
		throw new UnsupportedOperationException("Getting Expressions is not supported");
	}

	@Override
	public boolean
	isMutable() {
		return true;
	}

	@Override
	public Schema
	snapshot(SchemaVersion sv) {
		throw new UnsupportedOperationException("Snapshot is not supported");
	}
}
