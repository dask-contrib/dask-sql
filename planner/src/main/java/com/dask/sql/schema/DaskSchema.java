package com.dask.sql.schema;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * A DaskSchema contains the list of all known tables.
 *
 * In principle it is just a mapping table name -> table in a format that
 * calcite understands.
 */
public class DaskSchema implements Schema {
	/**
	 * Name of this schema. Typically we will only have a single schema (but might
	 * have more in the future)
	 */
	private final String name;
	/// Mapping of tables name -> table.
	private final Map<String, DaskTable> databaseTables;

	/// Create a new DaskSchema with the given name
	public DaskSchema(final String name) {
		this.databaseTables = new HashMap<String, DaskTable>();
		this.name = name;
	}

	/// Add an already created table to the list
	public void addTable(final DaskTable table) {
		this.databaseTables.put(table.getTableName(), table);
	}

	/// Remove a table from the list
	public void removeTable(final DaskTable table) {
		this.databaseTables.remove(table.getTableName());
	}

	/// Remove a table with the given name from the list
	public void removeTable(final String tableName) {
		this.databaseTables.remove(tableName);
	}

	/// Get the name of this schema
	public String getName() {
		return this.name;
	}

	/// calcite method: return the table with the given name
	@Override
	public Table getTable(final String name) {
		return this.databaseTables.get(name);
	}

	/// calcite method: return all stored table names
	@Override
	public Set<String> getTableNames() {
		final Set<String> tableNames = new LinkedHashSet<String>();
		tableNames.addAll(this.databaseTables.keySet());
		return tableNames;
	}

	/// calcite method: return all defined functions (currently none)
	@Override
	public Collection<Function> getFunctions(final String string) {
		final Collection<Function> functionCollection = new HashSet<Function>();
		return functionCollection;
	}

	/// calcite method: return all function names (currently none)
	@Override
	public Set<String> getFunctionNames() {
		final Set<String> functionSet = new HashSet<String>();
		return functionSet;
	}

	/// calcite method: return any sub-schema (none)
	@Override
	public Schema getSubSchema(final String string) {
		return null;
	}

	/// calcite method: return all sub-schema names (none)
	@Override
	public Set<String> getSubSchemaNames() {
		final Set<String> hs = new HashSet<String>();
		return hs;
	}

	/// calcite method: get a type of the give name (currently none)
	@Override
	public RelProtoDataType getType(final String name) {
		return null;
	}

	/// calcite method: get all type names (none)
	@Override
	public Set<String> getTypeNames() {
		final Set<String> hs = new HashSet<String>();
		return hs;
	}

	/// calcite method: get an expression (not supported)
	@Override
	public Expression getExpression(final SchemaPlus sp, final String string) {
		return null;
	}

	/// calcite method: the schema is not mutable (I think?)
	@Override
	public boolean isMutable() {
		return false;
	}

	/// calcite method: snapshot (not supported)
	@Override
	public Schema snapshot(final SchemaVersion sv) {
		return null;
	}
}
