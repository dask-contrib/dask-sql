package com.dask.sql.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

/**
 * A table in the form, that calcite understands.
 *
 * Basically just a list of columns, each column being a column name and a type.
 */
public class DaskTable implements ProjectableFilterableTable {
	// List of columns (name, column type)
	private final ArrayList<Pair<String, SqlTypeName>> tableColumns;
	// Name of this table
	private final String name;

	/// Construct a new table with the given name
	public DaskTable(final String name) {
		this.name = name;
		this.tableColumns = new ArrayList<Pair<String, SqlTypeName>>();
	}

	/// Add a column with the given type
	public void addColumn(final String columnName, final SqlTypeName columnType) {
		this.tableColumns.add(new Pair<>(columnName, columnType));
	}

	/// return the table name
	public String getTableName() {
		return this.name;
	}

	/// calcite method: Get the type of a row of this table (using the type factory)
	@Override
	public RelDataType getRowType(final RelDataTypeFactory relDataTypeFactory) {
		final RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(relDataTypeFactory);
		for (final Pair<String, SqlTypeName> column : tableColumns) {
			final String name = column.getKey();
			final SqlTypeName type = column.getValue();
			builder.add(name, relDataTypeFactory.createSqlType(type));
			builder.nullable(true);
		}
		return builder.build();
	}

	/// calcite method: statistics of this table (not implemented)
	@Override
	public Statistic getStatistic() {
		return Statistics.UNKNOWN;
	}

	/// calcite method: the type -> it is a table
	@Override
	public Schema.TableType getJdbcTableType() {
		return Schema.TableType.TABLE;
	}

	/// calcite method: it is not rolled up (I think?)
	@Override
	public boolean isRolledUp(final String string) {
		return false;
	}

	/// calcite method: no need to implement this, as it is not rolled up
	@Override
	public boolean rolledUpColumnValidInsideAgg(final String string, final SqlCall sc, final SqlNode sn,
			final CalciteConnectionConfig ccc) {
		throw new AssertionError("This should not be called!");
	}

	/**
	 * calcite method: normally, this would return the actual data - but we do not
	 * use the computation engine from calcite
	 */
	@Override
	public Enumerable<Object[]> scan(final DataContext root, final List<RexNode> filters, final int[] projects) {
		return null;
	}
}
