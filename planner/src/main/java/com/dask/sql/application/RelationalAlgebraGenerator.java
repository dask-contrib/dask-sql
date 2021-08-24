package com.dask.sql.application;

import java.sql.SQLException;
import java.util.List;

import com.dask.sql.schema.DaskSchema;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;

/**
 * The core of the calcite program: the generator for the relational algebra.
 * Using a passed schema, it generates (optimized) relational algebra out of SQL
 * query strings or throws an exception.
 *
 * This class is taken (in parts) from the blazingSQL project.
 */
public class RelationalAlgebraGenerator {
	final private DaskPlanner planner;
	final private DaskSqlToRelConverter sqlToRelConverter;
	final private DaskProgram program;
	final private DaskSqlParser parser;

	/// Create a new relational algebra generator from a schema
	public RelationalAlgebraGenerator(final String rootSchemaName, final List<DaskSchema> schemas) throws SQLException {
		this.planner = new DaskPlanner();
		this.sqlToRelConverter = new DaskSqlToRelConverter(this.planner, rootSchemaName, schemas);
		this.program = new DaskProgram(this.planner);
		this.parser = new DaskSqlParser();
	}

	static public SqlDialect getDialect() {
		return DaskSqlDialect.DEFAULT;
	}

	public SqlNode getSqlNode(final String sql) throws SqlParseException, ValidationException {
		final SqlNode sqlNode = this.parser.parse(sql);
		return sqlNode;
	}

	public RelNode getRelationalAlgebra(final SqlNode sqlNode) throws RelConversionException {
		return sqlToRelConverter.convert(sqlNode);
	}

	public RelNode getOptimizedRelationalAlgebra(final RelNode rel) {
		return this.program.run(rel);
	}

	/// Return the string representation of a rel node
	public String getRelationalAlgebraString(final RelNode relNode) {
		return RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES);
	}
}
