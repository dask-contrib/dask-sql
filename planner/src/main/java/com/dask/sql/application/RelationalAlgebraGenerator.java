package com.dask.sql.application;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.dask.sql.schema.DaskSchema;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

/**
 * The core of the calcite program which has references to all other helper
 * classes. Using a passed schema, it generates (optimized) relational algebra
 * out of SQL query strings or throws an exception.
 */
public class RelationalAlgebraGenerator {

	final private DaskPlanner planner;
	final private DaskSqlToRelConverter sqlToRelConverter;
	final private DaskProgram program;
	final private DaskSqlParser parser;

	public RelationalAlgebraGenerator(final String rootSchemaName,
									  final List<DaskSchema> schemas,
									  final boolean case_sensitive,
									  ArrayList<String> disabledRulesPython) throws SQLException {
		ArrayList<RelOptRule> disabledRules = new ArrayList<>();
		if (disabledRulesPython != null) {
			for (String rule : disabledRulesPython) {
				disabledRules.add(new RelOptRule() {
					@Override
					public void onMatch(RelOptRuleCall call) {

					}
				})
			}
		}

		this.planner = new DaskPlanner(disabledRules);
		this.sqlToRelConverter = new DaskSqlToRelConverter(this.planner, rootSchemaName, schemas, case_sensitive);
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

	public String getRelationalAlgebraString(final RelNode relNode) {
		return RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES);
	}
}
