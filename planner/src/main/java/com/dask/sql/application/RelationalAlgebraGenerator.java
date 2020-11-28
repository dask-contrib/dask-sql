package com.dask.sql.application;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.dask.sql.schema.DaskSchema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterRemoveIsNotDistinctFromRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

/**
 * The core of the calcite program: the generator for the relational algebra.
 * Using a passed schema, it generates (optimized) relational algebra out of SQL
 * query strings or throws an exception.
 *
 * This class is taken (in parts) from the blazingSQL project.
 */
public class RelationalAlgebraGenerator {
	/// The created planner
	private Planner planner;
	/// The planner for optimized queries
	private HepPlanner hepPlanner;

	/// Create a new relational algebra generator from a schema
	public RelationalAlgebraGenerator(final DaskSchema schema) throws ClassNotFoundException, SQLException {
		// Taken from https://calcite.apache.org/docs/ and blazingSQL

		final CalciteConnection calciteConnection = getCalciteConnection();
		calciteConnection.setSchema(schema.getName());

		final SchemaPlus rootSchema = calciteConnection.getRootSchema();
		rootSchema.add(schema.getName(), schema);

		final FrameworkConfig config = getConfig(rootSchema, schema.getName());

		planner = Frameworks.getPlanner(config);
		hepPlanner = getHepPlanner(config);
	}

	/// Create the framework config, e.g. containing with SQL dialect we speak
	private FrameworkConfig getConfig(final SchemaPlus rootSchema, final String schemaName) {
		final List<String> defaultSchema = new ArrayList<String>();
		defaultSchema.add(schemaName);

		final Properties props = new Properties();
		props.setProperty("defaultSchema", schemaName);

		final SchemaPlus schemaPlus = rootSchema.getSubSchema(schemaName);
		final CalciteSchema calciteSchema = CalciteSchema.from(schemaPlus);

		final CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(calciteSchema, defaultSchema,
				new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT), new CalciteConnectionConfigImpl(props));

		final List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
		sqlOperatorTables.add(SqlStdOperatorTable.instance());
		sqlOperatorTables.add(SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(SqlLibrary.POSTGRESQL));
		sqlOperatorTables.add(calciteCatalogReader);

		SqlParser.Config parserConfig = getDialect().configureParser(SqlParser.Config.DEFAULT)
				.withConformance(SqlConformanceEnum.DEFAULT).withParserFactory(new DaskSqlParserImplFactory());

		SqlOperatorTable operatorTable = SqlOperatorTables.chain(sqlOperatorTables);

		return Frameworks.newConfigBuilder().defaultSchema(schemaPlus).parserConfig(parserConfig)
				.executor(new RexExecutorImpl(null)).operatorTable(operatorTable).build();
	}

	/// Return the default dialect used
	public SqlDialect getDialect() {
		// This is basically PostgreSQL, but we would like to keep the casing
		// of unquoted table identifiers, as this is what pandas/dask
		// would also do.
		// See https://github.com/nils-braun/dask-sql/issues/84
		return new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT.withUnquotedCasing(Casing.UNCHANGED));
	}

	/// Get a connection to "connect" to the database.
	private CalciteConnection getCalciteConnection() throws SQLException {
		// Taken from https://calcite.apache.org/docs/
		final Properties info = new Properties();
		info.setProperty("lex", "JAVA");

		final Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
		return connection.unwrap(CalciteConnection.class);
	}

	/// get an optimizer hep planner
	private HepPlanner getHepPlanner(final FrameworkConfig config) {
		// TODO: check if these rules are sensible
		// Taken from blazingSQL
		final HepProgram program = new HepProgramBuilder()
				.addRuleInstance(AggregateExpandDistinctAggregatesRule.Config.JOIN.toRule())
				.addRuleInstance(FilterAggregateTransposeRule.Config.DEFAULT.toRule())
				.addRuleInstance(FilterJoinRule.JoinConditionPushRule.Config.DEFAULT.toRule())
				.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.Config.DEFAULT.toRule())
				.addRuleInstance(ProjectMergeRule.Config.DEFAULT.toRule())
				.addRuleInstance(FilterMergeRule.Config.DEFAULT.toRule())
				.addRuleInstance(ProjectJoinTransposeRule.Config.DEFAULT.toRule())
				// In principle, not a bad idea. But we need to keep the most
				// outer project - because otherwise the column name information is lost
				// in cases such as SELECT x AS a, y AS B FROM df
				// .addRuleInstance(ProjectRemoveRule.Config.DEFAULT.toRule())
				.addRuleInstance(ReduceExpressionsRule.ProjectReduceExpressionsRule.Config.DEFAULT.toRule())
				// this rule might make sense, but turns a < 1 into a SEARCH expression
				// which is currently not supported by dask-sql
				// .addRuleInstance(ReduceExpressionsRule.FilterReduceExpressionsRule.Config.DEFAULT.toRule())
				.addRuleInstance(FilterRemoveIsNotDistinctFromRule.Config.DEFAULT.toRule())
				.addRuleInstance(AggregateReduceFunctionsRule.Config.DEFAULT.toRule()).build();

		return new HepPlanner(program, config.getContext());
	}

	/// Parse a sql string into a sql tree
	public SqlNode getSqlNode(final String sql) throws SqlParseException {
		try {
			return planner.parse(sql);
		} catch (final SqlParseException e) {
			planner.close();
			throw e;
		}
	}

	/// Validate a sql node
	public SqlNode getValidatedNode(final SqlNode sqlNode) throws ValidationException {
		try {
			return planner.validate(sqlNode);
		} catch (final ValidationException e) {
			planner.close();
			throw e;
		}
	}

	/// Turn a validated sql node into a rel node
	public RelNode getRelationalAlgebra(final SqlNode validatedSqlNode) throws RelConversionException {
		try {
			return planner.rel(validatedSqlNode).project(true);
		} catch (final RelConversionException e) {
			planner.close();
			throw e;
		}
	}

	/// Turn a non-optimized algebra into an optimized one
	public RelNode getOptimizedRelationalAlgebra(final RelNode nonOptimizedPlan) {
		hepPlanner.setRoot(nonOptimizedPlan);
		planner.close();

		return hepPlanner.findBestExp();
	}

	/// Return the string representation of a rel node
	public String getRelationalAlgebraString(final RelNode relNode) {
		return RelOptUtil.toString(relNode);
	}
}
