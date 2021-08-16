package com.dask.sql.application;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.dask.sql.schema.DaskSchema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
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
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
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
	final Planner planner;
	final HepPlanner hepPlanner;

	/// Create a new relational algebra generator from a schema
	public RelationalAlgebraGenerator(final String rootSchemaName, final List<DaskSchema> schemas) throws ClassNotFoundException, SQLException {
		// Taken from https://calcite.apache.org/docs/ and blazingSQL
		final SchemaPlus rootSchema = createRootSchema(rootSchemaName, schemas);

		final JavaTypeFactoryImpl typeFactory = createTypeFactory();
		final CalciteCatalogReader calciteCatalogReader = createCatalogReader(rootSchemaName, rootSchema, typeFactory);
		final SqlOperatorTable operatorTable = createOperatorTable(calciteCatalogReader);
		final SqlParser.Config parserConfig = createParserConfig();
		final SchemaPlus schemaPlus = rootSchema.getSubSchema(rootSchemaName);
		final FrameworkConfig frameworkConfig = createFrameworkConfig(schemaPlus, operatorTable, parserConfig);

		this.planner = createPlanner(frameworkConfig);
		this.hepPlanner = createHepPlanner(frameworkConfig);
	}

	/// Return the default dialect used
	static public SqlDialect getDialect() {
		return DaskSqlDialect.DEFAULT;
	}

	/// Parse a sql string into a sql tree
	public SqlNode getSqlNode(final String sql) throws SqlParseException {
		try {
			return this.planner.parse(sql);
		} catch (final SqlParseException e) {
			this.planner.close();
			throw e;
		}
	}

	/// Validate a sql node
	public SqlNode getValidatedNode(final SqlNode sqlNode) throws ValidationException {
		try {
			return this.planner.validate(sqlNode);
		} catch (final ValidationException e) {
			this.planner.close();
			throw e;
		}
	}

	/// Turn a validated sql node into a rel node
	public RelNode getRelationalAlgebra(final SqlNode validatedSqlNode) throws RelConversionException {
		try {
			return this.planner.rel(validatedSqlNode).project(true);
		} catch (final RelConversionException e) {
			this.planner.close();
			throw e;
		}
	}

	/// Turn a non-optimized algebra into an optimized one
	public RelNode getOptimizedRelationalAlgebra(final RelNode nonOptimizedPlan) {
		this.hepPlanner.setRoot(nonOptimizedPlan);
		this.planner.close();

		return this.hepPlanner.findBestExp();
	}

	/// Return the string representation of a rel node
	public String getRelationalAlgebraString(final RelNode relNode) {
		return RelOptUtil.toString(relNode);
	}

	private Planner createPlanner(final FrameworkConfig config) {
		return Frameworks.getPlanner(config);
	}

	private JavaTypeFactoryImpl createTypeFactory() {
		return new JavaTypeFactoryImpl(DaskSqlDialect.DASKSQL_TYPE_SYSTEM);
	}

	private SchemaPlus createRootSchema(final String rootSchemaName, final List<DaskSchema> schemas) throws SQLException {
		final CalciteConnection calciteConnection = createConnection(rootSchemaName);
		final SchemaPlus rootSchema = calciteConnection.getRootSchema();
		for(DaskSchema schema : schemas) {
			rootSchema.add(schema.getName(), schema);
		}
		return rootSchema;
	}

	private CalciteConnection createConnection(final String schemaName) throws SQLException {
		// Taken from https://calcite.apache.org/docs/
		final Properties info = new Properties();
		info.setProperty("lex", "JAVA");

		final Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
		final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

		calciteConnection.setSchema(schemaName);
		return calciteConnection;
	}

	private CalciteCatalogReader createCatalogReader(final String schemaName, final SchemaPlus schemaPlus,
			final JavaTypeFactoryImpl typeFactory) {
		final CalciteSchema calciteSchema = CalciteSchema.from(schemaPlus);

		final Properties props = new Properties();
		props.setProperty("defaultSchema", schemaName);

		final List<String> defaultSchema = new ArrayList<String>();
		defaultSchema.add(schemaName);

		final CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(calciteSchema, defaultSchema,
				typeFactory, new CalciteConnectionConfigImpl(props));
		return calciteCatalogReader;
	}

	private SqlOperatorTable createOperatorTable(final CalciteCatalogReader calciteCatalogReader) {
		final List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
		sqlOperatorTables.add(SqlStdOperatorTable.instance());
		sqlOperatorTables.add(SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(SqlLibrary.POSTGRESQL));
		sqlOperatorTables.add(calciteCatalogReader);

		SqlOperatorTable operatorTable = SqlOperatorTables.chain(sqlOperatorTables);
		return operatorTable;
	}

	private Config createParserConfig() {
		return getDialect().configureParser(SqlParser.Config.DEFAULT).withConformance(SqlConformanceEnum.DEFAULT)
				.withParserFactory(new DaskSqlParserImplFactory());
	}

	private FrameworkConfig createFrameworkConfig(final SchemaPlus schemaPlus, SqlOperatorTable operatorTable,
			final SqlParser.Config parserConfig) {
		// Use our defined type system
		final Context defaultContext = Contexts.of(CalciteConnectionConfig.DEFAULT.set(
				CalciteConnectionProperty.TYPE_SYSTEM, "com.dask.sql.application.DaskSqlDialect#DASKSQL_TYPE_SYSTEM"));

		return Frameworks.newConfigBuilder().context(defaultContext).defaultSchema(schemaPlus)
				.parserConfig(parserConfig).executor(new RexExecutorImpl(null)).operatorTable(operatorTable).build();
	}

	private HepPlanner createHepPlanner(final FrameworkConfig config) {
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
				// TODO: remove AVG
				.addRuleInstance(AggregateReduceFunctionsRule.Config.DEFAULT.toRule()).build();

		return new HepPlanner(program, config.getContext());
	}

}
