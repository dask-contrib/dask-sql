package com.dask.sql.application;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.dask.sql.schema.DaskSchema;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/**
 * DaskSqlToRelConverter turns a tree of SqlNodes into a first, suboptimal
 * version of relational algebra nodes. It needs to know about all the tables
 * and schemas, therefore the configuration looks a bit more complicated.
 */
public class DaskSqlToRelConverter {
    private final SqlToRelConverter sqlToRelConverter;
    private final boolean caseSensitive;

    public DaskSqlToRelConverter(final RelOptPlanner optimizer, final String rootSchemaName,
                                 final List<DaskSchema> schemas, boolean caseSensitive) throws SQLException {
        this.caseSensitive = caseSensitive;
        final SchemaPlus rootSchema = createRootSchema(rootSchemaName, schemas);

        final JavaTypeFactoryImpl typeFactory = createTypeFactory();
        final CalciteCatalogReader calciteCatalogReader = createCatalogReader(rootSchemaName, rootSchema, typeFactory, this.caseSensitive);
        final SqlValidator validator = createValidator(typeFactory, calciteCatalogReader);
        final RelOptCluster cluster = RelOptCluster.create(optimizer, new RexBuilder(typeFactory));
        final SqlToRelConverter.Config config = SqlToRelConverter.config().withTrimUnusedFields(true).withExpand(true);

        this.sqlToRelConverter = new SqlToRelConverter(null, validator, calciteCatalogReader, cluster,
                StandardConvertletTable.INSTANCE, config);
    }

    public RelNode convert(SqlNode sqlNode) {
        RelNode root = sqlToRelConverter.convertQuery(sqlNode, true, true).project(true);
        return root;
    }

    private JavaTypeFactoryImpl createTypeFactory() {
        return new JavaTypeFactoryImpl(DaskSqlDialect.DASKSQL_TYPE_SYSTEM);
    }

    private SqlValidator createValidator(final JavaTypeFactoryImpl typeFactory,
            final CalciteCatalogReader calciteCatalogReader) {
        final SqlOperatorTable operatorTable = createOperatorTable(calciteCatalogReader);
        final CalciteConnectionConfig connectionConfig = calciteCatalogReader.getConfig();
        final SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                .withSqlConformance(connectionConfig.conformance())
                .withDefaultNullCollation(connectionConfig.defaultNullCollation()).withIdentifierExpansion(true);
        final SqlValidator validator = new CalciteSqlValidator(operatorTable, calciteCatalogReader, typeFactory,
                validatorConfig);
        return validator;
    }

    private SchemaPlus createRootSchema(final String rootSchemaName, final List<DaskSchema> schemas)
            throws SQLException {
        final CalciteConnection calciteConnection = createConnection(rootSchemaName);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();
        for (DaskSchema schema : schemas) {
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
            final JavaTypeFactoryImpl typeFactory, final boolean caseSensitive) throws SQLException {
        final CalciteSchema calciteSchema = CalciteSchema.from(schemaPlus);

        final Properties props = new Properties();
        props.setProperty("defaultSchema", schemaName);
        props.setProperty("caseSensitive", String.valueOf(caseSensitive));

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

    static class CalciteSqlValidator extends SqlValidatorImpl {
        CalciteSqlValidator(SqlOperatorTable opTab, CalciteCatalogReader catalogReader, JavaTypeFactory typeFactory,
                Config config) {
            super(opTab, catalogReader, typeFactory, config);
        }
    }
}
