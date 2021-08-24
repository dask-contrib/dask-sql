package com.dask.sql.application;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public class DaskSqlParser {
    private static SqlParser.Config DEFAULT_CONFIG = DaskSqlDialect.DEFAULT.configureParser(SqlParser.Config.DEFAULT)
            .withConformance(SqlConformanceEnum.DEFAULT).withParserFactory(new DaskSqlParserImplFactory());

    public DaskSqlParser() {

    }

    public SqlNode parse(String sql) throws SqlParseException {
        final SqlParser parser = SqlParser.create(sql, DEFAULT_CONFIG);
        final SqlNode sqlNode = parser.parseStmt();
        return sqlNode;
    }
}
