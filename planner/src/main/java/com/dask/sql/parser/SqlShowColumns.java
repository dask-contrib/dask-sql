package com.dask.sql.parser;

import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlShowColumns extends SqlDescribeTable {
    SqlIdentifier tableName;

    public SqlShowColumns(SqlParserPos pos, SqlIdentifier tableName) {
        super(pos, tableName, null);
        this.tableName = tableName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW");
        writer.keyword("COLUMNS");
        writer.keyword("FROM");
        this.tableName.unparse(writer, leftPrec, rightPrec);
    }
}
