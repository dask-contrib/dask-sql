package com.dask.sql.parser;

import org.apache.calcite.sql.SqlDescribeSchema;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;

import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlShowTables extends SqlDescribeSchema {

    public SqlShowTables(SqlParserPos pos, SqlIdentifier schemaName) {
        super(pos, schemaName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW");
        writer.keyword("TABLES");
        if (this.getSchema() != null) {
            this.getSchema().unparse(writer, leftPrec, rightPrec);
        }
    }
}
