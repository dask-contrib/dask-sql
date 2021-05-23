package com.dask.sql.parser;

import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlShowModelParams extends SqlDescribeTable {
    SqlIdentifier modelName;

    public SqlShowModelParams(SqlParserPos pos, SqlIdentifier modelName) {
        super(pos, modelName,null);
        this.modelName = modelName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DESCRIBE");
        writer.keyword("MODEL");
        this.modelName.unparse(writer, leftPrec, rightPrec);
    }
}
