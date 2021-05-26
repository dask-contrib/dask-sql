package com.dask.sql.parser;

import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlExportModel extends SqlDescribeTable {
    SqlIdentifier modelName;
    SqlIdentifier fileName;

    public SqlExportModel(SqlParserPos pos, SqlIdentifier modelName,SqlIdentifier fileName) {
        super(pos, modelName,null);
        this.modelName = modelName;
        this.fileName = fileName;
    }

    public SqlIdentifier getFileName() {
        return this.fileName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("EXPORT");
        writer.keyword("MODEL");
        this.modelName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("INTO");
        this.getFileName().unparse(writer,leftPrec,rightPrec);
    }

}
