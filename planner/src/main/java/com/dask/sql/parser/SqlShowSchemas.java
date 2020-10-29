package com.dask.sql.parser;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlNode;

import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlShowSchemas extends SqlCall {
    public SqlIdentifier catalog;
    public SqlNode like;

    public SqlShowSchemas(SqlParserPos pos, SqlIdentifier catalog, SqlNode like) {
        super(pos);
        this.catalog = catalog;
        this.like = like;
    }

    public SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    public List<SqlNode> getOperandList() {
        ArrayList<SqlNode> operandList = new ArrayList<SqlNode>();
        return operandList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW");
        writer.keyword("SCHEMAS");
        if (this.catalog != null) {
            writer.keyword("FROM");
            this.catalog.unparse(writer, leftPrec, rightPrec);
        }
        if (this.like != null) {
            writer.keyword("LIKE");
            this.like.unparse(writer, leftPrec, rightPrec);
        }
    }
}
