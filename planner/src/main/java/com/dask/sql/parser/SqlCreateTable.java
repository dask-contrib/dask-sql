package com.dask.sql.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlCreateTable extends SqlCall {
    final SqlIdentifier tableName;
    final HashMap<SqlNode, SqlNode> kwargs;

    public SqlCreateTable(final SqlParserPos pos, final SqlIdentifier tableName,
            final HashMap<SqlNode, SqlNode> kwargs) {
        super(pos);
        this.tableName = tableName;
        this.kwargs = kwargs;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");
        this.tableName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("WITH");
        writer.keyword("(");

        boolean firstRound = true;
        for (Map.Entry<SqlNode, SqlNode> entry : this.kwargs.entrySet()) {
            if (!firstRound) {
                writer.keyword(",");
            }
            entry.getKey().unparse(writer, leftPrec, rightPrec);
            writer.keyword("=");
            entry.getValue().unparse(writer, leftPrec, rightPrec);
            firstRound = false;
        }
        writer.keyword(")");
    }

    @Override
    public SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    public SqlIdentifier getTableName() {
        return this.tableName;
    }

    public HashMap<SqlNode, SqlNode> getKwargs() {
        return this.kwargs;
    }
}
