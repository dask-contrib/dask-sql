package com.dask.sql.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlKwargs extends SqlCall {
    final HashMap<SqlNode, SqlNode> map;

    public SqlKwargs(final SqlParserPos pos, final HashMap<SqlNode, SqlNode> map) {
        super(pos);
        this.map = map;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        boolean firstRound = true;
        for (Map.Entry<SqlNode, SqlNode> entry : this.getMap().entrySet()) {
            if (!firstRound) {
                writer.keyword(",");
            }
            entry.getKey().unparse(writer, leftPrec, rightPrec);
            writer.keyword("=");
            entry.getValue().unparse(writer, leftPrec, rightPrec);
            firstRound = false;
        }
    }

    @Override
    public SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    public HashMap<SqlNode, SqlNode> getMap() {
        return this.map;
    }
}
