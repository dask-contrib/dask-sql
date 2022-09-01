package com.dask.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlDistributeBy
        extends SqlCall {

    public final SqlNodeList distributeList;
    final SqlNode select;

    public SqlDistributeBy(SqlParserPos pos,
                           SqlNode select,
                           SqlNodeList distributeList) {
        super(pos);
        this.select = select;
        this.distributeList = distributeList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        this.select.unparse(writer, leftPrec, rightPrec);
        writer.keyword("DISTRIBUTE BY");
        writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA, this.distributeList);
    }

    @Override
    public SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("nullness")
    @Override
    public List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    public SqlNode getSelect() {
        return this.select;
    }

    public SqlNodeList getDistributeList() {
        return this.distributeList;
    }
}
