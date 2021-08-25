package com.dask.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlDistributeBy
        extends SqlCall {

    public final SqlNodeList selectList;
    public final SqlNodeList distributeList;
    public final SqlIdentifier tableName;

    public SqlDistributeBy(SqlParserPos pos,
                           SqlNodeList selectList,
                           SqlIdentifier tableName,
                           SqlNodeList distributeList) {
        super(pos);
        this.selectList = selectList;
        this.tableName = tableName;
        this.distributeList = distributeList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SELECT");

        final SqlNodeList selectClause = this.selectList != null ? this.selectList
                : SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO));
        writer.list(SqlWriter.FrameTypeEnum.SELECT_LIST, SqlWriter.COMMA, selectClause);

        writer.keyword("FROM");
        writer.keyword(this.tableName.getSimple());

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

    public List<SqlNode> getSelectList() {
        return selectList;
    }

    public SqlNodeList getDistributeList() {
        return distributeList;
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }
}
