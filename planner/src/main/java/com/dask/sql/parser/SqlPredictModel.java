package com.dask.sql.parser;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlPredictModel extends SqlCall {
    final SqlNodeList selectList;
    final SqlModelIdentifier modelName;
    final SqlNode select;

    public SqlPredictModel(final SqlParserPos pos, final SqlNodeList selectList, final SqlModelIdentifier modelName,
            final SqlNode select) {
        super(pos);
        this.selectList = selectList;
        this.modelName = modelName;
        this.select = select;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SELECT");

        final SqlNodeList selectClause = this.selectList != null ? this.selectList
                : SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO));
        writer.list(SqlWriter.FrameTypeEnum.SELECT_LIST, SqlWriter.COMMA, selectClause);

        writer.keyword("FROM PREDICT (");

        this.modelName.unparse(writer, leftPrec, rightPrec);
        writer.keyword(",");

        this.select.unparse(writer, leftPrec, rightPrec);

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

    public SqlNode getSelect() {
        return this.select;
    }

    public SqlNodeList getSelectList() {
        return this.selectList;
    }

    public SqlModelIdentifier getModelName() {
        return this.modelName;
    }
}
