package com.dask.sql.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlExportModel extends SqlCall {
    final SqlModelIdentifier modelName;
    final SqlKwargs kwargs;

    public SqlExportModel(SqlParserPos pos, SqlModelIdentifier modelName,
                          SqlKwargs kwargs) {
        super(pos);
        this.modelName = modelName;
        this.kwargs = kwargs;
    }

    public SqlModelIdentifier getModelName() {
        return this.modelName;
    }

    public HashMap<SqlNode, SqlNode> getKwargs() {
        return this.kwargs.getMap();
    }

    @Override
    public SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("EXPORT");
        writer.keyword("MODEL");
        this.modelName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("WITH (");
        this.kwargs.unparse(writer, leftPrec, rightPrec);
        writer.keyword(")");
    }

}
