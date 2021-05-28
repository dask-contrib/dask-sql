package com.dask.sql.parser;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;


public class SqlShowModelParams extends SqlCall {
    final SqlModelIdentifier modelName;

    public SqlShowModelParams(SqlParserPos pos, SqlModelIdentifier modelName) {
        super(pos);
        this.modelName = modelName;
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
        writer.keyword("DESCRIBE");
        writer.keyword("MODEL");
        this.modelName.unparse(writer, leftPrec, rightPrec);
    }
    public SqlModelIdentifier getModelName() {
        return this.modelName;
    }

}
