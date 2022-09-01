package com.dask.sql.parser;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlShowModels extends SqlCall {

    public SqlShowModels(SqlParserPos pos) {
        super(pos);
    }
    public SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    public List<SqlNode> getOperandList() {
        ArrayList<SqlNode> operandList = new ArrayList<SqlNode>();
        return operandList;
    }

    @Override
    public void unparse(SqlWriter writer,int leftPrec, int rightPrec) {
        writer.keyword("SHOW");
        writer.keyword("MODELS");

    }
}
