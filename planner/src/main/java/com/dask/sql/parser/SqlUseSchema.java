package com.dask.sql.parser;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlIdentifier;


public class SqlUseSchema extends SqlCall {
    final SqlIdentifier schemaName;

    public SqlUseSchema(SqlParserPos pos,final SqlIdentifier schemaName) {
        super(pos);
        this.schemaName = schemaName;
    }
    public SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    public List<SqlNode> getOperandList() {
        ArrayList<SqlNode> operandList = new ArrayList<SqlNode>();
        return operandList;
    }

    public SqlIdentifier getSchemaName() {
        return this.schemaName;
    }

    @Override
    public void unparse(SqlWriter writer,int leftPrec, int rightPrec) {
        writer.keyword("USE");
        writer.keyword("SCHEMA");
        this.getSchemaName().unparse(writer, leftPrec, rightPrec);

    }
}
