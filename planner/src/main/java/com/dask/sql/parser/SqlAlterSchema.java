package com.dask.sql.parser;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.SqlIdentifier;


public class SqlAlterSchema extends SqlCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ALTER SCHEMA", SqlKind.OTHER_DDL);
    final SqlIdentifier oldSchemaName;
    final SqlIdentifier newSchemaName;

    public SqlAlterSchema(SqlParserPos pos,final SqlIdentifier oldSchemaName,final SqlIdentifier newSchemaName) {
        super(pos);
        this.oldSchemaName = oldSchemaName;
        this.newSchemaName = newSchemaName;
    }
    public SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    public List<SqlNode> getOperandList() {
        ArrayList<SqlNode> operandList = new ArrayList<SqlNode>();
        return operandList;
    }

    public SqlIdentifier getOldSchemaName() {
        return this.oldSchemaName;
    }

    public SqlIdentifier getNewSchemaName() {
        return this.newSchemaName;
    }

    public void unparse(SqlWriter writer,
                                                   int leftPrec,
                                                   int rightPrec){
        writer.keyword("ALTER");
        writer.keyword("SCHEMA");

        this.getOldSchemaName().unparse(writer, leftPrec, rightPrec);
        writer.keyword("RENAME");
        writer.keyword("TO");
        this.getNewSchemaName().unparse(writer,leftPrec,rightPrec);

    }

}
