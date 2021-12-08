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


public class SqlAlterTable extends SqlCall {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ALTER TABLE", SqlKind.OTHER_DDL);
    final SqlIdentifier oldTableName;
    final SqlIdentifier newTableName;
    final boolean ifExists;

    public SqlAlterTable(SqlParserPos pos, boolean ifExists,
                         final SqlIdentifier oldTableName,
                         final SqlIdentifier newTableName) {
        super(pos);
        this.oldTableName = oldTableName;
        this.newTableName = newTableName;
        this.ifExists = ifExists;
    }
    public SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    public List<SqlNode> getOperandList() {
        ArrayList<SqlNode> operandList = new ArrayList<SqlNode>();
        return operandList;
    }

    public boolean getIfExists() {
        return this.ifExists;
    }

    public SqlIdentifier getOldTableName() {
        return this.oldTableName;
    }

    public SqlIdentifier getNewTableName() {
        return this.newTableName;
    }

    public void unparse(SqlWriter writer,
                        int leftPrec,
                        int rightPrec){
        writer.keyword("ALTER");
        writer.keyword("TABLE");
        if (this.getIfExists()) {
            writer.keyword("IF EXISTS");
        }
        this.getOldTableName().unparse(writer, leftPrec, rightPrec);
        writer.keyword("RENAME");
        writer.keyword("TO");
        this.getNewTableName().unparse(writer,leftPrec,rightPrec);

    }

}
