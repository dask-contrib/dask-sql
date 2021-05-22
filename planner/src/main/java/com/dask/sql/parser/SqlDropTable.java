package com.dask.sql.parser;

import java.util.List;

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlDropTable extends SqlDrop {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("DROP TABLE", SqlKind.DROP_TABLE);

    final SqlIdentifier tableName;

    SqlDropTable(SqlParserPos pos, boolean ifExists, SqlIdentifier tableName) {
        super(OPERATOR, pos, ifExists);
        this.tableName = tableName;
    }

    public SqlIdentifier getTableName() {
        return this.tableName;
    }

    public boolean getIfExists() {
        return this.ifExists;
    }

    @Override
    public List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP TABLE");
        if (this.getIfExists()) {
            writer.keyword("IF EXISTS");
        }
        this.getTableName().unparse(writer, leftPrec, rightPrec);
    }
}
