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

public class SqlDropModel extends SqlDrop {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("DROP MODEL", SqlKind.OTHER_DDL);

    final SqlIdentifier modelName;

    SqlDropModel(SqlParserPos pos, boolean ifExists, SqlIdentifier modelName) {
        super(OPERATOR, pos, ifExists);
        this.modelName = modelName;
    }

    public SqlIdentifier getModelName() {
        return this.modelName;
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
        writer.keyword("DROP MODEL");
        if (this.getIfExists()) {
            writer.keyword("IF EXISTS");
        }
        this.getModelName().unparse(writer, leftPrec, rightPrec);
    }
}
