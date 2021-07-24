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

public class SqlDropSchema extends SqlDrop {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("DROP SCHEMA", SqlKind.OTHER_DDL);

    final SqlIdentifier schemaName;

    SqlDropSchema(SqlParserPos pos, boolean ifExists, SqlIdentifier schemaName) {
        super(OPERATOR, pos, ifExists);
        this.schemaName = schemaName;
    }

    public SqlIdentifier getSchemaName() {
        return this.schemaName;
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
        writer.keyword("DROP SCHEMA");
        if (this.getIfExists()) {
            writer.keyword("IF EXISTS");
        }
        this.getSchemaName().unparse(writer, leftPrec, rightPrec);
    }
}
