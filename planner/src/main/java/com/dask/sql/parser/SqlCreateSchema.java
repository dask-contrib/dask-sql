package com.dask.sql.parser;

import java.util.HashMap;
import java.util.List;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlCreateSchema extends SqlCreate {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE SCHEMA", SqlKind.OTHER_DDL);

    final SqlIdentifier schemaName;

    public SqlCreateSchema(final SqlParserPos pos, final boolean replace, final boolean ifNotExists,
                          final SqlIdentifier schemaName) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.schemaName = schemaName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.getReplace()) {
            writer.keyword("CREATE OR REPLACE SCHEMA");
        } else {
            writer.keyword("CREATE SCHEMA");
        }
        if (this.getIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        this.getSchemaName().unparse(writer, leftPrec, rightPrec);

    }

    @Override
    public List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    public SqlIdentifier getSchemaName() {
        return this.schemaName;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }
}
