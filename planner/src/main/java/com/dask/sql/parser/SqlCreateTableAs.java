package com.dask.sql.parser;

import java.util.List;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlCreateTableAs extends SqlCreate {
    private static final SqlOperator OPERATOR_TABLE = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);
    private static final SqlOperator OPERATOR_VIEW = new SqlSpecialOperator("CREATE VIEW", SqlKind.CREATE_VIEW);

    private static SqlOperator correctOperator(final boolean persist) {
        if (persist) {
            return OPERATOR_TABLE;
        } else {
            return OPERATOR_VIEW;
        }
    }

    final SqlIdentifier tableName;
    final SqlNode select;
    final boolean persist;

    public SqlCreateTableAs(final SqlParserPos pos, final boolean replace, final boolean ifNotExists,
            final SqlIdentifier tableName, final SqlNode select, final boolean persist) {
        super(correctOperator(persist), pos, replace, ifNotExists);
        this.tableName = tableName;
        this.select = select;
        this.persist = persist;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.getReplace()) {
            writer.keyword("CREATE OR REPLACE");
        } else {
            writer.keyword("CREATE");
        }
        if (this.persist) {
            writer.keyword("TABLE");
        } else {
            writer.keyword("VIEW");
        }
        if (this.getIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        this.tableName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        this.select.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    public SqlIdentifier getTableName() {
        return this.tableName;
    }

    public SqlNode getSelect() {
        return this.select;
    }

    public boolean isPersist() {
        return this.persist;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }
}
