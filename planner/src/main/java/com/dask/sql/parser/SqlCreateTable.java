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

public class SqlCreateTable extends SqlCreate {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    final SqlIdentifier tableName;
    final SqlKwargs kwargs;

    public SqlCreateTable(final SqlParserPos pos, final boolean replace, final boolean ifNotExists,
            final SqlIdentifier tableName, final SqlKwargs kwargs) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.tableName = tableName;
        this.kwargs = kwargs;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.getReplace()) {
            writer.keyword("CREATE OR REPLACE TABLE");
        } else {
            writer.keyword("CREATE TABLE");
        }
        if (this.getIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        this.getTableName().unparse(writer, leftPrec, rightPrec);
        writer.keyword("WITH (");
        this.kwargs.unparse(writer, leftPrec, rightPrec);
        writer.keyword(")");
    }

    @Override
    public List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    public SqlIdentifier getTableName() {
        return this.tableName;
    }

    public HashMap<SqlNode, SqlNode> getKwargs() {
        return this.kwargs.getMap();
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }
}
