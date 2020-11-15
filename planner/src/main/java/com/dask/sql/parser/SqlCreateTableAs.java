package com.dask.sql.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlCreateTableAs extends SqlCall {
    final SqlIdentifier tableName;
    final SqlSelect select;
    final boolean persist;

    public SqlCreateTableAs(final SqlParserPos pos, final SqlIdentifier tableName, final SqlSelect select,
            final boolean persist) {
        super(pos);
        this.tableName = tableName;
        this.select = select;
        this.persist = persist;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (this.persist) {
            writer.keyword("TABLE");
        } else {
            writer.keyword("VIEW");
        }
        this.tableName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        this.select.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    public SqlIdentifier getTableName() {
        return this.tableName;
    }

    public SqlSelect getSelect() {
        return this.select;
    }

    public boolean isPersist() {
        return this.persist;
    }
}
