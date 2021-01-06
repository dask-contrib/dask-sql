package com.dask.sql.parser;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAnalyzeTable extends SqlCall {
    final SqlIdentifier tableName;
    final List<SqlIdentifier> columnList;

    public SqlAnalyzeTable(final SqlParserPos pos, final SqlIdentifier tableName,
            final List<SqlIdentifier> columnList) {
        super(pos);
        this.tableName = tableName;
        this.columnList = columnList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ANALYZE TABLE");
        this.getTableName().unparse(writer, leftPrec, rightPrec);
        writer.keyword("COMPUTE STATISTICS");

        if (this.columnList.isEmpty()) {
            writer.keyword("FOR ALL COLUMNS");
        } else {
            boolean first = false;
            for (SqlIdentifier column : this.columnList) {
                if (!first) {
                    writer.keyword(",");
                }
                column.unparse(writer, leftPrec, rightPrec);
            }
        }
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

    public List<SqlIdentifier> getColumnList() {
        return this.columnList;
    }
}
