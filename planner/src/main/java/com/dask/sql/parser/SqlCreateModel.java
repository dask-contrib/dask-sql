package com.dask.sql.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlCreateModel extends SqlCreate {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE MODEL", SqlKind.OTHER_DDL);

    final SqlIdentifier modelName;
    final SqlKwargs kwargs;
    final SqlNode select;

    public SqlCreateModel(final SqlParserPos pos, final boolean replace, final boolean ifNotExists,
            final SqlIdentifier modelName, final SqlKwargs kwargs, final SqlNode select) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.modelName = modelName;
        this.kwargs = kwargs;
        this.select = select;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.getReplace()) {
            writer.keyword("CREATE OR REPLACE MODEL");
        } else {
            writer.keyword("CREATE MODEL");
        }
        if (this.getIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        this.getModelName().unparse(writer, leftPrec, rightPrec);
        writer.keyword("WITH (");
        this.kwargs.unparse(writer, leftPrec, rightPrec);
        writer.keyword(")");
        writer.keyword("AS");
        this.getSelect().unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    public SqlIdentifier getModelName() {
        return this.modelName;
    }

    public HashMap<SqlNode, SqlNode> getKwargs() {
        return this.kwargs.getMap();
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    public SqlNode getSelect() {
        return this.select;
    }
}
