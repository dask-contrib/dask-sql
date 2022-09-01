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

public class SqlCreateExperiment extends SqlCreate {
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE EXPERIMENT", SqlKind.OTHER_DDL);

    final SqlIdentifier experimentName;
    final SqlKwargs kwargs;
    final SqlNode select;

    public SqlCreateExperiment(final SqlParserPos pos, final boolean replace, final boolean ifNotExists,
                          final SqlIdentifier experimentName, final SqlKwargs kwargs, final SqlNode select) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.experimentName = experimentName;
        this.kwargs = kwargs;
        this.select = select;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (this.getReplace()) {
            writer.keyword("CREATE OR REPLACE EXPERIMENT");
        } else {
            writer.keyword("CREATE EXPERIMENT");
        }
        if (this.getIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        this.getExperimentName().unparse(writer, leftPrec, rightPrec);
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

    public SqlIdentifier getExperimentName() {
        return this.experimentName;
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
