package com.dask.sql.parser;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlModelIdentifier extends SqlCall {
    public enum IdentifierType {
        REFERENCE
    }

    final SqlIdentifier modelName;
    final IdentifierType identifierType;

    public SqlModelIdentifier(final SqlParserPos pos, final IdentifierType identifierType,
            final SqlIdentifier modelName) {
        super(pos);
        this.modelName = modelName;
        this.identifierType = identifierType;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        switch (this.identifierType) {
            case REFERENCE:
                writer.keyword("MODEL");
                break;
        }
        this.modelName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public final SqlOperator getOperator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final List<SqlNode> getOperandList() {
        throw new UnsupportedOperationException();
    }

    public final SqlIdentifier getIdentifier() {
        return this.modelName;
    }

    public final IdentifierType getIdentifierType() {
        return this.identifierType;
    }
}
