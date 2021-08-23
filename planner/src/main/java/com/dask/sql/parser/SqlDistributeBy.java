package com.dask.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class SqlDistributeBy
        extends SqlCall {

    public static final SqlSpecialOperator OPERATOR = new Operator() {
        @SuppressWarnings("argument.type.incompatible")
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                            SqlParserPos pos, @Nullable SqlNode... operands) {
            //return new SqlDistributeBy(pos, operands[0], (SqlNodeList) operands[1]);
            return new SqlDistributeBy(pos);
        }
    };

//    public final SqlNode query;
//    public final SqlNodeList orderList;

    //public SqlDistributeBy(SqlParserPos pos, SqlNode query, SqlNodeList orderList) {
    public SqlDistributeBy(SqlParserPos pos) {
        super(pos);
//        this.query = query;
//        this.orderList = orderList;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @SuppressWarnings("nullness")
    @Override
    public List<SqlNode> getOperandList() {
        //return ImmutableNullableList.of(query, orderList);
        throw new UnsupportedOperationException();
    }

    /** Definition of {@code DISTRIBUTE BY} operator. */
    private static class Operator extends SqlSpecialOperator {
        private Operator() {
            // NOTE:  make precedence lower then SELECT to avoid extra parens
            super("DISTRIBUTE_BY", SqlKind.ORDER_BY, 0);
        }

        @Override public SqlSyntax getSyntax() {
            return SqlSyntax.POSTFIX;
        }

        @Override public void unparse(
                SqlWriter writer,
                SqlCall call,
                int leftPrec,
                int rightPrec) {
            SqlDistributeBy distributeBy = (SqlDistributeBy) call;
            final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY);
            //distributeBy.query.unparse(writer, getLeftPrec(), getRightPrec());
            writer.endList(frame);
        }
    }
}
