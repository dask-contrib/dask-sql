package com.dask.sql.parser;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class SqlDistributeBy
        extends SqlCall {

    public static final SqlSpecialOperator OPERATOR = new Operator() {
        @SuppressWarnings("argument.type.incompatible")
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                                            SqlParserPos pos, @Nullable SqlNode... operands) {
            return new SqlDistributeBy(pos,
                    (List<SqlNode>) operands[0],
                    (SqlIdentifier) operands[1],
                    (List<SqlNode>) operands[2]);
        }
    };

    public final List<SqlNode> selectList;
    public final SqlNodeList distributeList;
    public final SqlIdentifier tableName;

    public SqlDistributeBy(SqlParserPos pos,
                           List<SqlNode> selectList,
                           SqlIdentifier tableName,
                           List<SqlNode> orderList) {
        super(pos);
        this.selectList = selectList;
        this.tableName = tableName;
        this.distributeList = new SqlNodeList(orderList, pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @SuppressWarnings("nullness")
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(distributeList);
    }

    /** Definition of {@code DISTRIBUTE BY} operator. */
    private static class Operator extends SqlSpecialOperator {
        private Operator() {
            // NOTE:  make precedence lower then SELECT to avoid extra parens
            super("DISTRIBUTE BY", SqlKind.ORDER_BY, 0);
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

//            orderBy.query.unparse(writer, getLeftPrec(), getRightPrec());
//            if (orderBy.orderList != SqlNodeList.EMPTY) {
//                writer.sep(getName());
//                writer.list(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
//                        orderBy.orderList);
//            }
//            if (orderBy.offset != null || orderBy.fetch != null) {
//                writer.fetchOffset(orderBy.fetch, orderBy.offset);
//            }
//            writer.endList(frame);
        }
    }
}
