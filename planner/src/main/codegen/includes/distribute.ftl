/*lNode SqlDistributeBy() :
-
    final Span s;
    final List<SqlNode> selectList;
    final SqlNode stmt;
}
{
    (
        LOOKAHEAD(<SELECT> SelectList() <FROM> <DISTRIBUTE_BY>)
        <SELECT>
        { s = span(); }
        selectList = SelectList()
        <FROM> <DISTRIBUTE_BY>
        {
            return new SqlDistributeBy(s.end(this));
        }
    |
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        {
            return stmt;
        }
    )
}14 4RQZ2325890-gjjyuhr00fzAs3 qaw534332