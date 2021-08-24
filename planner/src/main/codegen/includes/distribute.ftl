SqlNode SqlDistributeBy():
{
    final Span s;
    final List<SqlNode> selectList;
    final SqlIdentifier tableName;
    final List<SqlIdentifier> distributeList;
    final SqlNode stmt;
}
{
    (
        LOOKAHEAD(<SELECT> SelectList() <FROM> tableName = CompoundTableIdentifier() <DISTRIBUTE> <BY>)
        <SELECT>
        { s = span(); }
        selectList = SelectList()
        <FROM> tableName = CompoundTableIdentifier() <DISTRIBUTE> <BY> distributeList = ColumnIdentifierList()
        {
            return new SqlDistributeBy(s.end(this), selectList, tableName);
        }
    |
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        {
            return stmt;
        }
    )
}
