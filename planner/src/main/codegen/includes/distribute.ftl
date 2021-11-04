SqlNode SqlDistributeBy():
{
    final List<SqlNode> distributeList = new ArrayList<SqlNode>();
    final SqlNode stmt;
}
{
    stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    (
        <DISTRIBUTE> <BY> SimpleIdentifierCommaList(distributeList)
        {
            return new SqlDistributeBy(getPos(), stmt,
                                       new SqlNodeList(distributeList, Span.of(distributeList).pos()));
        }
    |
        E()
        {
            return stmt;
        }
    )
}
