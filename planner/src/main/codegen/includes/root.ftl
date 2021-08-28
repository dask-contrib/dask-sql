SqlNode ExpressionOrPredict() :
{
    final SqlNode e;
}
{
    (
        LOOKAHEAD(<SELECT> SelectList() <FROM> <PREDICT>)
        e = SqlPredictModel()
    |
        e = SqlDistributeBy()
    |
        e = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    )
    {
        return e;
    }
}
