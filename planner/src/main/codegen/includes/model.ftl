SqlIdentifier ModelIdentifier() :
{
    final SqlIdentifier modelName;
}
{
    <MODEL>
    modelName = SimpleIdentifier()
    {
        return modelName;
    }
}

SqlNode SqlPredictModel() :
{
    final SqlParserPos pos;
    final List<SqlNode> selectList;
    final SqlIdentifier model;
    final SqlNode select;
    final SqlNode stmt;
}
{
    (
        LOOKAHEAD(<SELECT> SelectList() <FROM> <PREDICT>)
        <SELECT>
        { pos = getPos(); }
        selectList = SelectList()
        <FROM> <PREDICT>
        <LPAREN>
        model = ModelIdentifier()
        <COMMA>
        select = OptionallyParenthesizedExpression()
        <RPAREN>
        {
            return new SqlPredictModel(pos, new SqlNodeList(selectList, Span.of(selectList).pos()), model, select);
        }
    |
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        {
            return stmt;
        }
    )
}