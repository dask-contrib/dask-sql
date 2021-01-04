SqlModelIdentifier ModelIdentifier() :
{
    final Span s;
    final SqlIdentifier modelName;
}
{
    <MODEL> { s = span(); }
    modelName = SimpleIdentifier()
    {
        return new SqlModelIdentifier(s.end(this), SqlModelIdentifier.IdentifierType.REFERENCE, modelName);
    }
}

SqlNode SqlPredictModel() :
{
    final Span s;
    final List<SqlNode> selectList;
    final SqlModelIdentifier model;
    final SqlNode select;
    final SqlNode stmt;
}
{
    (
        LOOKAHEAD(<SELECT> SelectList() <FROM> <PREDICT>)
        <SELECT>
        { s = span(); }
        selectList = SelectList()
        <FROM> <PREDICT>
        <LPAREN>
        model = ModelIdentifier()
        <COMMA>
        select = OptionallyParenthesizedQuery()
        <RPAREN>
        {
            return new SqlPredictModel(s.end(this), new SqlNodeList(selectList, Span.of(selectList).pos()), model, select);
        }
    |
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        {
            return stmt;
        }
    )
}

/*
 * Production for
 *   CREATE MODEL name WITH ( key = value ) AS
 */
SqlCreate SqlCreateModel(final Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier modelName;
    final SqlKwargs kwargs;
    final SqlNode select;
}
{
    <MODEL>
    ifNotExists = IfNotExists()
    modelName = SimpleIdentifier()
    <WITH>
    kwargs = ParenthesizedKeyValueExpressions()
    <AS>
    select = OptionallyParenthesizedQuery()
    {
        return new SqlCreateModel(s.end(this), replace, ifNotExists, modelName, kwargs, select);
    }
}


/*
 * Production for
 *   DROP MODEL model
 */
SqlDrop SqlDropModel(final Span s, boolean replace) :
{
    final SqlIdentifier modelName;
    final boolean ifExists;
}
{
    <MODEL>
    ifExists = IfExists()
    modelName = SimpleIdentifier()
    {
        return new SqlDropModel(s.end(this), ifExists, modelName);
    }
}
