/*
 * A keyword argument in the form key = value
 * where key can be any identifier and value
 * any valid literal, array, map or multiset.
 */
void KeyValueExpression(final HashMap<SqlNode, SqlNode> kwargs) :
{
    final SqlNode keyword;
    final SqlNode literal;
}
{
    keyword = SimpleIdentifier()
    <EQ>
    (
        literal = Literal()
    |
        literal = MultisetConstructor()
    |
        literal = ArrayConstructor()
    |
        LOOKAHEAD(3)
        literal = MapConstructor()
    |
        LOOKAHEAD(3)
        literal = ParenthesizedKeyValueExpressions()
    )
    {
        kwargs.put(keyword, literal);
    }
}

/*
 * A list of key = value, separated by comma
 * and in left and right parenthesis.
 */
SqlKwargs ParenthesizedKeyValueExpressions() :
{
    final Span s;
    final HashMap<SqlNode, SqlNode> kwargs = new HashMap<SqlNode, SqlNode>();
}
{
    <LPAREN> { s = span(); }
    KeyValueExpression(kwargs)
    (
        <COMMA>
        KeyValueExpression(kwargs)
    )*
    <RPAREN>
    {
        return new SqlKwargs(s.end(this), kwargs);
    }
}


SqlNode ExpressionOrPredict() :
{
    final SqlNode e;
}
{
    (
        e = SqlPredictModel()
    |
        e = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    )
    {
        return e;
    }
}

/*
 * A query, optionally in parenthesis.
 * As we might want to replace the query with
 * our own syntax later, we are not using the
 * buildin production for this.
 */
SqlNode OptionallyParenthesizedQuery() :
{
    SqlNode query;
}
{
    (
        <LPAREN>
        query = ExpressionOrPredict()
        <RPAREN>
    |
        query = ExpressionOrPredict()
    )
    {
        return query;
    }
}

/// Optional if not exists
boolean IfNotExists() :
{ }
{
    [
        <IF> <NOT> <EXISTS>
        { return true; }
    ]
    { return false; }
}

/// Optional if exists
boolean IfExists() :
{ }
{
    [
        <IF> <EXISTS>
        { return true; }
    ]
    { return false; }
}

/// List of comma separated column names
List<SqlIdentifier> ColumnIdentifierList() :
{
    final List<SqlIdentifier> columnList = new ArrayList<SqlIdentifier>();
    SqlIdentifier columnName;
}
{
    columnName = SimpleIdentifier()
    {
        columnList.add(columnName);
    }
    (
        <COMMA>
        columnName = SimpleIdentifier()
        {
            columnList.add(columnName);
        }
    )*
    {
        return columnList;
    }
}