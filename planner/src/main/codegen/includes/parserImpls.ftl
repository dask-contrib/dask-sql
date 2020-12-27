// SHOW SCHEMAS
SqlNode SqlShowSchemas() :
{
    final SqlParserPos pos;
    SqlIdentifier catalog = null;
    SqlNode like = null;
}
{
    <SHOW> <SCHEMAS>
    (
        <FROM> catalog = SimpleIdentifier()
    )?
    (
        <LIKE> like = Literal()
    )?
    {
        pos = getPos();
        return new SqlShowSchemas(pos, catalog, like);
    }
}

// SHOW TABLES FROM "schema"
SqlNode SqlShowTables() :
{
   final Span s;
   final SqlIdentifier schema;
}
{
    <SHOW> { s = span(); } <TABLES> <FROM>
    schema = CompoundIdentifier() {
        return new SqlShowTables(s.end(schema), schema);
    }
}

// SHOW COLUMNS FROM "schema"."timeseries"
SqlNode SqlShowColumns() :
{
    final Span s;
    final SqlIdentifier tableName;
}
{
    <SHOW> { s = span(); } <COLUMNS> <FROM>
    tableName = CompoundIdentifier()
    {
        return new SqlShowColumns(s.end(tableName), tableName);
    }
}

SqlNode SqlDescribeTable() :
{
    final Span s;
    final SqlIdentifier schemaName;
    final SqlIdentifier tableName;
}
{
    <DESCRIBE> { s = span(); }

    tableName = CompoundTableIdentifier()
    {
        return new SqlShowColumns(s.end(tableName), tableName);
    }
}

void KeyValueExpression(final HashMap<SqlNode, SqlNode> kwargs) :
{
    final SqlNode keyword;
    final SqlNode literal;
}
{
    keyword = SimpleIdentifier()
    <EQ>
    literal = Literal()
    {
        kwargs.put(keyword, literal);
    }
}

// CREATE TABLE name WITH (key = value) or
// CREATE TABLE name AS
SqlNode SqlCreateTable() :
{
    final SqlParserPos pos;
    final SqlIdentifier tableName;
    final HashMap<SqlNode, SqlNode> kwargs = new HashMap<SqlNode, SqlNode>();
    final SqlNode select;
}
{
    <CREATE> { pos = getPos(); } <TABLE>
    tableName = SimpleIdentifier()
    (
        <WITH>
        <LPAREN>
        KeyValueExpression(kwargs)
        (
            <COMMA>
            KeyValueExpression(kwargs)
        )*
        <RPAREN>
        {
            return new SqlCreateTable(pos, tableName, kwargs);
        }
    |
        <AS>
        select = OptionallyParenthesizedExpression()
        {
            // True = do make persistent
            return new SqlCreateTableAs(pos, tableName, select, true);
        }
    )
}

// CREATE VIEW name AS
SqlNode SqlCreateView() :
{
    final SqlParserPos pos;
    final SqlIdentifier tableName;
    final SqlNode select;
}
{
    <CREATE> { pos = getPos(); } <VIEW>
    tableName = SimpleIdentifier()
    <AS>
    select = OptionallyParenthesizedExpression()
    {
        // False = do not make persistent
        return new SqlCreateTableAs(pos, tableName, select, false);
    }
}

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

SqlNode OptionallyParenthesizedExpression() :
{
    final SqlNode e;
}
{
    (
        <LPAREN>
        e = ExpressionOrPredict()
        <RPAREN>
    |
        e = ExpressionOrPredict()
    )
    {
        return e;
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