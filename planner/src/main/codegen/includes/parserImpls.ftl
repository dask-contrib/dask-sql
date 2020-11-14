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
    final SqlSelect select;
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
        (
            <LPAREN>
            select = SqlSelect()
            <RPAREN>
        |
            select = SqlSelect()
        )
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
    final SqlSelect select;
}
{
    <CREATE> { pos = getPos(); } <VIEW>
    tableName = SimpleIdentifier()
    <AS>
    (
        <LPAREN>
        select = SqlSelect()
        <RPAREN>
    |
        select = SqlSelect()
    )
    {
        // False = do not make persistent
        return new SqlCreateTableAs(pos, tableName, select, false);
    }
}
