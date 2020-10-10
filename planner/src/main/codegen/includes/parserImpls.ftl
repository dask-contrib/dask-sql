// SHOW SCHEMAS
SqlNode SqlShowSchemas() :
{
    SqlParserPos pos;
}
{
    <SHOW> <SCHEMAS>
    {
        pos = getPos();
        return new SqlShowSchemas(pos);
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

// CREATE TABLE name WITH (key = value)
SqlNode SqlCreateTable() :
{
    final SqlParserPos pos;
    final SqlIdentifier tableName;
    final HashMap<SqlNode, SqlNode> kwargs = new HashMap<SqlNode, SqlNode>();
}
{
    <CREATE> { pos = getPos(); } <TABLE>
    tableName = SimpleIdentifier()
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
}
