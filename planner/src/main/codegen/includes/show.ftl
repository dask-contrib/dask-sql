// SHOW SCHEMAS
SqlNode SqlShowSchemas() :
{
    final Span s;
    SqlIdentifier catalog = null;
    SqlNode like = null;
}
{
    <SHOW> { s = span(); } <SCHEMAS>
    (
        <FROM> catalog = SimpleIdentifier()
    )?
    (
        <LIKE> like = Literal()
    )?
    {
        return new SqlShowSchemas(s.end(this), catalog, like);
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
    schema = CompoundIdentifier()
    {
        return new SqlShowTables(s.end(this), schema);
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
    tableName = CompoundTableIdentifier()
    {
        return new SqlShowColumns(s.end(this), tableName);
    }
}

// DESCRIBE <table_name>
// DESCRIBE TABLE <table_name>
// DESCRIBE MODEL <model_name>
SqlNode SqlDescribeTableOrModel() :
{
    final Span s;
    final SqlIdentifier schemaName;
    final SqlIdentifier tableName;
    final SqlModelIdentifier modelName;
}
{
    <DESCRIBE> { s = span(); }
    (
        LOOKAHEAD(2)
        modelName = ModelIdentifier()
        {
            return new SqlShowModelParams(s.end(this), modelName);
        }
    |
        [ <TABLE> ]
        tableName = CompoundTableIdentifier()
        {
            return new SqlShowColumns(s.end(this), tableName);
        }
    )
}

// ANALYZE TABLE table_identifier COMPUTE STATISTICS [ FOR COLUMNS col [ , ... ] | FOR ALL COLUMNS ]
SqlNode SqlAnalyzeTable() :
{
    final Span s;
    final SqlIdentifier tableName;
    final List<SqlIdentifier> columnList;
}
{
    <ANALYZE> { s = span(); } <TABLE>
    tableName = CompoundTableIdentifier()
    <COMPUTE> <STATISTICS>
    (
        LOOKAHEAD(2)
        <FOR> <COLUMNS>
        columnList = ColumnIdentifierList()
    |
        <FOR> <ALL> <COLUMNS>
        {
            columnList = new ArrayList<SqlIdentifier>();
        }
    )
    {
        return new SqlAnalyzeTable(s.end(this), tableName, columnList);
    }
}

// SHOW MODELS
SqlNode SqlShowModels() :
{
   final Span s;
}
{
    <SHOW> { s = span(); } <MODELS>
    {
        return new SqlShowModels(s.end(this));
    }
}
