/*
 * Production for
 *   CREATE TABLE name WITH (key = value) and
 *   CREATE TABLE name AS
 */
SqlCreate SqlCreateTable(final Span s, boolean replace) :
{
    final SqlIdentifier tableName;
    final SqlKwargs kwargs;
    final SqlNode select;
    final boolean ifNotExists;
}
{
    <TABLE>
    ifNotExists = IfNotExists()
    tableName = CompoundTableIdentifier()
    (
        <WITH>
        kwargs = ParenthesizedKeyValueExpressions()
        {
            return new SqlCreateTable(s.end(this), replace, ifNotExists, tableName, kwargs);
        }
    |
        <AS>
        select = OptionallyParenthesizedQuery()
        {
            // True = do make persistent
            return new SqlCreateTableAs(s.end(this), replace, ifNotExists, tableName, select, true);
        }
    )
}

SqlCreate SqlCreateSchema(final Span s, boolean replace) :
{
    final SqlIdentifier schemaName;
    final boolean ifNotExists;
    final SqlKwargs kwargs;
}
{
    <SCHEMA>
    ifNotExists = IfNotExists()
    schemaName = SimpleIdentifier()
    {
     return new SqlCreateSchema(s.end(this), replace, ifNotExists, schemaName);
    }
}

/*
 * Production for
 *   CREATE VIEW name AS
 */
SqlCreate SqlCreateView(final Span s, boolean replace) :
{
    final SqlIdentifier tableName;
    final SqlNode select;
    final boolean ifNotExists;
}
{
    <VIEW>
    ifNotExists = IfNotExists()
    tableName = CompoundTableIdentifier()
    <AS>
    select = OptionallyParenthesizedQuery()
    {
        // False = do not make persistent
        return new SqlCreateTableAs(s.end(this), replace, ifNotExists, tableName, select, false);
    }
}

/*
 * Production for
 *   DROP TABLE table and
 *   DROP VIEW table
 */
SqlDrop SqlDropTable(final Span s, boolean replace) :
{
    final SqlIdentifier tableName;
    final boolean ifExists;
}
{
    (
        <TABLE>
    |
        <VIEW>
    )
    ifExists = IfExists()
    tableName = CompoundTableIdentifier()
    {
        return new SqlDropTable(s.end(this), ifExists, tableName);
    }
}

SqlDrop SqlDropSchema(final Span s, boolean replace) :
{
    final SqlIdentifier schemaName;
    final boolean ifExists;
}
{
    <SCHEMA>
    ifExists = IfExists()
    schemaName = SimpleIdentifier()
    {
        return new SqlDropSchema(s.end(this), ifExists, schemaName);
    }
}
