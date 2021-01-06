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
    tableName = SimpleIdentifier()
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
    tableName = SimpleIdentifier()
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
    tableName = SimpleIdentifier()
    {
        return new SqlDropTable(s.end(this), ifExists, tableName);
    }
}

