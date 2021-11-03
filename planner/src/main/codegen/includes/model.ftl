SqlModelIdentifier ModelIdentifier() :
{
    final Span s;
    final SqlIdentifier modelName;
}
{
    <MODEL> { s = span(); }
    modelName = CompoundTableIdentifier()
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
}
{
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
}

// EXPORT MODEL "name" WITH ()
SqlNode SqlExportModel() :
{
    final Span s;
    final SqlModelIdentifier modelName;
    final SqlKwargs kwargs;

}
{
    <EXPORT> { s = span(); }
    modelName = ModelIdentifier()
    <WITH>
    kwargs = ParenthesizedKeyValueExpressions()
    {
        return new SqlExportModel(s.end(this), modelName,kwargs);
    }
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
    modelName = CompoundTableIdentifier()
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
 *   CREATE EXPERIMENT name WITH ( key = value ) AS
 */
SqlCreate SqlCreateExperiment(final Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier experimentName;
    final SqlKwargs kwargs;
    final SqlNode select;
}
{
    <EXPERIMENT>
    ifNotExists = IfNotExists()
    experimentName = CompoundTableIdentifier()
    <WITH>
    kwargs = ParenthesizedKeyValueExpressions()
    <AS>
    select = OptionallyParenthesizedQuery()
    {
        return new SqlCreateExperiment(s.end(this), replace, ifNotExists, experimentName, kwargs, select);
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
    modelName = CompoundTableIdentifier()
    {
        return new SqlDropModel(s.end(this), ifExists, modelName);
    }
}
