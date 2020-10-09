<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->
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
