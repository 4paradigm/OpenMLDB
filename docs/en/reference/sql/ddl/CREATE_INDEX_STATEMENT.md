# CREATE INDEX

## Syntax

```sql
CreateIndexstmt ::=
    'CREATE' 'INDEX' IndexName ON TableName IndexColumn OptOptionsList

IndexName ::= Identifier

TableName ::=
    Identifier ('.' Identifier)?


IndexColumn ::=
    IndexColumnPrefix ")"

IndexColumnPrefix ::=
    "(" ColumnExpression
    | IndexColumnPrefix "," ColumnExpression

ColumnExpression ::=
    Identifier
     
OptOptionsList ::=
    "OPTIONS" OptionList

OptionList ::=
    OptionsListPrefix ")"

OptionsListPrefix ::=
    "(" OptionEntry
    | OptionsListPrefix "," OptionEntry

OptionEntry ::=
    Identifier "=" Identifier

```

**Description**

The `CREATE INDEX` statement is used to create a new index on OpenMLDB. If there are data in the table, data will be loaded asynchronously. We can see the job status in `ns_client` with [`showopstatus`](../../../maintain/cli.md) command.

## **Example**
```SQL
CREATE INDEX index2 ON t5 (col2);
-- SUCCEED
```
**Note**: If `OPTIONS` is not specified, the created index cannot be use in the SQL to serving online as deployment.

We can also set ts column as below:
```SQL
CREATE INDEX index3 ON t5 (col3) OPTIONS (ts=ts1, ttl_type=absolute, ttl=30d);
-- SUCCEED
```
Please refer [here](./CREATE_TABLE_STATEMENT.md) for more details about `TTL` and `TTL_TYPE`.

## Related SQL

[DROP INDEX](./ddl/DROP_INDEX_STATEMENT.md)