# CREATE INDEX

## Syntax

```sql
CreateIndextmt ::=
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

The `CREATE INDEX` statement is used to create a new index on OpenMLDB. If there are data in the table, some asynchronous jobs will be executed to load data. We can see the job status in `ns_client` with [`showopstatus`](../../../maintain/cli.md) command.

## **Example**
```SQL
CREATE INDEX index2 ON t5 (col2);
-- SUCCEED
```

We can also set ts column as below:
```SQL
CREATE INDEX index3 ON t5 (col3) OPTIONS (ts=ts1, ttl_type=absolute, ttl=30d);
-- SUCCEED
```
More details about `TTL` and `TTL_TYPE` refer [here](./CREATE_TABLE_STATEMENT.md) 

## Related SQL

[DROP INDEX](./ddl/DROP_INDEX_STATEMENT.md)