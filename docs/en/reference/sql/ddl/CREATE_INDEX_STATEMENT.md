# CREATE INDEX

The `CREATE INDEX` statement is used to create a new index on existing table. If there is data in the table, data will be loaded asynchronously. 
The job status can be checked through the `showopstatus` command of `ns_client`, see [Operations in CLI](../../../maintain/cli.md#showopstatus).

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



## **Example**
```SQL
CREATE INDEX index2 ON t5 (col2);
-- SUCCEED
```
```{note}
If `OPTIONS` is not provided, the SQL with the created index cannot be deployed online, since the index doesn't have TS (timestamp).
```
We can also set `TS` column as below:
```SQL
CREATE INDEX index3 ON t5 (col3) OPTIONS (ts=ts1, ttl_type=absolute, ttl=30d);
-- SUCCEED
```
Please refer [here](./CREATE_TABLE_STATEMENT.md) for more details about `TTL` and `TTL_TYPE`.

## Related SQL

[DROP INDEX](./DROP_INDEX_STATEMENT.md)