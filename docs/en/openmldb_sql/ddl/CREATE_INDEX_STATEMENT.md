# CREATE INDEX

The `CREATE INDEX` statement is used to create a new index on an existing table. Running `CREATE INDEX` initiates an asynchronous job, and you can check the status of the job by executing `SHOW JOBS FROM NAMESERVER`. Please note that the index is not available until the asynchronous task is completed, and any scenarios that require the new index will fail.

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
1. If `OPTIONS` is not provided, the SQL with the created index cannot be deployed online, since the index doesn't have TS (timestamp).
2. The data type of `TS` column should be BigInt or Timestamp.
```
We can also set `TS` column as below:
```SQL
CREATE INDEX index3 ON t5 (col3) OPTIONS (ts=ts1, ttl_type=absolute, ttl=30d);
-- SUCCEED
```
Please refer [here](./CREATE_TABLE_STATEMENT.md) for more details about `TTL` and `TTL_TYPE`.

## Related SQL

[DROP INDEX](./DROP_INDEX_STATEMENT.md)
