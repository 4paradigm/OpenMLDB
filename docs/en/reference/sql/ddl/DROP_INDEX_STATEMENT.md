# DROP INDEX
The `DROP INDEX` statement is used to drop an index of a specific table.

## Syntax

```sql
DROPIndexstmt ::=
    'DROP' 'INDEX' TableName.IndexName
```




## **Example**
```SQL
DROP INDEX t5.index2;
-- SUCCEED
```

## Related SQL

[CREATE INDEX](./CREATE_INDEX_STATEMENT.md)