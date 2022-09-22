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

**Note**: If you want to recreate the deleted index, you should wait two `gc_interval`. The default value of `gc_interval` is 60 minute.

## Related SQL

[CREATE INDEX](./CREATE_INDEX_STATEMENT.md)