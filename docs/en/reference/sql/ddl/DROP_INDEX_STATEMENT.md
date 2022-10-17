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

**Note**: If you want to recreate the deleted index, you should wait two `gc_interval` because data will be delete in GC. The default value of `gc_interval` is 60 minute and can be setted in conf/tablet.flags. GC will execute frequently if `gc_interval` is setted too small.

## Related SQL

[CREATE INDEX](./CREATE_INDEX_STATEMENT.md)
