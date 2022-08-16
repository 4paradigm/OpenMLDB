# DROP INDEX

## Syntax

```sql
DROPIndexstmt ::=
    'DROP' 'INDEX' TableName.IndexName
```

**Description**

The `DROP INDEX` statement is used to drop an index from OpenMLDB.

## **Example**
```SQL
DROP INDEX t5.index2;
-- SUCCEED
```

## Related SQL

[CREATE INDEX](./CREATE_INDEX_STATEMENT.md)