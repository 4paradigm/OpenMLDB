# DROP INDEX
`DROP INDEX`语句用来删除表中已有的索引。

## 语法

```sql
DROPIndexstmt ::=
    'DROP' 'INDEX' TableName.IndexName
```



## **示例**
```SQL
DROP INDEX t5.index2;
-- SUCCEED
```

## 相关SQL

[CREATE INDEX](./CREATE_INDEX_STATEMENT.md)