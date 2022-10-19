# DELETE

## 语法

```sql
DeleteStmt ::=
    DELETE FROM TableName WHERE where_condition

TableName ::=
    Identifier ('.' Identifier)?
```

**说明**

`DELETE` 语句删除指定列的索引下面对应值的所有数据。

## Examples

```SQL
DELETE FROM t1 WHERE col1 = 'aaaa';

DELETE FROM t1 WHERE col1 = 'aaaa' and col2 = 'bbbb';
```