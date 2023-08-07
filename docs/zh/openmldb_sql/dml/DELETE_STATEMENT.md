# DELETE

## 语法

```sql
DeleteStmt ::=
    DELETE FROM TableName WHERE where_condition

TableName ::=
    Identifier ('.' Identifier)?
```

**说明**

- `DELETE` 语句删除在线表满足指定条件的数据
- `WHERE` 指定的筛选列必须是索引列。如果是key列只能用等于

## Examples

```SQL
DELETE FROM t1 WHERE col1 = 'aaaa';

DELETE FROM t1 WHERE col1 = 'aaaa' and ts_col = 1687145994000;

DELETE FROM t1 WHERE col1 = 'aaaa' and ts_col > 1687059594000 and ts_col < 1687145994000;

DELETE FROM t1 WHERE ts_col > 1687059594000 and ts_col < 1687145994000;
```