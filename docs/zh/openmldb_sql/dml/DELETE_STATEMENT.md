# DELETE

## 语法

```sql
DeleteStmt ::=
    DELETE FROM TableName WHERE where_condition

TableName ::=
    Identifier ('.' Identifier)?
```

**说明**

- `DELETE` 语句删除在线表满足指定条件的数据，删除并不是所有索引中满足条件的数据都被删除，只会删除与where condition相关的索引，示例见[功能边界](../../quickstart/function_boundary.md#delete)。
- `WHERE` 指定的筛选列必须是索引列。如果是key列只能用等于

## Examples

```SQL
DELETE FROM t1 WHERE col1 = 'aaaa';

DELETE FROM t1 WHERE col1 = 'aaaa' and ts_col = 1687145994000;

DELETE FROM t1 WHERE col1 = 'aaaa' and ts_col > 1687059594000 and ts_col < 1687145994000;

DELETE FROM t1 WHERE ts_col > 1687059594000 and ts_col < 1687145994000;
```
