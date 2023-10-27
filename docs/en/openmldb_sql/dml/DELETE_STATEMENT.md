# DELETE

## Syntax

```sql
DeleteStmt ::=
    DELETE FROM TableName WHERE where_condition

TableName ::=
    Identifier ('.' Identifier)?
```

**Description**

- `DELETE` statement will delete all data from the index of specific column value of online table.
- The filter columns sepcified by `WHERE` must be an index column. if it is a key column, only `=` can be used.

## Examples

```SQL
DELETE FROM t1 WHERE col1 = 'aaaa';

DELETE FROM t1 WHERE col1 = 'aaaa' and ts_col = 1687145994000;

DELETE FROM t1 WHERE col1 = 'aaaa' and ts_col > 1687059594000 and ts_col < 1687145994000;

DELETE FROM t1 WHERE ts_col > 1687059594000 and ts_col < 1687145994000;
```