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
- The filter columns sepcified by `WHERE` must be an index column and the condition can only be `=`.

## Examples

```SQL
DELETE FROM t1 WHERE col1 = 'aaaa';

DELETE FROM t1 WHERE col1 = 'aaaa' and col2 = 'bbbb';
```