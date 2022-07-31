# DELETE

## Syntax

```sql
DeleteStmt ::=
    DELETE FROM TableName WHERE where_condition

TableName ::=
    Identifier ('.' Identifier)?
```

**Description**

`DELETE` statement will delete all data from the first index of specific column value. 


## Examples

```SQL
DELETE FROM t1 WHERE col1 = 'aaaa';

DELETE FROM t1 WHERE col1 = 'aaaa' and col2 = 'bbbb';
```