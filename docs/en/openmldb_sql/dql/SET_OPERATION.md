# SET Operation

The SET operation combines two or more query statements with the same output schema into a single result output based on specific rules. There are three set operators: `UNION`, `INTERSECT`, and `EXCEPT`. Currently, OpenMLDB only supports `UNION`.

## Syntax

```yacc
set_operation:
  query set_operator query

set_operator:
  UNION { ALL | DISTINCT }
```

Note:

- `ALL/DISTINCT` is mandatory and the standalone form of `UNION` is not supported.
- Within the same level of Set Operation, multiple `UNION` operations must all be either `UNION ALL` or `UNION DISTINCT`. You can use subqueries to concatenate queries with different DISTINCT attributes.
- The syntax for `UNION` here is different from the `WINDOW UNION` syntax in the [WINDOW clause](./WINDOW_CLAUSE.md).

## Boundary Descriptions

| SELECT Statement Element | Offline Mode | Online Preview Mode | Online Request Mode | Description |
| :------------------------ | ------------ | -------------------- | ------------------- | :---------- |
| SET Operation             | **``✓``**     | **``✓``**            | **``✓``**           |             |

- Online mode only supports `UNION ALL`, while offline mode supports both `UNION ALL` and `UNION DISTINCT`.


## Example

```sql
SELECT * FROM t1
UNION ALL
SELECT * FROM t2
UNION ALL
SELECT * FROM t3

-- differnt DISTINCT field
SELECT * FROM t1
UNION ALL
(
    SELECT * FROM t2
    UNION DISTINCT
    SELECT * FROM t3
)
```

