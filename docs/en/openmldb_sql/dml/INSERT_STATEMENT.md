# INSERT

OpenMLDB supports single-row and multi-row insert statements.

## Syntax

```
INSERT INFO tbl_name (column_list) VALUES (value_list) [, value_list ...]

column_list:
    col_name [, col_name] ...

value_list:
    value [, value] ...
```

**Description**
- `INSERT` statement only works in online execute mode

## Examples

```SQL
-- insert a row into table with all columns
INSERT INTO t1 values(1, 2, 3.0, 4.0, "hello");

-- insert a row into table with given columns's values
INSERT INTO t1(COL1, COL2, COL5) values(1, 2, "hello");

-- insert multiple rows into table with all columns
INSERT INTO t1 values(1, 2, 3.0, 4.0, "hello"), (10, 20, 30.0, 40.0, "world");

-- insert multiple rows into table with given columns's values
INSERT INTO t1(COL1, COL2, COL5) values(1, 2, "hello"), (10, 20, "world");
```

