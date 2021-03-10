# DML(Data Manipulation Language)

SQL supports the following statements for manipulating data:

- `INSERT`
- `UPDATE` （Planing）
- `DELETE` （Planing）
- `MERGE`（Planing）

## INSERT

### syntax

```
INSERT INFO tbl_name (column_list) VALUES (value_list)

column_list:
    col_name [, col_name] ...

value_list:
    value [, value] ...
```

### Examples

```SQL
-- insert into table with all columns
INSERT INTO t1 values(1, 2, 3.0, 4.0, "hello");

-- insert into table with given columns's values
INSERT INTO t1(COL1, COL2, COL5) values(1, 2, "hello")
```

