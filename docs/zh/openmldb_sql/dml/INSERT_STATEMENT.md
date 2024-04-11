# INSERT

OpenMLDB 支持一次插入单行或多行数据。

## syntax

```
INSERT [[OR] IGNORE] INTO tbl_name (column_list) VALUES (value_list) [, value_list ...]

column_list:
    col_name [, col_name] ...

value_list:
    value [, value] ...
```

**说明**
- 默认`INSERT`不会去重，`INSERT OR IGNORE` 则可以忽略已存在于表中的数据，可以反复重试。
- 离线模式仅支持`INSERT`，不支持`INSERT OR IGNORE`

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

