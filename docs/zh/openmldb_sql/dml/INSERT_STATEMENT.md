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
- 离线模式仅支持`INSERT`，不支持`INSERT OR IGNORE`。
- 离线模式`INSERT`不支持写入有软链接的表。OpenMLDB 中，表的离线数据地址分为两类，离线地址和软链接地址，详见[离线导入规则](./LOAD_DATA_STATEMENT.md#离线导入规则)，离线模式`INSERT`的数据会写入离线地址中，写入格式固定为parquet格式。由于软链接的数据格式设置自由，若表存在软链接地址，写入数据可能导致数据格式冲突，因此当前离线模式`INSERT`不支持写入有软链接的表。

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

