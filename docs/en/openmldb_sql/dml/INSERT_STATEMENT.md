# INSERT

OpenMLDB supports single-row and multi-row insert statements.

## Syntax

```
INSERT [[OR] IGNORE] INFO tbl_name (column_list) VALUES (value_list) [, value_list ...]

column_list:
    col_name [, col_name] ...

value_list:
    value [, value] ...
```

**Description**
- By default, `INSERT` does not deduplicate records, whereas `INSERT OR IGNORE` allows ignoring data that already exists in the table, making it suitable for repeated attempts.
- Offline execute mode only supports `INSERT`, not `INSERT OR IGNORE`.
- `INSERT` statement in offline execute mode is unsupported on tables with symbolic path.In OpenMLDB, tables have two types of offline data addresses: Data path and Symbolic path, as detailed in [Offline Import Rules](./LOAD_DATA_STATEMENT.md#offline-import-rules). `INSERT` data in offline execute mode will be written to Data path and the writing format is in Parquet format. Since the data format of Symbolic path can be set freely, `INSERT` data may cause data format conflicts if the table has symbolic path. Therefore, `INSERT` statement in offline execute mode is unsupported on tables with symbolic paths currently.

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

