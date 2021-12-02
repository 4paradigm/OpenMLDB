# Introduction to OpenMLDB's Performance-Sensitive Mode

## 1. Introduction

OpenMLDB supports execution under different levels of performance sensitivity.
When `performance_sensitive` mode is enabled, SQL queries cannot be executed without corresponding index-optimization. When `performance_sensitive` mode is disabled, SQL queries can be executed with no restrictions.

**Note that, currently the performance-sensitive mode configuration is applicable to the standalone mode only. For the cluster mode, the performance-sensitive mode is always enabled.**

To determine whether a query can be executed with the performance-sensitive mode enabled, it depends on the `index` option when creating tables `CREATE TABLE ...(..., index(key=..., ts=...))`. Specifically, `key` represents the column for indexing, and `ts` represents the ordered column applicable to `ORDER BY`. The below SQL clauses cannot be executed with the mode enabled if the requirements are unsatisfied:

- `PARTITION BY`: it must be the index column specified by `key`
- `ORDER BY`: it must be the ordered column specified by `ts`
- `WHERE`: it must be the index column specified by `key`
- `GROUP BY`: it must be the index column specified by `key`
- `LAST JOIN`: it must be the index column specified by `key`

## 2. Examples

Table `t1` has been created with the index option `(key=col2, ts=col4)`:

```sqlite
CREATE TABLE t1(
    col1 string,
    col2 string,
    col3 double,
    col4 timestamp,
    INDEX(key=col2, ts=col4));
```
**Case 1:** 

```sql
SELECT col1, col2, SUM(col3) OVER w1 AS w1_sum_col3 FROM t1 
    WINDOW w1 AS (PARTITION BY col1 ORDER BY col4 
    ROWS BETWEEN 100 PRECEDING AND CURRENT ROW);
```
- It cannot be executed with the `performance_sensitive` enabled, since the clause `PARTITION BY col1` does not match the index column `col2`.
- It can be executed with the `performance_sensitive` disabled.

**Case 2:** 

```sql
SELECT col1, col2, SUM(col3) OVER w2 AS w2_sum_col3 FROM t1 
    WINDOW w2 AS (PARTITION BY col2 ORDER BY col4 
    ROWS BETWEEN 100 PRECEDING AND CURRENT ROW);
```
- It can be executed with the `performance_sensitive` enabled, since the clause `PARTITION BY col2` and `ORDER BY col4` match the index column `col2` and ordered column `col4` respectively.
- It can be executed with the `performance_sensitive` disabled.


## 3. Mode Configuration (Standalone Mode Only)

### 3.1. Default configuration
- OpenMLDB online: performance_sensitive **enabled** by default
- OpenMLDB CLI: performance_sensitive **enabled** by default
- OpenMLDB-batch: performance_sensitive **disabled** by default

### 3.2. Configuration via CLI
CLI provides the command to enable/disable `performance_sensitive` mode for the standalone mode.
- Using `SET performance_sensitive=true` to enable `performance_sensitive` mode
```sqlite
> CREATE DATABASE db1;
> USE db1;
> SET performance_sensitive=true
```

- Using `SET performance_sensitive=false` to disable `performance_sensitive` mode
```sqlite
> CREATE DATABASE db1;
> USE db1;
> SET performance_sensitive=false
```
### 4. Future Work
We support to config `performance_sensitive` via CLI for the standalone mode only at this moment. We will support configuration via SDK (Java/Python) in the near future.

