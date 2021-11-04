# Introduction to OpenMLDB's Performance-Sensitive Mode

## Concept

OpenMLDB supports execution under different levels of performance sensitivity.
When `performance_sensitive` mode is enabled, complex SQL queries can't be executed without index-optimization.
When `performance_sensitive` mode is disabled, most SQL queries can be executed with no restrictions.

For instance:
Table `t1` has been created with index `(key = col2, ts = col4)`:
```sqlite
CREATE table t1(
    col1 string,
    col2 string,
    col3 double,
    col4 timestamp,
    index(key = col2, ts = col4));
```
### Case 1: the following SQL
```sql
SELECT col1, col2, SUM(col3) over w1 as w1_sum_col3 from t1 
window w1 as (PARTITION BY col1 ORDER BY col4 ROWS BETWEEN 100 PRECEDING AND CURRENT ROW);
```
- can't be executed when we enable `performance_sensitive` mode, since the SQL fail to be optimized.
- can be executed when we disable `performance_sensitive` mode.

### Case 2: the following SQL
```sql
-- 
SELECT col1, col2, SUM(col3) over w2 as w2_sum_col3 from t1
                                                             window w2 as (PARTITION BY col2 ORDER BY col4 ROWS 
BETWEEN 100 PRECEDING AND CURRENT ROW);
```
- can be executed when we enable `performance_sensitive` mode, since the SQL can be optimized.
- can be executed when we disable `performance_sensitive` mode.


## Usage

### Default config
- OpenMLDB online: turn on performance_sensitive by default.
- OpenMLDB CLI: turn on performance_sensitive by default.
- OpenMLDB-batch: turn off performance_sensitive by default.

### Config via CLI
CLI provides the command to enable/disable `performance_sensitive` mode
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
### Future work
Now, we only support to config `performance_sensitive` via CLI command.
We will support configuration via SDK(JAVA/PYTHON) in the future.
