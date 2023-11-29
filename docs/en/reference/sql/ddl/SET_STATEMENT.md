# SET STATEMENT
The `SET` statement is used to set system variables of OpenMLDB. At present, the system variables of OpenMLDB include session system variables and global system variables. Modifications to session variables will only affect the current session (that is, the current database connection). Modifications to global variables take effect for all sessions.

## Syntax

```sql
SetStatement ::=
    'SET' variableName '=' value

variableName ::=
	sessionVariableName
	
sessionVariableName ::= '@@'Identifier | '@@session.'Identifier | '@@global.'Identifier
```
The following format is also equivalent.
```sql
'SET' [ GLOBAL | SESSION ] <variableName> '=' <value>
```


- Session system variables are usually prefixed with `@session`, such as SET @@session.execute_mode = "offline". Session system variables can also be optionally prefixed with `@@` directly, that is, `SET @@execute_mode = "offline"` is equivalent to the previous configuration statement. 
- Global system variables are prefixed with `@global`, such as `SET @@global.enable_trace = true;`
- `SET STATEMENT` can only be used to set/modify existing (built-in) system variables.
- Variable names are case-insensitive.

## Currently Supported System Variables


### SESSION System Variable

| SESSION System Variable                        | Note                                                                                                                                                                                                                                                                                                                                  | Variable Value              | Default Value |
| -------------------------------------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------| ----- |
| @@session.execute_mode｜@@execute_mode | The execution mode of OpenMDLB in the current session. Currently supports `offline` and `online` two modes.<br />In offline execution mode, only offline data will be imported/inserted and queried.<br />In online execution mode, only online data will be imported/inserted and queried.                                           | `offline`, <br /> `online"` | `offline` |
| @@session.enable_trace｜@@enable_trace | When the value is `true`, an error message stack will be printed when the SQL statement has a syntax error or an error occurs during the plan generation process. <br />When the value is `false`, only the basic error message will be printed if there is a SQL syntax error or an error occurs during the plan generation process. | `true`, <br /> `false`      | `false` |
| @@session.sync_job｜@@sync_job | When the value is `true`, the offline command will be executed synchronously, waiting for the final result of the execution.<br />When the value is `false`, the offline command returns immediately. If you need to check the execution, please use `SHOW JOB` command.                                                              | `true`, <br /> `false`      | `false` |
| @@session.sync_timeout｜@@sync_timeout | When `sync_job=true`, you can configure the waiting time for synchronization commands. The timeout will return immediately. After the timeout returns, you can still view the command execution through `SHOW JOB`.                                                                                                                   | Int                         | 20000 |
| @@session.spark_config｜@@spark_config | Set the Spark configuration for offline jobs, configure like 'spark.executor.memory=2g;spark.executor.cores=2'. Notice that the priority of this Spark configuration is higer than TaskManager Spark configuration but lower than CLI Spark configuration file.                                                                                                                    | String                         | "" |

## Example

### Set and Display Session System Variables

```sql
> SHOW VARIABLES;
 --------------- ---------
  Variable_name   Value
 --------------- ---------
  enable_trace    false
  execute_mode    offline
  job_timeout     20000
  sync_job        false
 --------------- ---------

4 rows in set
> SET @@session.execute_mode = "online";
-- SUCCEED
> SHOW VARIABLES;
 --------------- ---------
  Variable_name   Value
 --------------- ---------
  enable_trace    false
  execute_mode    online
  job_timeout     20000
  sync_job        false
 --------------- ---------

4 rows in set
> SET @@session.enable_trace = "true";
 -- SUCCEED
> SHOW VARIABLES;
  --------------- ---------
  Variable_name   Value
 --------------- ---------
  enable_trace    true
  execute_mode    online
  job_timeout     20000
  sync_job        false
 --------------- ---------

4 rows in set
```


### Set and Display Global System Variables
```sql
> SHOW GLOBAL VARIABLES;
 --------------- ----------------
  Variable_name   Variable_value
 --------------- ----------------
  enable_trace    false
  sync_job        false
  job_timeout     20000
  execute_mode    offline
 --------------- ----------------

4 rows in set
> SET @@global.enable_trace = "true";
-- SUCCEED
> SHOW GLOBAL VARIABLES;
 --------------- ----------------
  Variable_name   Variable_value
 --------------- ----------------
  enable_trace    true
  sync_job        false
  job_timeout     20000
  execute_mode    offline
 --------------- ----------------

4 rows in set
```

### Configure `enable_trace`

- Create a database `db1` and create table `t1`.

```sql
CREATE DATABASE db1;
-- SUCCEED
USE db1;
-- SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
--SUCCEED
```

- When `enable_trace` is `false`, executing an invalid SQL will generate the following information.

```sql
> set @@enable_trace = "false";
-- SUCCEED    
> select sum(col1) over w1 from t1 window w1 as (partition by col1 order by col0 rows_range between 10d preceding and current row);
-- ERROR: Invalid Order column type : kVarchar
```

- When `enable_trace` is `true`, executing an invalid SQL will generate the following information.

```sql
> set @@enable_trace = "true";
-- SUCCEED
> select sum(col1) over w1 from t1 window w1 as (partition by col1 order by col0 rows_range between 10d preceding and current row);
-- ERROR: Invalid Order column type : kVarchar
    (At /Users/chenjing/work/chenjing/OpenMLDB/hybridse/src/vm/sql_compiler.cc:263)
    (At /Users/chenjing/work/chenjing/OpenMLDB/hybridse/src/vm/sql_compiler.cc:166)
    (Caused by) Fail to generate physical plan batch mode
    (At /Users/chenjing/work/chenjing/OpenMLDB/hybridse/src/vm/transform.cc:1672)
    (Caused by) Fail to transform query statement
    (At /Users/chenjing/work/chenjing/OpenMLDB/hybridse/src/vm/transform.cc:103)
    (At /Users/chenjing/work/chenjing/OpenMLDB/hybridse/src/vm/transform.cc:1249)
    (At /Users/chenjing/work/chenjing/OpenMLDB/hybridse/src/vm/transform.cc:1997)
```

### Offline Commands Configuration Details

- Set the synchronous execution for offline commands:

```sql
> SET @@sync_job = "true";
```

```{caution}
If offline sync job is longer than 30min(the default timeout for offline sync job), you should change the config of TaskManager and client.
- set `server.channel_keep_alive_time` bigger in TaskManager config file.
- choose one:
    - set a bigger session job_timeout, we'll use `max(session_job_timeout, default_gflag_sync_job_timeout)`.
    - set `--sync_job_timeout` of sql client, less than `server.channel_keep_alive_time`. SDK can't change the config now.
```

- Set the waiting time for offline async commands or offline admin commands (in milliseconds):
```sql
> SET @@job_timeout = "600000";
```


## Related SQL Statements

[SHOW VARIABLES](../ddl/SHOW_VARIABLES_STATEMENT.md)

