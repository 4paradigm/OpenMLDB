# SET STATEMENT

## Syntax

```sql
SetStatement ::=
    'SET' variableName '=' value

variableName ::=
		|	sessionVariableName
	
sessionVariableName ::= '@@'Identifier | '@@session.'Identifier | '@@global.'Identifier
```
in the following way
```sql
'SET' [ GLOBAL | SESSION ] <variableName> '=' <value>
```
**Description**

The `SET` statement is used to set system variables on OpenMLDB. At present, the system variables of OpenMLDB include session system variables and global system variables. Modifications to session variables will only affect the current session (that is, the current database connection). Modifications to global variables take effect for all sessions.

- Session system variables are usually prefixed with `@session`, such as SET @@session.execute_mode = "offline". `Note⚠️: Session system variables can also be optionally prefixed with `@@` directly, that is, `SET @@execute_mode = "offline"` is equivalent to the previous configuration statement. Variable names are case-insensitive.
- Global system variables are prefixed with `@global`, such as SET @@global.enable_trace = true;
- OpenMLDB's SET statement can only be used to set/modify existing (built-in) system variables.

## Currently Supported System Variables


### SESSION System Variable

| SESSION System Variable                        | Variable Description                                                     | Variable Value                | Default Value    |
| -------------------------------------- | ------------------------------------------------------------ | --------------------- | --------- |
| @@session.execute_mode｜@@execute_mode | The execution mode of OpenMDLB in the current session. Currently supports "offline" and "online" two modes.<br />In offline execution mode, only offline data will be imported/inserted and queried.<br />In online execution mode, only online data will be imported/inserted and queried. | "offline" \| "online" | "offline" |
| @@session.enable_trace｜@@enable_trace | Console error message trace switch. <br />When the switch is on (`SET @@enable_trace = "true"`), an error message stack is printed when the SQL statement has a syntax error or an error occurs during the plan generation process. <br />When the switch is off (`SET @@enable_trace = "false"`), the SQL statement has a syntax error or an error occurs during the plan generation process, only the basic error message is printed. | "true" \| "false"     | "false"   |
| @@session.sync_job｜@@sync_job | ...开关。<br />When the switch is on (`SET @@sync_job = "true"`), the offline command will become synchronous, waiting for the final result of the execution.<br />When the switch is closed (`SET @@sync_job = "false"`), the offline command returns immediately, and you need to check the command execution through `SHOW JOB`. | "true" \| "false"     | "false"   |
| @@session.sync_timeout｜@@sync_timeout | ...<br />When offline command synchronization is enabled, you can configure the waiting time for synchronization commands. The timeout will return immediately. After the timeout returns, you can still view the command execution through `SHOW JOB`. | Int | "20000" |

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
### Set and Display Session System Variables
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

### Configure enable_trace

- Create a database `db1` and create table t1

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully
USE db1;
-- SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
--SUCCEED: Create successfully

```

- When enable_trace is turned off, the wrong SQL is executed:

```sql
> set @@enable_trace = "false";
> select sum(col1) over w1 from t1 window w1 as (partition by col1 order by col0 rows_range between 10d preceding and current row);
-- ERROR: Invalid Order column type : kVarchar
```

- When enable_trace is turned on, the wrong SQL is executed:

```sql
> set @@enable_trace = "true";
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

### Configure Offline Command Synchronous Execution


- Set offline command synchronous execution:

```sql
> SET @@sync_job = "true";
```

- Set the wait time for synchronization commands (in milliseconds):
```sql
> SET @@job_timeout = "600000";
```


## Related SQL Statements

[SHOW VARIABLES](../ddl/SHOW_VARIABLES_STATEMENT.md)

