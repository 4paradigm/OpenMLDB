# SET STATEMENT

## Syntax

```sql
SetStatement ::=
    'SET' variableName '=' value

variableName ::=
		|	sessionVariableName
	
sessionVariableName ::= '@@'Identifier | '@@session.'Identifier

```

**Description**

`SET` 语句用于在 OpenMLDB 上设置系统变量。目前OpenMLDB的系统变量仅支持会话系统变量。对会话变量的修改，只会影响到当前的会话（也就是当前的数据库连接）。

- 会话系统变量一般以`@session前缀，形如`SET @@session.execute_mode = "offline"。`注意⚠️：会话系统变量也可以选择直接以`@@`为前缀，即`SET @@execute_mode = "offline"`和前面的配置语句是等价的。变量名是大小写不敏感的。
- OpenMLDB的SET语句只能用于设置/修改已存在（内置的）的系统变量。

## 目前支持的系统变量

### SESSION 系统变量

| SESSION系统变量                        | 变量描述                                                     | 变量值                | 默认值    |
| -------------------------------------- | ------------------------------------------------------------ | --------------------- | --------- |
| @@session.execute_mode｜@@execute_mode | OpenMDLB在当前会话下的执行模式。目前支持"offline"和"online"两种模式。<br />在离线执行模式下，只会导入/插入以及查询离线数据。<br />在在线执行模式下，只会导入/插入以及查询在线数据。 | "offline" \| "online" | "offline" |
| @@session.enable_trace｜@@enable_trace | 控制台的错误信息trace开关。<br />当开关打开时(`SET @@enable_trace = "true"`)，SQL语句有语法错误或者在计划生成过程发生错误时，会打印错误信息栈。<br />当开关关闭时(`SET @@enable_trace = "false"`)，SQL语句有语法错误或者在计划生成过程发生错误时，仅打印基本错误信息。 | "true" \| "false"     | "false"   |
| @@session.sync_job｜@@sync_job | ...开关。<br />当开关打开时(`SET @@sync_job = "true"`)，离线的命令将变为同步，等待执行的最终结果。<br />当开关关闭时(`SET @@sync_job = "false"`)，离线的命令即时返回，需要通过`SHOW JOB`查看命令执行情况。 | "true" \| "false"     | "false"   |
| @@session.sync_timeout｜@@sync_timeout | ...<br />离线命令同步开启的情况下，可配置同步命令的等待时间。超时将立即返回，超时返回后仍可通过`SHOW JOB`查看命令执行情况。 | Int | "20000" |

## Example

### 设置和显示系统变量

```sql
> SHOW VARIABLES;
 --------------- --------
  Variable_name   Value
 --------------- --------
  enable_trace    false
  execute_mode    online
 --------------- --------
> SET @@session.execute_mode = "offline";
> SHOW VARIABLES;
 --------------- --------
  Variable_name   Value
 --------------- --------
  enable_trace    false
  execute_mode    offline
 --------------- --------
 
> SET @@session.enable_trace = "true";
> SHOW VARIABLES;
 --------------- --------
  Variable_name   Value
 --------------- --------
  enable_trace    true
  execute_mode    offline
 --------------- --------
```

### 配置enable_trace

- 创建一个数据库`db1`，并建立表t1

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully
USE db1;
-- SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
--SUCCEED: Create successfully

```

- 关闭enable_trace时，执行错误的SQL：

```sql
> set @@enable_trace = "false";
> select sum(col1) over w1 from t1 window w1 as (partition by col1 order by col0 rows_range between 10d preceding and current row);
-- ERROR: Invalid Order column type : kVarchar
```

- 打开enable_trace时，执行错误的SQL：

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

### 配置离线命令同步执行

- 设置离线命令同步执行：

```sql
> SET @@sync_job = "true";
```

- 设置同步命令的等待时间(单位为毫秒)：
```sql
> SET @@job_timeout = "600000";
```


## 相关SQL语句

[SHOW VARIABLES](../ddl/SHOW_VARIABLES_STATEMENT.md)

