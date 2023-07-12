# SET STATEMENT

`SET` 语句用于在 OpenMLDB 上设置系统变量。目前OpenMLDB的系统变量包括会话系统变量和全局系统变量。对会话变量的修改，只会影响到当前的会话（也就是当前的数据库连接）。对全局变量的修改会对所有会话生效。


## Syntax

```sql
SetStatement ::=
    'SET' variableName '=' value

variableName ::=
	sessionVariableName
	
sessionVariableName ::= '@@'Identifier | '@@session.'Identifier | '@@global.'Identifier
```
或者用下面的语法格式
```sql
'SET' [ GLOBAL | SESSION ] <variableName> '=' <value>
```

- 会话系统变量一般以`@session前缀`，如`SET @@session.execute_mode = "offline";`。会话系统变量也可以选择直接以`@@`为前缀，即`SET @@execute_mode = "offline"`和前面的配置语句是等价的。
- 全局系统变量以`@global为前缀`，如`SET @@global.enable_trace = true;` 
- 会话系统变量也可以选择直接以`@@`为前缀，即`SET @@execute_mode = "offline"`和前面的配置语句是等价的。
- OpenMLDB的SET语句只能用于设置/修改已存在（内置的）的系统变量。


## 目前支持的系统变量

### SESSION 系统变量

| SESSION系统变量                        | 变量描述                                                                                                          | 变量值                | 默认值    |
| -------------------------------------- |---------------------------------------------------------------------------------------------------------------| --------------------- | --------- |
| @@session.execute_mode｜@@execute_mode | OpenMDLB在当前会话下的执行模式。目前支持`offline`和`online`两种模式。<br />在离线执行模式下，只会导入/插入以及查询离线数据。<br />在在线执行模式下，只会导入/插入以及查询在线数据。 | "offline" \| "online" | "offline" |
| @@session.enable_trace｜@@enable_trace | 当该变量值为 `true`，SQL语句有语法错误或者在计划生成过程发生错误时，会打印错误信息栈。<br />当该变量值为 `false`，SQL语句有语法错误或者在计划生成过程发生错误时，仅打印基本错误信息。      | "true" \| "false"     | "false"   |
| @@session.sync_job｜@@sync_job | 当该变量值为 `true`，离线的命令将变为同步，等待执行的最终结果。<br />当该变量值为 `false`，离线的命令即时返回，若要查看命令的执行情况，请使用`SHOW JOB`。                  | "true" \| "false"     | "false"   |
| @@session.job_timeout｜@@job_timeout | 可配置离线异步命令或离线管理命令的等待时间（以*毫秒*为单位），将立即返回。离线异步命令返回后仍可通过`SHOW JOB`查看命令执行情况。                             | Int | "20000" |

## Example

### 设置和显示会话系统变量

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
### 设置和显示全局系统变量
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

### 配置enable_trace

- 创建一个数据库`db1`，并建立表t1

```sql
CREATE DATABASE db1;
-- SUCCEED
USE db1;
-- SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
--SUCCEED
```

- 关闭enable_trace时，执行错误的SQL：

```sql
> set @@enable_trace = "false";
-- SUCCEED    
> select sum(col1) over w1 from t1 window w1 as (partition by col1 order by col0 rows_range between 10d preceding and current row);
-- ERROR: Invalid Order column type : kVarchar
```

- 打开enable_trace时，执行错误的SQL：

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

### 离线命令配置详情

- 设置离线命令同步执行，同步的超时时间将自动设置为30min（同时也是与TaskManager的最大超时时间，仅手动设置更大的此超时时间是无意义的）：

```sql
> SET @@sync_job = "true";
```

```{caution}
如果离线同步命令执行时间超过30min，需要同时调整TaskManager配置和客户端的配置。
- 调大TaskManager的`server.channel_keep_alive_time`
- 配置客户端`--sync_job_timeout`，不可大于`server.channel_keep_alive_time`。SDK暂不支持修改。
```

- 设置离线异步命令或离线管理命令的等待时间，单位为毫秒，默认为1min：
```sql
> SET @@job_timeout = "600000";
```

## 相关SQL语句

[SHOW VARIABLES](../ddl/SHOW_VARIABLES_STATEMENT.md)

