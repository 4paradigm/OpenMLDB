# SHOW VARIABLES
SHOW VARIABLES 语句用于查看系统变量。其中：
- `SHOW SESSION VARIABLES`或`SHOW VARIABLES`语句用于显示当前会话的系统变量。
- `SHOW GLOBAL VARIABLES`可用于查看全局系统变量。
目前OpenMLDB只支持会话系统变量和全局系统变量，不支持用户变量。对会话变量的修改，只影响当前的会话（也就是当前的数据库连接）。因此，当关闭数据库连接（或者退出控制台）后,再重新连接（或者重新登陆控制台），先前对会话变量的配置和修改都将被重置。

## Syntax

```sql
ShowVariablesStmt ::=
	ShowSessionVariablesStmt | ShowGlobalVariablesStmt

ShowSessionVariablesStmt ::= 
	'SHOW' 'VARIABLES'
	|'SHOW' 'SESSION' 'VARIABLES'
ShowGlobalVariablesStmt ::=
    'SHOW' 'GLOBAL' 'VARIABLES'
```




## Example

```sql
> SHOW SESSION VARIABLES;
 --------------- ---------
  Variable_name   Value
 --------------- ---------
  enable_trace    false
  execute_mode    offline
  job_timeout     20000
  sync_job        false
 --------------- ---------

4 rows in set
 
      
> SET @@enable_trace = "true"
 --SUCCEED
> SHOW VARIABLES;
 --------------- ---------
  Variable_name   Value
 --------------- ---------
  enable_trace    true
  execute_mode    offline
  job_timeout     20000
  sync_job        false
 --------------- ---------

4 rows in set
   
      
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
```

退出控制台后，重新登录控制台。

```sql
> SHOW SESSION VARIABLES;
 --------------- ---------
  Variable_name   Value
 --------------- ---------
  enable_trace    false
  execute_mode    offline
  job_timeout     20000
  sync_job        false
 --------------- ---------

4 rows in set

      
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
```



## 相关SQL语句

[SET VARIABLE](../ddl/SET_STATEMENT.md)

