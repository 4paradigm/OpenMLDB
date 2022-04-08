# SHOW VARIABLES

```sql
ShowVariablesStmt ::=
	ShowSessionVariablesStmt

ShowSessionVariablesStmt ::= 
													'SHOW' 'VARIABLES'
													|'SHOW' 'SESSION' 'VARIABLES'

```

`SHOW SESSION VARIABLES`或`SHOW VARIABLES`语句用于显示当前会话的系统变量。

目前OpenMLDB只支持会话系统变量。对会话变量的修改，只会影响到当前的会话（也就是当前的数据库连接）。因此，当关闭数据库连接（或者退出控制台）后,再重新连接（或者重新登陆控制台），先前对会话变量的配置和修改都将被重置。

## Example

```sql
> SHOW SESSION VARIABLES;
 --------------- --------
  Variable_name   Value
 --------------- --------
  enable_trace    false
  execute_mode    online
 --------------- --------
 
> SET @@enable_trace = "true"

> SHOW VARIABLES;
 --------------- --------
  Variable_name   Value
 --------------- --------
  enable_trace    true
  execute_mode    online
 --------------- --------
```

退出控制台后，重新登录控制台

```sql
> SHOW SESSION VARIABLES;
 --------------- --------
  Variable_name   Value
 --------------- --------
  enable_trace    false
  execute_mode    online
 --------------- --------
```



## 相关SQL语句

[SET VARIABLE](../ddl/SET_STATEMENT.md)

