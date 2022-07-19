# SHOW VARIABLES

```sql
ShowVariablesStmt ::=
	ShowSessionVariablesStmt

ShowSessionVariablesStmt ::= 
													'SHOW' 'VARIABLES'
													|'SHOW' 'SESSION' 'VARIABLES'

```

The `SHOW SESSION VARIABLES` or `SHOW VARIABLES` statement is used to display system variables for the current session.

Currently OpenMLDB only supports session system variables. Modifications to session variables will only affect the current session (that is, the current database connection). Therefore, when you close the database connection (or exit the console), and then reconnect (or log in to the console again), the previous configuration and modification of session variables will be reset.

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

After exiting the console, log back into the console

```sql
> SHOW SESSION VARIABLES;
 --------------- --------
  Variable_name   Value
 --------------- --------
  enable_trace    false
  execute_mode    online
 --------------- --------
```



## Relevant SQL Statements

[SET VARIABLE](../ddl/SET_STATEMENT.md)

