# SHOW VARIABLES
`SHOW VARIABLES` is used to view system variables.
- The `SHOW SESSION VARIABLES` or `SHOW VARIABLES` statement can display system variables of the **current session**.
- `SHOW GLOBAL VARIABLES` is used to display the **global** system variables

Currently, OpenMLDB only supports session system variables and global system variables but doesn't support user variables. Modifications to session variables will only affect the current session (that is, the current database connection). Therefore, when you close the connection (or exit the console), and then reconnect (or log in to the console again), the previous configuration and modification of session variables will be reset.

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

After exiting the console, login again into the console and check the variables again.

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



## Relevant SQL Statements

[SET VARIABLE](../ddl/SET_STATEMENT.md)

