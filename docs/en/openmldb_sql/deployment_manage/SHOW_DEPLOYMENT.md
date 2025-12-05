# View DEPLOYMENT Details

The `SHOW DEPLOYMENT` statement is used to display the detail of a specific task that has been deployed under Online Request mode.


```SQL
SHOW DEPLOYMENT deployment_name;
```


## Example

Create a database and set it as the current database:

```sql
CREATE DATABASE db1;
-- SUCCEED

USE db1;
-- SUCCEED: Database changed


```

Create a table `t1`:

```sql
CREATE TABLE t1(col0 STRING);
-- SUCCEED

```

Deploy the query statement of table t1:

```sql
DEPLOY demo_deploy select col0 from t1;
-- SUCCEED
```

Check out the newly deployed deployment:

```sql
SHOW DEPLOYMENT demo_deploy;

 ----- ------------- 
  DB    Deployment   
 ----- ------------- 
  db1   demo_deploy  
 ----- ------------- 
 1 row in set
 
 ---------------------------------------------------------------------------------- 
  SQL                                                                               
 ---------------------------------------------------------------------------------- 
  CREATE PROCEDURE deme_deploy (col0 varchar) BEGIN SELECT
  col0
FROM
  t1
; END;  
 ---------------------------------------------------------------------------------- 
1 row in set

# Input Schema
 --- ------- ---------- ------------ 
  #   Field   Type       IsConstant  
 --- ------- ---------- ------------ 
  1   col0    Varchar   NO          
 --- ------- ---------- ------------ 

# Output Schema
 --- ------- ---------- ------------ 
  #   Field   Type       IsConstant  
 --- ------- ---------- ------------ 
  1   col0    Varchar   NO          
 --- ------- ---------- ------------
```

## Related Statements

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[DEPLOY ](../deployment_manage/DEPLOY_STATEMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)

