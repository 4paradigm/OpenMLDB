# View DEPLOYMENT Details

```SQL
SHOW DEPLOYMENT deployment_name;
```

The `SHOW DEPLOYMENT` statement is used to display the details of an OnlineServing.

## Example

Create a database and set it as the current database:

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

USE db1;
-- SUCCEED: Database changed


```

Create a table `t1`:

```sql
CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully

```

Deploy the query statement of table t1 to OnlineServing:

```sql
DEPLOY demo_deploy select col0 from t1;
-- SUCCEED: deploy successfully
```

Check out the newly deployed deployment:

```sql
SHOW DEPLOYMENT demo_deploy;
```

```
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
  1   col0    kVarchar   NO          
 --- ------- ---------- ------------ 

# Output Schema
 --- ------- ---------- ------------ 
  #   Field   Type       IsConstant  
 --- ------- ---------- ------------ 
  1   col0    kVarchar   NO          
 --- ------- ---------- ------------ 

```

## Related Statements

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[DEPLOY ](../deployment_manage/DEPLOY_STATEMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)

