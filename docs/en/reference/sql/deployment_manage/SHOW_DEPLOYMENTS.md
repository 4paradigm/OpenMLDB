# View DEPLOYMENTS List

The `SHOW DEPLOYMENTS` statement displays the tasks that have been deployed in the current database under Online Request mode.


```SQL
SHOW DEPLOYMENTS;
```


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

View all deployments in the current database:

```sql
SHOW DEPLOYMENTS;
 ----- ------------- 
  DB    Deployment   
 ----- ------------- 
  db1   demo_deploy  
 ----- ------------- 
1 row in set

```

## Related Statements

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[DEPLOY ](../deployment_manage/DEPLOY_STATEMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)

