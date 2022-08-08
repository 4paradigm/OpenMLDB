# Delete DEPLOYMENT

```SQL
DROP DEPLOYMENT deployment_name
```

The `DROP DEPLOYMENT` statement is used to drop an OnlineServing deployment.

## Example:

Create a database and set it as the current database:

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully
USE db1;
-- SUCCEED: Database changed
```

Create a table `t1`:

```
CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully

```

Deploy the query statement of table t1 to OnlineServing:

```sql
> DEPLOY demo_deploy select col0 from t1;
SUCCEED: deploy successfully
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

Delete the specified deployment:

```sql
DROP DEPLOYMENT demo_deploy;
-- Drop deployment demo_deploy? yes/no
-- yes
-- SUCCEED: Drop successfully

```

After deletion, check the deployments under the database again, it should be an empty list:

```sql
SHOW DEPLOYMENTS;
Empty set
```



