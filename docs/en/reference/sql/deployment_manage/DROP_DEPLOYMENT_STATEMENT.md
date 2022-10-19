# Delete DEPLOYMENT
The `DROP DEPLOYMENT` statement is used to drop a deployment under Online Request mode.

```SQL
DROP DEPLOYMENT deployment_name
```


## Example:

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

Deploy the query statement of table t1 under Online Request mode:

```sql
> DEPLOY demo_deploy select col0 from t1;
SUCCEED
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
-- SUCCEED

```

After deletion, check the deployments under the database again, it should be an empty list:

```sql
SHOW DEPLOYMENTS;
 ---- ------------
  DB   Deployment
 ---- ------------

0 rows in set
```



