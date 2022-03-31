# 删除 DEPLOYMENT 

```SQL
DROP DEPLOYMENT deployment_name
```

`DROP DEPLOYMENT`语句用于删除一个OnlineServing的部署。

## Example:

创建一个数据库，并设置为当前数据库:

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully
USE db1;
-- SUCCEED: Database changed
```

创建一张表`t1`:

```
CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully

```

部署表t1的查询语句到OnlineServing:

```sql
> DEPLOY demo_deploy select col0 from t1;
SUCCEED: deploy successfully
```

查看当前数据库下所有的deployments:

```sql
SHOW DEPLOYMENTS;
 ----- ------------- 
  DB    Deployment   
 ----- ------------- 
  db1   demo_deploy  
 ----- ------------- 
1 row in set

```

删除指定的deployment:

```sql
DROP DEPLOYMENT demo_deploy;
-- Drop deployment demo_deploy? yes/no
-- yes
-- SUCCEED: Drop successfully

```

删除后，再次查看数据库下的deployments，应为是空列表：

```sql
SHOW DEPLOYMENTS;
Empty set
```



