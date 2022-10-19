# 删除 DEPLOYMENT 

`DROP DEPLOYMENT`语句用于删除一个在线请求模式下的部署。

```SQL
DROP DEPLOYMENT deployment_name
```


## Example:

创建一个数据库，并设置为当前数据库:

```sql
CREATE DATABASE db1;
-- SUCCEED
USE db1;
-- SUCCEED: Database changed
```

创建一张表`t1`:

```sql
CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully
```

在线请求模式下，部署表t1的查询语句:

```sql
DEPLOY demo_deploy select col0 from t1;
-- SUCCEED
```

查看当前数据库下所有的 deployments:

```sql
SHOW DEPLOYMENTS;
 ----- ------------- 
  DB    Deployment   
 ----- ------------- 
  db1   demo_deploy  
 ----- ------------- 
1 row in set

```

删除指定的 deployment:

```sql
DROP DEPLOYMENT demo_deploy;
-- Drop deployment demo_deploy? yes/no
-- yes
-- SUCCEED
```

删除后，再次查看数据库下的 deployments，应为空列表：

```sql
SHOW DEPLOYMENTS;
 ---- ------------
  DB   Deployment
 ---- ------------

0 rows in set
```



