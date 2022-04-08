# SHOW DATABASES

```sql
SHOW DATABASES;
```

`SHOW DATABASES` 语句用于显示当前用户有权访问的数据库列表。

## Example

```sql
CREATE DATABASE db1;

SHOW DATABASES;
 ----------- 
  Databases  
 ----------- 
  db1               
 ----------- 

CREATE DATABASE db2;
SHOW DATABASES;
 ----------- 
  Databases  
 ----------- 
  db1        
  db2        
 ----------- 
```

