# SHOW DATABASES

```sql
SHOW DATABASES;
```

The `SHOW DATABASES` statement is used to display a list of databases that the current user has access to.

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

