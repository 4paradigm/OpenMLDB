# DDL(Data Definition Language)

## Database


### create database
```
CREATE DATABASE db_name
```


### drop database
```
DROP DATABASE db_name
```

## Table

### create table

```
CREATE TABLE [IF NOT EXISTS] tbl_name
    (create_definition,...)
    [table_options]
  
table_options:
    table_option [[,] table_option] ...
table_option: {
    REPLICANUM=replica_num
    | DISTRIBUTION(ROLETYPE=endpoint [[,] ROLETYPE=endpoint]...)
}
ROLETYPE: {
    LEADER | FOLLOWER
}
```


### drop table
```
DROP TABLE table_name
```

## Procedure

### create procedure

> - proc_parameter are decided by schema
> - CONST identify whether a column is const or not

```
CREATE PROCEDURE sp_name(proc_parameter[,...])
BEGIN
　　select_stmt;
END
 
proc_parameter:
    [CONST] param_name type
type:
    Any valid FESQL data type
select_stmt:
    Valid select sql statement
```


### drop procedure
```
DROP procedure sp_name
```
