# DDL(Data Definition Language, 数据定义语言)

- database
    - [create database](#create-database) 创建database
    - [drop database](#drop-database) 删除database
- table 
    - [create table](#create-table) 创建表
    - [drop table](#drop-table) 删除表
- procedure
    - [create procedure](#create-procedure) 创建存储过程
    - [drop procedure](#drop-procedure) 删除存储过程

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

> - proc_parameter由主表对应的schema决定
> - CONST用来标识是不是公共列

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
