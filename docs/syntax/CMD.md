## FeSQL CMD 



#### 创建数据库

```mysql
CREATE DATABASE db_name
```

#### 进入数据库

```MYSQL
USE db_name;
```

#### 查看所有数据库列表信息

```mysql
 SHOW DATABASES;
```

#### 查看当前数据库下表信息

```mysql
SHOW TABLES;
```

#### 从文件创建schema(待定)

```mysql
CREATE TABLE schema_file_path
```

#### 查看表schema

```mysql
DESC table_name
+---------+---------+------+
| Field   | Type    | Null |
+---------+---------+------+
| column1 | kInt32  | NO   |
| col2    | kString | NO   |
| col3    | kFloat  | NO   |
+---------+---------+------+
```

