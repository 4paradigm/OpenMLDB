# Python SDK Quickstart

Notice, The Python SDK currently only supports the cluster version, and the stand-alone version will be planned to be supported in the next version v0.5.0.

## 1. Install the OpenMLDB Python package

Install using `pip`.

```bash
pip install openmldb
````

## 2. Using OpenMLDB DBAPI

### 2.1 Create connection

Here database name is not required to exist. If it does not exist, you need to create the database after the connection is created.

````python
import openmldb.dbapi

db = openmldb.dbapi.connect("db1", "$zkcluster", "$zkpath")

cursor = db.cursor()
````

### 2.2 Create database

````python
cursor.execute("CREATE DATABASE db1")
````

### 2.3 Create table

````python
cursor.execute("CREATE TABLE t1 (col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1))")
````

### 2.4 Insert data to table

````python
cursor.execute("INSERT INTO t1 VALUES(1000, '2020-12-25', 'guangdon', 'shenzhen', 1)")
````

### 2.5 Execute SQL query

````python
result = cursor.execute("SELECT * FROM t1")
print(result.fetchone())
print(result.fetchmany(10))
print(result.fetchall())
````

### 2.6 Delete table

````python
cursor.execute("DROP TABLE t1")
````

### 2.7 Delete database

````python
cursor.execute("DROP DATABASE db1")
````

### 2.8 Close the connection

````python
cursor.close()
````

## 3. Using OpenMLDB SQLAlchemy

### 3.1 Create connection

`create_engine('openmldb:///db_name?zk=zkcluster&zkPath=zkpath')`
Here db_name is not required to exist. If it does not exist, you need to create the database after the connection is created.

````python
import sqlalchemy as db

engine = db.create_engine('openmldb:///db1?zk=127.0.0.1:2181&zkPath=/openmldb')

connection = engine.connect()
````

### 3.2 Create database

Create a database using the `connection.execute()` interface:

````python
try:
    connection.execute("CREATE DATABASE db1");
except Exception as e:
    print(e)
````

### 3.3 Create table

Create a table using the `connection.execute()` interface:

````python
try:
    connection.execute("CREATE TABLE t1 ( col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1))")
except Exception as e:
    print(e)
````

### 3.4 Insert data into the table

Use the `connection.execute(ddl)` interface to execute the SQL insert statement to insert data to the table:

````python
try:
    connection.execute("INSERT INTO t1 VALUES(1000, '2020-12-25', 'guangdon', 'shenzhen', 1);")
except Exception as e:
    print(e)
````

Use the `connection.execute(ddl, data)` interface to execute the insert statement of SQL with the planceholder, and the inserted data can be dynamically specified:

````python
try:
    insert = "INSERT INTO t1 VALUES(1002, '2020-12-27', ?, ?, 3);"
    connection.execute(insert, ({"col3":"fujian", "col4":"fuzhou"}))
except Exception as e:
    print(e)
````

### 3.5 Execute SQL batch query

Use the `connection.execute(sql)` interface to execute SQL batch query statements:

````python
try:
    rs = connection.execute("SELECT * FROM t1");
    for row in rs:
        print(row)
    rs = connection.execute("SELECT * FROM t1 WHERE col3 = ?;", ('hefei'))
except Exception as e:
    print(e)
````

### 3.6 Execute SQL on-demand queries

Use `connection.execute(sql, request)` interface to execute SQL batch query statement: request query, you can put the input data in the second parameter of execute

````python
try:
   rs = connection.execute("SELECT * FROM t1", ({"col1":9999, "col2":'2020-12-27', "col3":'zhejiang', "col4":'hangzhou', " col5":100}));
except Exception as e:
    print(e)
````

### 3.7 Delete table

Use the `connection.execute(ddl)` interface to delete a table:

````python
try:
    connection.execute("DROP TABLE t1")
except Exception as e:
    print(e)
````

### 3.8 Delete database

Use the `connection.execute(ddl)` interface to delete a database:

````python
try:
    connection.execute("DROP DATABASE db1")
except Exception as e:
    print(e)
````

## 4. Using Notebook Magic Function

OpenMLDB Python SDK supports Notebook magic function extension, use the following statement to register the function.

````
import openmldb

db = openmldb.dbapi.connect('demo_db','0.0.0.0:2181','/openmldb')

openmldb.sql_magic.register(db)
````

The line magic function `%sql` and block magic function `%%sql` can then be used in Notebook.

![](https://github.com/4paradigm/openmldb-docs-zh/blob/bbe11c98d0f0b1ae7f9723a6b32c30f8e42e1903/quickstart/images/openmldb_magic_function.png)
