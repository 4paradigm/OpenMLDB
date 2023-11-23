# CREATE TABLE
 The `CREATE TABLE` statement is used to create a table. The table name must be unique in one database. 

## Syntax

```sql
CreateTableStmt ::=
    'CREATE' 'TABLE' IfNotExists TableName ( 
      TableElementList CreateTableSelectOpt | LikeTableWithOrWithoutParen ) OnCommitOpt
IfNotExists ::=
    ('IF' 'NOT' 'EXISTS')?
TableName ::=
    Identifier ('.' Identifier)?
    
TableElementList ::=
    TableElement ( ',' TableElement )*
TableElement ::=
    ColumnDef | ColumnIndex
```

The `TableElementList` needs to be defined in the `CREATE TABLE` statement. `TableElementList` consists of `ColumnDef` (column definition) and `ColumnIndex`. OpenMLDB requires at least one `ColumnDef` in the `TableElementList`.

Or use Hive tables and Parquet files to create new tables.

```sql
CreateTableStmt ::=
    'CREATE' 'TABLE' TableName LIKE LikeType PATH

TableName ::=
    Identifier ('.' Identifier)?

LikeType ::=
    'HIVE' | 'PARQUET'

PATH ::=
    string_literal
```

Here is the known issues of creating tables with Hive.

* May get timeout for the default CLI config and need to show tables to check result.
* The column constraints of Hive tables such as `NOT NULL` will not copy to new tables.

### ColumnDef (required)

```SQL
ColumnDef ::=
    ColumnName ( ColumnType ) [ColumnOptionList]
ColumnName ::=
    Identifier ( '.' Identifier ( '.' Identifier )? )?      
         
ColumnType ::=
						'INT' | 'INT32'
						|'SMALLINT' | 'INT16'
						|'BIGINT' | 'INT64'
						|'FLOAT'
						|'DOUBLE'
						|'TIMESTAMP'
						|'DATE'
						|'BOOL'
						|'STRING' | 'VARCHAR'
						
ColumnOptionList ::= 
    ColumnOption*	
ColumnOption ::= 
    ['DEFAULT' DefaultValueExpr ] ['NOT' 'NULL']
				 	  
DefaultValueExpr ::= 
    int_literal | float_literal | double_literal | string_literal
```

A table contains one or more columns. The column description `ColumnDef` for each column describes the column name, column type, and options.

- `ColumnName`: The name of the column in the table. Column names within the same table must be unique.
- `ColumnType`: The data type of the column. To learn about the data types supported by OpenMLDB, please refer to [Data Types](../data_types/reference.md).
- `ColumnOptionList`:
  - `NOT NULL`: The column does not allow null values.
  - `DEFAULT`: The default value of this column. It is recommended to configure the default value if `NOT NULL` is configured. In this case, when inserting data, if the value of the column is not defined, the default value will be inserted. If the `NOT NULL` attribute is configured but the `DEFAULT` value is not configured, OpenMLDB will throw an error when the change column value is not defined in the INSERT statement.

#### Example

**Example 1: Create a Table**

The following SQL commands set the current database to `db1` and create a table `t1` in the current database, including the column named `col0`. The data type of `col0` is `STRING`.

```sql
CREATE DATABASE db1;
-- SUCCEED
USE db1;
-- SUCCEED: Database changed
CREATE TABLE t1(col0 STRING);
-- SUCCEED

```
The following SQL command shows how to create a table in a database which is not the database currently used.
```sql
CREATE TABLE db1.t2 (col0 STRING, col1 int);
-- SUCCEED
```
Switch to database `db1` to see the details of the table just created.
```sql
USE db1;
-- SUCCEED: Database changed
desc t2;
 --- ------- --------- ------ --------- 
  #   Field   Type      Null   Default  
 --- ------- --------- ------ --------- 
  1   col0    Varchar   YES             
  2   col1    Int       YES             
 --- ------- --------- ------ --------- 
 --- -------------------- ------ ---- ------ --------------- 
  #   name                 keys   ts   ttl    ttl_type       
 --- -------------------- ------ ---- ------ --------------- 
  1   INDEX_0_1639524201   col0   -    0min   kAbsoluteTime  
 --- -------------------- ------ ---- ------ --------------- 
 --------------
  storage_mode
 --------------
  Memory
 --------------
```

**Example 2: Create a Duplicate Table**

The following SQL command creates a table, whose name is the same as an existing table of this database.
```sql
CREATE TABLE t1 (col0 STRING NOT NULL, col1 int);
-- SUCCEED
CREATE TABLE t1 (col0 STRING NOT NULL, col1 int);
-- Error: table already exists
CREATE TABLE t1 (col0 STRING NOT NULL, col1 string);
-- Error: table already exists
```

**Example 3: Create a Table with NOT NULL on Certain Columns**

```sql
USE db1;
-- SUCCEED: Database changed
CREATE TABLE t3 (col0 STRING NOT NULL, col1 int);
-- SUCCEED
```

```sql
desc t3;
 --- ------- --------- ------ ---------
  #   Field   Type      Null   Default
 --- ------- --------- ------ ---------
  1   col0    Varchar   NO
  2   col1    Int       YES
 --- ------- --------- ------ ---------
 --- -------------------- ------ ---- ------ ---------------
  #   name                 keys   ts   ttl    ttl_type
 --- -------------------- ------ ---- ------ ---------------
  1   INDEX_0_1657327434   col0   -    0min   kAbsoluteTime
 --- -------------------- ------ ---- ------ ---------------
 --------------
  storage_mode
 --------------
  Memory
 --------------
```

**Example 4: Create a Table with Default Value**

```sql
USE db1;
-- SUCCEED: Database changed
CREATE TABLE t3 (col0 STRING NOT NULL, col1 int);
-- SUCCEED
```

```sql
desc t3;
 --- ------- --------- ------ ---------
  #   Field   Type      Null   Default
 --- ------- --------- ------ ---------
  1   col0    Varchar   NO
  2   col1    Int       YES
 --- ------- --------- ------ ---------
 --- -------------------- ------ ---- ------ ---------------
  #   name                 keys   ts   ttl    ttl_type
 --- -------------------- ------ ---- ------ ---------------
  1   INDEX_0_1657327434   col0   -    0min   kAbsoluteTime
 --- -------------------- ------ ---- ------ ---------------
 --------------
  storage_mode
 --------------
  Memory
 --------------
```

**Example 5: Create a Table from the Hive table**

At first configure OpenMLDB to support Hive, then create table with the following SQL.

```sql
CREATE TABLE t1 LIKE HIVE 'hive://hive_db.t1';
-- SUCCEED
```

**Example 6: Create a Table from the Parquet files**

```sql
CREATE TABLE t1 LIKE PARQUET 'file://t1.parquet';
-- SUCCEED
```

### ColumnIndex (optional）

```sql
ColumnIndex ::= 
    'INDEX' <OptionalIndexName> '(' IndexOptionList ')' 
 
IndexOptionList ::= 
    IndexOption ( ',' IndexOption )*
   
IndexOption ::= 
    IndexOptionName '=' expr
```

Indexes can be used by database search engines to speed up data retrieval. Simply put, an index is a pointer to the data in a table. Configuring a column index generally requires configuring the index key (`KEY`), index time column (`TS`), `TTL` and `TTL_TYPE`. 
The index key must be configured, and other configuration items are optional. The following table introduces these configuration items in detail.


| Configuration Item        | Note                                                                                                                                                                                                                                                                                                                        | Expression                                                                                      | Example                                                                                                                    |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| `KEY`      | It defines the index column (required). OpenMLDB supports single-column indexes as well as joint indexes. When `KEY`=one column, a single-column index is configured. When `KEY`=multiple columns, the joint index of these columns is configured: several columns are concatenated into a new string as an index in order. | Single-column index: `ColumnName`<br/>Joint index: <br/>`(ColumnName (, ColumnName)* ) `        | Single-column index: `INDEX(KEY=col1)`<br />Joint index: `INDEX(KEY=(col1, col2))`                                         |
| `TS`       | It defines the index time column (optional). Data on the same index will be sorted by the index time column. When `TS` is not explicitly configured, the timestamp of data insertion is used as the index time. The data type of time column should be BigInt or Timestamp                                                                                                             | `ColumnName`                                                                                    | `INDEX(KEY=col1, TS=std_time)`。 The index column is col1, and the data rows with the same col1 value are sorted by std_time. |
| `TTL_TYPE` | It defines the elimination rules (optional). Including four types. When `TTL_TYPE` is not explicitly configured, the `ABSOLUTE` expiration configuration is used by default.                                                                                                                                                | Supported expr: `ABSOLUTE` <br/> `LATEST`<br/>`ABSORLAT`<br/> `ABSANDLAT`。                      | For specific usage, please refer to **Configuration Rules for TTL and TTL_TYP** below.                                     |
| `TTL`      | It defines the maximum survival time/number. Different TTL_TYPEs determines different `TTL` configuration methods. When `TTL` is not explicitly configured, `TTL=0` which means OpenMLDB will not evict records.                                                                                                            | Supported expr: `int_literal`<br/>  `interval_literal`<br/>`( interval_literal , int_literal )` | For specific usage, please refer to "Configuration Rules for TTL and TTL_TYPE" below.                                      |


**Configuration details of TTL and TTL_TYPE**:

| TTL_TYPE    | TTL                                                                                                                                                                                               | Note                                                                                                   | Example                                                                                                                                                       |
| ----------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ABSOLUTE`  | The value of TTL represents the expiration time. The configuration value is a time period such as `100m, 12h, 1d, 365d`. The maximum configurable expiration time is `15768000m` (ie 30 years)    | When a record expires, it is eliminated.                                                               | `INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=100m)`<br />OpenMLDB will delete data older than 100 minutes.                                            |
| `LATEST`    | The value of TTL represents the maximum number of surviving entries. That is, under the same index, the maximum number of data items allowed exists. Up to 1000 can be configured                 | When the record exceeds the maximum number, it will be eliminated.                                     | `INDEX(KEY=col1, TS=std_time, TTL_TYPE=LATEST, TTL=10)`. OpenMLDB will only keep the last 10 records and delete the previous records.                         |
| `ABSORLAT`  | It defines the expiration time and the maximum number of live records. The configuration value is a 2-tuple of the form `(100m, 10), (1d, 1)`. The maximum can be configured `(15768000m, 1000)`. | Eliminates if and only if the record expires** or if the record exceeds the maximum number of records. | `INDEX(key=c1, ts=c6, ttl=(120min, 100), ttl_type=absorlat)`. When the record exceeds 100, **OR** when the record expires, it will be eliminated              |
| `ABSANDLAT` | It defines the expiration time and the maximum number of live records. The configuration value is a 2-tuple of the form `(100m, 10), (1d, 1)`. The maximum can be configured `(15768000m, 1000)`.  | When records expire **OR** records exceed the maximum number of records, records will be eliminated.   | `INDEX(key=c1, ts=c6, ttl=(120min, 100), ttl_type=absandlat)`. When there are more than 100 records, **OR** the records expire, they will also be eliminated. |


#### Example


**Example 1**

The following sql example creates a table with a single-column index.

```sql
USE db1;
--SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1));
-- SUCCEED
desc t1;
 --- ---------- ----------- ------ --------- 
  #   Field      Type        Null   Default  
 --- ---------- ----------- ------ --------- 
  1   col0       Varchar     YES             
  2   col1       Int         YES             
  3   std_time   Timestamp   YES             
 --- ---------- ----------- ------ --------- 
 --- -------------------- ------ ---- ------ --------------- 
  #   name                 keys   ts   ttl    ttl_type       
 --- -------------------- ------ ---- ------ --------------- 
  1   INDEX_0_1639524520   col1   -    0min   kAbsoluteTime  
 --- -------------------- ------ ---- ------ --------------- 
```

**Example 2**

The following sql example creates a table with a joint index.

```sql
USE db1;
--SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=(col0, col1)));
-- SUCCEED
desc t1;
 --- ---------- ----------- ------ --------- 
  #   Field      Type        Null   Default  
 --- ---------- ----------- ------ --------- 
  1   col0       Varchar     YES             
  2   col1       Int         YES             
  3   std_time   Timestamp   YES             
 --- ---------- ----------- ------ --------- 
 --- -------------------- ----------- ---- ------ --------------- 
  #   name                 keys        ts   ttl    ttl_type       
 --- -------------------- ----------- ---- ------ --------------- 
  1   INDEX_0_1639524576   col0|col1   -    0min   kAbsoluteTime  
 --- -------------------- ----------- ---- ------ --------------- 
```

**Example 3**

The following sql example creates a table with a single-column index configuring the time column.


```sql
USE db1;
--SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time));
-- SUCCEED
desc t1;
 --- ---------- ----------- ------ --------- 
  #   Field      Type        Null   Default  
 --- ---------- ----------- ------ --------- 
  1   col0       Varchar     YES             
  2   col1       Int         YES             
  3   std_time   Timestamp   YES             
 --- ---------- ----------- ------ --------- 
 --- -------------------- ------ ---------- ------ --------------- 
  #   name                 keys   ts         ttl    ttl_type       
 --- -------------------- ------ ---------- ------ --------------- 
  1   INDEX_0_1639524645   col0   std_time   0min   kAbsoluteTime  
 --- -------------------- ------ ---------- ------ --------------- 
```

**Example 4**

The following sql example creates a table with a single-column index configuring the time column, TTL_TYPE and TTL.

```sql
USE db1;
--SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
-- SUCCEED
desc t1;
 --- ---------- ----------- ------ --------- 
  #   Field      Type        Null   Default  
 --- ---------- ----------- ------ --------- 
  1   col0       Varchar     YES             
  2   col1       Int         YES             
  3   std_time   Timestamp   YES             
 --- ---------- ----------- ------ --------- 
 --- -------------------- ------ ---------- ---------- --------------- 
  #   name                 keys   ts         ttl        ttl_type       
 --- -------------------- ------ ---------- ---------- --------------- 
  1   INDEX_0_1639524729   col1   std_time   43200min   kAbsoluteTime  
 --- -------------------- ------ ---------- ---------- --------------- 
```

**Example 5** 

The following sql commands create a table with a single-column index and set TTL_TYPE=LATEST.

```sql
USE db1;
--SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=latest, TTL=1));
-- SUCCEED
desc t1;
 --- ---------- ----------- ------ --------- 
  #   Field      Type        Null   Default  
 --- ---------- ----------- ------ --------- 
  1   col0       Varchar     YES             
  2   col1       Int         YES             
  3   std_time   Timestamp   YES             
 --- ---------- ----------- ------ --------- 
 --- -------------------- ------ ---------- ----- ------------- 
  #   name                 keys   ts         ttl   ttl_type     
 --- -------------------- ------ ---------- ----- ------------- 
  1   INDEX_0_1639524802   col1   std_time   1     kLatestTime  
 --- -------------------- ------ ---------- ----- ------------- 
```


**Example 6** 

The following sql commands create a table with a single-column index, set TTL_TYPE=absandlat and configure the maximum number of retained records as 10.


```sql
USE db1;
--SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absandlat, TTL=(30d,10)));
-- SUCCEED
desc t1;
 --- ---------- ----------- ------ --------- 
  #   Field      Type        Null   Default  
 --- ---------- ----------- ------ --------- 
  1   col0       Varchar     YES             
  2   col1       Int         YES             
  3   std_time   Timestamp   YES             
 --- ---------- ----------- ------ --------- 
 --- -------------------- ------ ---------- -------------- ------------ 
  #   name                 keys   ts         ttl            ttl_type    
 --- -------------------- ------ ---------- -------------- ------------ 
  1   INDEX_0_1639525038   col1   std_time   43200min&&10   kAbsAndLat  
 --- -------------------- ------ ---------- -------------- ------------ 
```

**Example 7** 

The following sql commands create a table with a single-column index, set TTL_TYPE=absorlat and configure the maximum number of retained records as 10.
```sql
USE db1;
--SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absorlat, TTL=(30d,10)));
--SUCCEED
desc t1;
 --- ---------- ----------- ------ --------- 
  #   Field      Type        Null   Default  
 --- ---------- ----------- ------ --------- 
  1   col0       Varchar     YES             
  2   col1       Int         YES             
  3   std_time   Timestamp   YES             
 --- ---------- ----------- ------ --------- 
 --- -------------------- ------ ---------- -------------- ----------- 
  #   name                 keys   ts         ttl            ttl_type   
 --- -------------------- ------ ---------- -------------- ----------- 
  1   INDEX_0_1639525079   col1   std_time   43200min||10   kAbsOrLat  
 --- -------------------- ------ ---------- -------------- ----------- 
```

**Example 8** 

The following sql commands create a multi-index table.

```sql
USE db1;
--SUCCEED: Database changed
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col0, TS=std_time), INDEX(KEY=col1, TS=std_time));
--SUCCEED
desc t1;
 --- ---------- ----------- ------ ---------
  #   Field      Type        Null   Default
 --- ---------- ----------- ------ ---------
  1   col0       Varchar     YES
  2   col1       Int         YES
  3   std_time   Timestamp   YES
 --- ---------- ----------- ------ ---------
 --- -------------------- ------ ---------- ------ ---------------
  #   name                 keys   ts         ttl    ttl_type
 --- -------------------- ------ ---------- ------ ---------------
  1   INDEX_0_1648692457   col0   std_time   0min   kAbsoluteTime
  2   INDEX_1_1648692457   col1   std_time   0min   kAbsoluteTime
 --- -------------------- ------ ---------- ------ ---------------
```

### Table Property TableOptions (optional)

```sql
TableOptions
						::= 'OPTIONS' '(' TableOptionItem (',' TableOptionItem)* ')'
TableOptionItem
						::= PartitionNumOption
						    | ReplicaNumOption
						    | DistributeOption
						    | StorageModeOption
								
PartitionNumOption
						::= 'PARTITIONNUM' '=' int_literal
ReplicaNumOption
						::= 'REPLICANUM' '=' int_literal
						
DistributeOption
						::= 'DISTRIBUTION' '=' DistributionList
DistributionList
						::= DistributionItem (',' DistributionItem)*
DistributionItem
						::= '(' LeaderEndpoint ',' FollowerEndpointList ')'
LeaderEndpoint 
						::= 	Endpoint	
FollowerEndpointList
						::= '[' Endpoint (',' Endpoint)* ']'
Endpoint
				::= string_literals
StorageModeOption
						::= 'STORAGE_MODE' '=' StorageMode
StorageMode
						::= 'Memory'
						    | 'HDD'
						    | 'SSD'
CompressTypeOption
						::= 'COMPRESS_TYPE' '=' CompressType
CompressType
						::= 'NoCompress'
						    | 'Snappy
```




| Configuration Item | Note                                                                                                                                                                                                                                                                                                                                                                                                                                            | Example                                                                       |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `PARTITIONNUM`     | It defines the number of partitions for the table. OpenMLDB divides the table into different partition blocks for storage. A partition is the basic unit of storage, replica, and fail-over related operations in OpenMLDB. When not explicitly configured, `PARTITIONNUM` defaults to 8.                                                                                                                                                       | `OPTIONS (PARTITIONNUM=8)`                                                    |
| `REPLICANUM`       | It defines the number of replicas for the table. Note that the number of replicas is only configurable in Cluster version.                                                                                                                                                                                                                                                                                                                      | `OPTIONS (REPLICANUM=3)`                                                      |
| `DISTRIBUTION`     | It defines the distributed node endpoint configuration. Generally, it contains a Leader node and several followers. `(leader, [follower1, follower2, ..])`. Without explicit configuration, OpenMLDB will automatically configure `DISTRIBUTION` according to the environment and nodes.                                                                                                                                                        | `DISTRIBUTION = [ ('127.0.0.1:6527', [ '127.0.0.1:6528','127.0.0.1:6529' ])]` |
| `STORAGE_MODE`     | It defines the storage mode of the table. The supported modes are `Memory`, `HDD` and `SSD`. When not explicitly configured, it defaults to `Memory`. <br/>If you need to support a storage mode other than `Memory` mode, `tablet` requires additional configuration options. For details, please refer to [tablet configuration file **conf/tablet.flags**](../../../deploy/conf.md#the-configuration-file-for-apiserver:-conf/tablet.flags). | `OPTIONS (STORAGE_MODE='HDD')`                                                |
| `COMPRESS_TYPE` | It defines the compress types of the table. The supported compress type are `NoCompress` and `Snappy`. The default value is `NoCompress`                                               | `OPTIONS (COMPRESS_TYPE='Snappy')`


#### The Difference between Disk Table and Memory Table
- If the value of `STORAGE_MODE` is `HDD` or `SSD`, the table is a **disk table**. If `STORAGE_MODE` is `Memory`, the table is a **memory table**.
- Currently, disk tables do not support GC operations
- When inserting data into a disk table, if (`key`, `ts`) are the same under the same index, the old data will be overwritten; a new piece of data will be inserted into the memory table.
- Disk tables do not support `addindex` or `deleteindex` operations, so you need to define all required indexes when creating a disk table. The `deploy` command will automatically add the required indexes, so for a disk table, if the corresponding index is missing when it is created, `deploy` will fail.



#### Example
The following sql commands create a table and configure the number of partitions as 8, the number of replicas as 3, and the storage_mode as HDD.

```sql
USE db1;
--SUCCEED: Database changed    
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time)) OPTIONS(partitionnum=8, replicanum=3, storage_mode='HDD');
--SUCCEED
DESC t1;
--- ---------- ----------- ------ ----------
#   Field      Type        Null   Default
 --- ---------- ----------- ------ ---------
  1   col0       Varchar     YES
  2   col1       Int         YES
  3   std_time   Timestamp   YES
 --- ---------- ----------- ------ ---------
 --- -------------------- ------ ---------- ------ ---------------
  #   name                 keys   ts         ttl    ttl_type
 --- -------------------- ------ ---------- ------ ---------------
  1   INDEX_0_1651143735   col1   std_time   0min   kAbsoluteTime
 --- -------------------- ------ ---------- ------ ---------------
 --------------- --------------
  compress_type   storage_mode
 --------------- --------------
  NoCompress      HDD
 --------------- --------------
```
The following sql command create a table with specified distribution.
```sql
create table t1 (col0 string, col1 int) options (DISTRIBUTION=[('127.0.0.1:30921', ['127.0.0.1:30922', '127.0.0.1:30923']), ('127.0.0.1:30922', ['127.0.0.1:30921', '127.0.0.1:30923'])]);
--SUCCEED
```

## Related SQL

[CREATE DATABASE](../ddl/CREATE_DATABASE_STATEMENT.md)

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)



