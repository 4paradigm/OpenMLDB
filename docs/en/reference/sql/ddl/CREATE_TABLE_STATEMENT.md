# CREATE TABLE

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
    ColumnDef
|   ColumnIndex
```

 The `CREATE TABLE` statement is used to create a table. The table name must be unique if it's in the same database. If the table with the same name is created repeatedly, an error will occur.

The `table_element` list needs to be defined in the table creation statement. `table_element` is divided into column description `ColumnDef` and `Constraint`. OpenMLDB requires at least one ColumnDef in the `table_element` list.

### Related Syntax Elements

#### Column Description ColumnDef (required)

```SQL
ColumnDef ::=
    ColumnName ( ColumnType ) [ColumnOptionList]

ColumnName
         ::= Identifier ( '.' Identifier ( '.' Identifier )? )?      
         
ColumnType ::=
						'INT' | 'INT32'
						|'SMALLINT' | 'INT16'
						|'BIGINT' | 'INT64'
						|'FLOAT'
						|'DOUBLE'
						|'TIMESTAMP'
						|'DATE'
						|'STRING' | 'VARCHAR'
						
ColumnOptionList
         ::= ColumnOption*	
ColumnOption 
				 ::= ['DEFAULT' DefaultValueExpr ] ['NOT' 'NULL']
				 	  
DefaultValueExpr
				::= int_literal | float_literal | double_literal | string_literal
```

A table contains one or more columns. The column description `ColumnDef` for each column describes the column name, column type, and class configuration.

- Column Name: The name of the column in the table. Column names within the same table must be unique.
- Column Type: The type of the column. To learn about the data types supported by OpenMLDB, please refer to [Data Types](../data_types/reference.md).
- Column Constraint Configuration:
  - `NOT NULL`: The configuration column does not allow null values.
  - `DEFAULT`: Configure column default values. The attribute of `NOT NULL` will also configure the default value of `DEFAULT`. In this case, when the data is checked, if the value of the column is not defined, the default value will be inserted. If the `NOT NULL` attribute is configured and the `DEFAULT` value is not configured, OpenMLDB will throw an error when the change column value is not defined in the insert statement.创建一张表

##### Example: Create a Table

Set the current database to `db1`, create a table `t1` in the current database, including the column `col0`, the column type is STRING

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

USE db1;
-- SUCCEED: Database changed

CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully

```

Specifies to create a table `t1` in the database `db1`, including the column `col0`, the column type is STRING

```sql
CREATE TABLE db1.t1 (col0 STRING, col1 int);
-- SUCCEED: Create successfully
desc t1;
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
```

##### Example: Create A Table, Configuration Columns Are Not Allowed To Be Empty NOT NULL

```sql
USE db1;
CREATE TABLE t1 (col0 STRING NOT NULL, col1 int);
-- SUCCEED: Create successfully
```

```sql
desc t1;
 --- ------- --------- ------ --------- 
  #   Field   Type      Null   Default  
 --- ------- --------- ------ --------- 
  1   col0    Varchar   NO              
  2   col1    Int       YES             
 --- ------- --------- ------ --------- 
 --- -------------------- ------ ---- ------ --------------- 
  #   name                 keys   ts   ttl    ttl_type       
 --- -------------------- ------ ---- ------ --------------- 
  1   INDEX_0_1639523978   col0   -    0min   kAbsoluteTime  
 --- -------------------- ------ ---- ------ --------------- 
```

##### Example:  Create A Table, Configurion Column Default Value

```sql
USE db1;
CREATE TABLE t1 (col0 STRING DEFAULT "NA", col1 int);
-- SUCCEED: Create successfully
```

```sql
desc t1;
--- ------- --------- ------ --------- 
  #   Field   Type      Null   Default  
--- ------- --------- ------ --------- 
  1   col0    Varchar   NO     NA       
  2   col1    Int       YES             
--- ------- --------- ------ --------- 
--- -------------------- ------ ---- ------ --------------- 
  #   name                 keys   ts   ttl    ttl_type       
--- -------------------- ------ ---- ------ --------------- 
  1   INDEX_0_1639524344   col0   -    0min   kAbsoluteTime  
--- -------------------- ------ ---- ------ --------------- 
```

##### Example: Create A Table With The Same Name Repeatedly In The Same Database

```sql
USE db1;
CREATE TABLE t1 (col0 STRING NOT NULL, col1 int);
-- SUCCEED: Create successfully
CREATE TABLE t1 (col1 STRING NOT NULL, col1 int);
-- SUCCEED: Create successfully
```

#### ColumnIndex (optional）

```sql
ColumnIndex
         ::= 'INDEX' IndexName '(' IndexOptionList ')' 
 
IndexOptionList
         ::= IndexOption ( ',' IndexOption )*
IndexOption
         ::=  'KEY' '=' ColumnNameList 
              | 'TS' '=' ColumnName
              | 
              | 'TTL' = int_literal
              | 'REPLICANUM' = int_literal
  
-- IndexKeyOption
IndexKeyOption 
						::= 'KEY' '=' ColumnNameList 
ColumnNameList
				 :: = '(' ColumnName (',' ColumnName)* ')'
-- IndexTsOption         
IndexTsOption 
						::= 'TS' '=' ColumnName
-- IndexTtlTypeOption					
IndexTtlTypeOption
						::= 'TTL_TYPE' '=' TTLType
TTLType ::= 
						'ABSOLUTE'
						| 'LATEST'
						| 'ABSORLAT'
						| 'ABSANDLAT'
   
-- IndexTtlOption
IndexTtlOption
						::= 'TTL' '=' int_literal|interval_literal

interval_literal ::= int_literal 'S'|'D'|'M'|'H'


```

Indexes can be used by database search engines to speed up data retrieval. Simply put, an index is a pointer to the data in a table. Configuring a column index generally requires configuring the index key, index time column, TTL and TTL_TYPE. The index key must be configured, and other configuration items are optional. The following table lists the column index configuration items：

| configuration item     | describe                                                         | Usage example                                                     |
| ---------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `KEY`      | Index column (required). OpenMLDB supports single-column indexes as well as joint indexes. When `KEY`=one column, a single-column index is configured. When `KEY`=multiple columns, the joint index of these columns is configured, specifically, several columns are spliced ​​into a string as an index in order. | Single-column index: `INDEX(KEY=col1)`<br />Joint index: `INDEX(KEY=(col1, col2))` |
| `TS`       | Index time column (optional). Data on the same index will be sorted by the time index column. When `TS` is not explicitly configured, the timestamp of data insertion is used as the index time. | `INDEX(KEY=col1, TS=std_time)`. The index column is col1, and the data rows with the same col1 are sorted by std_time. |
| `TTL_TYPE` | Elimination rules (optional). Including: `ABSOLUTE`, `LATEST`, `ABSORLAT`, `ABSANDLAT` these four types. When `TTL_TYPE` is not explicitly configured, the `ABSOLUTE` expiration configuration is used by default. | For specific usage, please refer to "Configuration Rules for TTL and TTL_TYPE"                   |
| `TTL`      | Maximum survival time/number of bars () is optional. Different TTL_TYPEs have different configuration methods. When `TTL` is not explicitly configured, `TTL=0`. A `TTL` of 0 means no eviction rule is set, and OpenMLDB will not evict records.
 |                                                              |

Configuration details of TTL and TTL_TYPE:

| TTL_TYPE    | TTL                                                          | describe                                                 | Usage example                                                     |
| ----------- | ------------------------------------------------------------ | ---------------------------------------------------- | ------------------------------------------------------------ |
| `ABSOLUTE`  | The value of TTL represents the expiration time. The configuration value is a time period such as `100m, 12h, 1d, 365d`. The maximum configurable expiration time is `15768000m` (ie 30 years) | When a record expires, it is eliminated.                             | `INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=100m)`<br />OpenMLDB will delete data older than 100 minutes. |
| `LATEST`    | The value of TTL represents the maximum number of surviving entries. That is, under the same index, the maximum number of data items allowed exists. Up to 1000 can be configured | When the record exceeds the maximum number, it will be eliminated.                       | `INDEX(KEY=col1, TS=std_time, TTL_TYPE=LATEST, TTL=10)`. OpenMLDB will only keep the last 10 records and delete the previous records. |
| `ABSORLAT`  | Configure the expiration time and the maximum number of live records. The configuration value is a 2-tuple of the form `(100m, 10), (1d, 1)`. The maximum can be configured `(15768000m, 1000)`. | Eliminates if and only if the record expires** or if the record exceeds the maximum number of records. | `INDEX(key=c1, ts=c6, ttl=(120min, 100), ttl_type=absorlat)`. When the record exceeds 100, **OR** when the record expires, it will be eliminated |
| `ABSANDLAT` |Configure the expiration time and the maximum number of live records. The configuration value is a 2-tuple of the form `(100m, 10), (1d, 1)`. The maximum can be configured `(15768000m, 1000)`. | When records expire **AND** records exceed the maximum number of records, records will be eliminated.   | `INDEX(key=c1, ts=c6, ttl=(120min, 100), ttl_type=absandlat)`. When there are more than 100 records, **AND** the records expire, they will also be eliminated. |

##### Example: Create A Table With A Single-Column Index

```sql
USE db1;
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1));
-- SUCCEED: Create successfully

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

##### Example: Create A Table With A Union Column Index

```sql
USE db1;

CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=(col0, col1)));
-- SUCCEED: Create successfully

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

##### Example: Create A Table With A Single Column Index + Time Column

```sql
USE db1;

CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time));
-- SUCCEED: Create successfully

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

##### Example: Create A Table With A Single Column Index + Time Column With A TTL Type Of Abusolute, And Configure The TTL To 30 Days

```sql
USE db1;

CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
-- SUCCEED: Create successfully

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

##### Example: Create A Table With Latest TTL Type, With A Single Column Index + Time Column, And Configure The TTL To 1

```sql
USE db1;

CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=latest, TTL=1));
-- SUCCEED: Create successfully

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

##### Example: Create A Table With A Single-Column Index + Time Column Whose TTL Type Is absANDlat, And Configure The Expiration Time To Be 30 Days And The Maximum Number Of Retained Records As 10


```sql
USE db1;

CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absandlat, TTL=(30d,10)));
-- SUCCEED: Create successfully

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

##### Example: Create A Table With A Single-Column Index + Time Column Whose TTL Type Is absORlat, And Configure The Expiration Time To Be 30 Days And The Maximum Number Of Retained Records As 10

```sql
USE db1;

CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absorlat, TTL=(30d,10)));
--SUCCEED: Create successfully

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

##### Example: Create A Multi-Index Table
```sql
USE db1;

CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col0, TS=std_time), INDEX(KEY=col1, TS=std_time));
--SUCCEED: Create successfully

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

#### Table Property TableOptions (optional)

```sql
TableOptions
						::= 'OPTIONS' '(' TableOptionItem (',' TableOptionItem)* ')'

TableOptionItem
						::= PartitionNumOption
						    | ReplicaNumOption
						    | DistributeOption
						    | StorageModeOption
								
-- PartitionNum
PartitionNumOption
						::= 'PARTITIONNUM' '=' int_literal
-- ReplicaNumOption
ReplicaNumOption
						::= 'REPLICANUM' '=' int_literal
						
-- DistributeOption				
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

-- StorageModeOption
StorageModeOption
						::= 'STORAGE_MODE' '=' StorageMode

StorageMode
						::= 'Memory'
						    | 'HDD'
						    | 'SSD'
```



| configuration item     | describe                                                         | Usage example                                                     |
| ---------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `PARTITIONNUM` | Configure the number of partitions for the table. OpenMLDB divides the table into different partition blocks for storage. A partition is the basic unit of storage, replica, and failover related operations in OpenMLDB. When not explicitly configured, `PARTITIONNUM` defaults to 8.                                                                      | `OPTIONS (PARTITIONNUM=8)`                                                    |
| `REPLICANUM`   | Configure the number of replicas for the table. Note that the number of replicas is only configurable in Cluster OpenMLDB.                                                                                                                        | `OPTIONS (REPLICANUM=3)`                                                      |
| `DISTRIBUTION` | Configure the distributed node endpoint configuration. Generally, it contains a Leader node and several follower nodes. `(leader, [follower1, follower2, ..])`. Without explicit configuration, OpenMLDB will automatically configure `DISTRIBUTION` according to the environment and node.                               | `DISTRIBUTION = [ ('127.0.0.1:6527', [ '127.0.0.1:6528','127.0.0.1:6529' ])]` |
| `STORAGE_MODE` | The storage mode of the table. The supported modes are `Memory`, `HDD` or `SSD`. When not explicitly configured, it defaults to `Memory`. <br/>If you need to support a storage mode other than `Memory` mode, `tablet` requires additional configuration options. For details, please refer to [tablet configuration file conf/tablet.flags](../../../deploy/ conf.md). | `OPTIONS (STORAGE_MODE='HDD')`                                                |

##### Disk Table（`STORAGE_MODE` == `HDD`|`SSD`）With Memory Table（`STORAGE_MODE` == `Memory`）The Difference
- Currently disk tables do not support GC operations
- When inserting data into a disk table, if (`key`, `ts`) are the same under the same index, the old data will be overwritten; a new piece of data will be inserted into the memory table
- Disk tables do not support `addindex` and `deleteindex` operations, so you need to define all required indexes when creating a disk table
(The `deploy` command will automatically add the required indexes, so for a disk table, if the corresponding index is missing when it is created, `deploy` will fail)

##### Example: Create A Band Table, Configure The Number Of Partions As 8, The Number Of Replicas As 3, And The Storage Mode As HDD

```sql
USE db1;

CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time)) OPTIONS(partitionnum=8, replicanum=3, storage_mode='HDD');
--SUCCEED: Create successfully

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
 --------------
  storage_mode
 --------------
  HDD
 --------------
```

## Related SQL

[CREATE DATABASE](../ddl/CREATE_DATABASE_STATEMENT.md)

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)



