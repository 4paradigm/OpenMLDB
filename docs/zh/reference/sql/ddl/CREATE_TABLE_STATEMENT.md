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

 `CREATE TABLE` 语句用于创建一张表。同一个数据库下，表名在必须是唯一的，在同一个数据库下，重复创建同名表，会发生错误。

建表语句中需要定义`table_element`列表。`table_element`分为列描述`ColumnDef`和`Constraint`。OpenMLDB要求`table_element`列表中至少包含一个`ColumnDef`。

### 相关语法元素

#### 列描述ColumnDef（必要）

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

一张表中包含一个或多个列。每一列的列描述`ColumnDef`描述了列名、列类型以及类配置。

- 列名：列在表中的名字。同一张表内的列名必须是唯一的。
- 列类型：列的类型。想要了解OpenMLDB支持的数据类型，可以参考[数据类型](../data_types/reference.md)。
- 列约束配置：
  - `NOT NULL`: 配置列的不允许为空值。
  - `DEFAULT`: 配置列默认值。`NOT NULL`的属性会同时配置`DEFAULT`默认值，这样的话，查入数据时，若没有定义该列的值，会插入默认值。若配置`NOT NULL`属性且没有配置`DEFAULT`值，插入语句中未定义改列值时，OpenMLDB会抛出错误。

##### Example: 创建一张表

将当前数据库设为`db1`，在当前数据库中创建一张表`t1`，包含列`col0`，列类型为STRING

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

USE db1;
-- SUCCEED: Database changed

CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully

```

指定在数据库`db1`中创建一张表`t1`，包含列`col0`，列类型为STRING

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

##### Example: 创建一张表，配置列不允许为空NOT NULL

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

##### Example: 创建一张表，配置列配置默认值

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

##### Example: 在同一个数据库下重复创建同名表

```sql
USE db1;
CREATE TABLE t1 (col0 STRING NOT NULL, col1 int);
-- SUCCEED: Create successfully
CREATE TABLE t1 (col1 STRING NOT NULL, col1 int);
-- SUCCEED: Create successfully
```

#### 列索引ColumnIndex（可选）

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

索引可以被数据库搜索引擎用来加速数据的检索。 简单说来，索引就是指向表中数据的指针。配置一个列索引一般需要配置索引key，索引时间列, TTL和TTL_TYPE。其中索引key是必须配置的，其他配置项都为可选。下表列出了列索引配置项：

| 配置项     | 描述                                                         | 用法示例                                                     |
| ---------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `KEY`      | 索引列（必选）。OpenMLDB支持单列索引，也支持联合索引。当`KEY`=一列时，配置的是单列索引。当`KEY`=多列时，配置的是这几列的联合索引，具体来说会将几列按顺序拼接成一个字符串作为索引。 | 单列索引：`INDEX(KEY=col1)`<br />联合索引：`INDEX(KEY=(col1, col2))` |
| `TS`       | 索引时间列（可选）。同一个索引上的数据将按照时间索引列排序。当不显式配置`TS`时，使用数据插入的时间戳作为索引时间。 | `INDEX(KEY=col1, TS=std_time)`。索引列为col1,col1相同的数据行按std_time排序。 |
| `TTL_TYPE` | 淘汰规则（可选）。包括：`ABSOLUTE`, `LATEST`, `ABSORLAT`, `ABSANDLAT`这四种类型。当不显式配置`TTL_TYPE`时，默认使用`ABSOLUTE`过期配置。 | 具体用法可以参考“TTL和TTL_TYPE的配置细则”                    |
| `TTL`      | 最大存活时间/条数（）可选。不同的TTL_TYPE有不同的配置方式。当不显式配置`TTL`时，`TTL=0`。`TTL`为0表示不设置淘汰规则，OpenMLDB将不会淘汰记录。 |                                                              |

TTL和TTL_TYPE的配置细则：

| TTL_TYPE    | TTL                                                          | 描述                                                 | 用法示例                                                     |
| ----------- | ------------------------------------------------------------ | ---------------------------------------------------- | ------------------------------------------------------------ |
| `ABSOLUTE`  | TTL的值代表过期时间。配置值为时间段如`100m, 12h, 1d, 365d`。最大可以配置的过期时间为`15768000m`(即30年) | 当记录过期时，会被淘汰。                             | `INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=100m)`<br />OpenMLDB将会删除100分钟之前的数据。 |
| `LATEST`    | TTL的值代表最大存活条数。即同一个索引下面，最大允许存在的数据条数。最大可以配置1000条 | 记录超过最大条数时，会被淘汰。                       | `INDEX(KEY=col1, TS=std_time, TTL_TYPE=LATEST, TTL=10)`。OpenMLDB只会保留最近10条记录，删除以前的记录。 |
| `ABSORLAT`  | 配置过期时间和最大存活条数。配置值是一个2元组，形如`(100m, 10), (1d, 1)`。最大可以配置`(15768000m, 1000)`。 | 当且仅当记录过期**或**记录超过最大条数时，才会淘汰。 | `INDEX(key=c1, ts=c6, ttl=(120min, 100), ttl_type=absorlat)`。当记录超过100条，**或者**当记录过期时，会被淘汰 |
| `ABSANDLAT` | 配置过期时间和最大存活条数。配置值是一个2元组，形如`(100m, 10), (1d, 1)`。最大可以配置`(15768000m, 1000)`。 | 当记录过期**且**记录超过最大条数时，记录会被淘汰。   | `INDEX(key=c1, ts=c6, ttl=(120min, 100), ttl_type=absandlat)`。当记录超过100条，**而且**记录过期时，会被淘汰 |

##### Example: 创建一张带单列索引的表

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

##### Example: 创建一张带联合列索引的表

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

##### Example: 创建一张带单列索引+时间列的表

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

##### Example: 创建一张带单列索引+时间列的TTL type为abusolute表，并配置ttl为30天

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

##### Example: 创建一张带单列索引+时间列的TTL type为latest表，并配置ttl为1

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

##### Example: 创建一张带单列索引+时间列的TTL type为absANDlat表，并配置过期时间为30天，最大留存条数为10条

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

##### Example: 创建一张带单列索引+时间列的TTL type为absORlat表，并配置过期时间为30天，最大留存条数为10条

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

##### Example: 创建一张多索引的表
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

#### 表属性TableOptions（可选）

```sql
TableOptions
						::= 'OPTIONS' '(' TableOptionItem (',' TableOptionItem)* ')'

TableOptionItem
						::= PartitionNumOption
								|ReplicaNumOption
								|DistributeOption
								
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
```



| 配置项         | 描述                                                         | 用法示例                                                     |
| -------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `PARTITIONNUM` | 配置表的分区数。OpenMLDB将表分为不同的分区块来存储。分区是OpenMLDB的存储、副本、以及故障恢复相关操作的基本单元。不显式配置时，`PARTITIONNUM`默认值为8。 | `OPTIONS (PARTITIONNUM=8)`                                   |
| `REPLICANUM`   | 配置表的副本数。请注意，副本数只有在Cluster OpenMLDB中才可以配置。 | `OPTIONS (REPLICANUM=3)`                                     |
| `DISTRIBUTION` | 配置分布式的节点endpoint配置。一般包含一个Leader节点和若干follower节点。`(leader, [follower1, follower2, ..])`。不显式配置是，OpenMLDB会自动的根据环境和节点来配置`DISTRIBUTION`。 | `DISTRIBUTION = [ ('127.0.0.1:6527', [ '127.0.0.1:6528','127.0.0.1:6529' ])]` |

##### Example: 创建一张带表，配置分片数为8，副本数为3（例子需要补充完整）@denglong需要提供一个带例子以及desc table的结果

```sql
USE db1;

CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time)) OPTIONS(partitionnum=8, replicanum=3);
--SUCCEED: Create successfully
```

## 相关SQL

[CREATE DATABASE](../ddl/CREATE_DATABASE_STATEMENT.md)

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)



