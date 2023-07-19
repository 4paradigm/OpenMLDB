# CREATE TABLE
 `CREATE TABLE` 语句用于创建一张表。同一个数据库下，表名必须是唯一的，在同一个数据库下，重复创建同名表，会发生错误。

## Syntax

```sql
CreateTableStmt ::=
    'CREATE' 'TABLE' IfNotExists TableName ( TableElementList CreateTableSelectOpt | LikeTableWithOrWithoutParen ) OnCommitOpt

IfNotExists ::=
    ('IF' 'NOT' 'EXISTS')?
    
TableName ::=
    Identifier ('.' Identifier)?
    
TableElementList ::=
    TableElement ( ',' TableElement )*
    
TableElement ::=
    ColumnDef | ColumnIndex
```

建表语句中需要定义`TableElementList`，即`TableElement`列表。`TableElement`分为列描述`ColumnDef`和列索引`ColumnIndex`。OpenMLDB要求`TableElement`列表中至少包含一个`ColumnDef`。

或者基于 `LIKE` 语法建表，目前支持基于 Hive 和 Parquet 格式。

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

基于 Hive 建表的详情可查看文档 [Hive 数据源支持](../../integration/offline_data_sources/hive.md)。

### 列描述ColumnDef（必要）

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

一张表中包含一个或多个列。每一列的列描述`ColumnDef`描述了列名、列类型以及列约束配置。

- 列名：列在表中的名字。同一张表内的列名必须是唯一的。
- 列类型：列的类型。关于OpenMLDB支持的数据类型，详见[数据类型](../../openmldb_sql/data_types)。
- 列约束配置：
  - `NOT NULL`: 该列的取值不允许为空。
  - `DEFAULT`: 设置该列的默认值。`NOT NULL`的属性推荐同时配置`DEFAULT`默认值，在插入数据时，若没有定义该列的值，会插入默认值。若设置了`NOT NULL`属性但没有配置`DEFAULT`值，插入语句中未定义该列值时，OpenMLDB会抛出错误。

#### Example
 **示例1：创建一张表**

将当前数据库设为`db1`，在当前数据库中创建一张表`t1`，包含列`col0`，列类型为STRING

```sql
CREATE DATABASE db1;
-- SUCCEED
USE db1;
-- SUCCEED: Database changed
CREATE TABLE t1(col0 STRING);
-- SUCCEED
```
假如当前会话不在数据库`db1`下，但是仍要在`db1`中创建一张表`t2`，包含列`col0`，列类型为STRING；列`col1`，列类型为int。

```sql
CREATE TABLE db1.t2 (col0 STRING, col1 int);
-- SUCCEED
```
切换到数据库`db1`，查看表`t2`的详细信息。
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


**示例2：在同一个数据库下重复创建同名表**

```sql
CREATE TABLE t1 (col0 STRING NOT NULL, col1 int);
-- SUCCEED
CREATE TABLE t1 (col0 STRING NOT NULL, col1 int);
-- Error: table already exists
CREATE TABLE t1 (col0 STRING NOT NULL, col1 string);
-- Error: table already exists
```


**示例3：创建一张表，配置列不允许为空（NOT NULL）**

```sql
USE db1;
-- SUCCEED: Database changed
CREATE TABLE t3 (col0 STRING NOT NULL, col1 int);
-- SUCCEED
```
查看该表的详细信息
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


**示例4：创建一张表，设置列默认值**

```sql
USE db1;
--SUCCEED: Database changed
CREATE TABLE t4 (col0 STRING DEFAULT "NA", col1 int);
-- SUCCEED
desc t4;
 --- ------- --------- ------ ---------
  #   Field   Type      Null   Default
 --- ------- --------- ------ ---------
  1   col0    Varchar   YES    NA
  2   col1    Int       YES
 --- ------- --------- ------ ---------
 --- -------------------- ------ ---- ------ ---------------
  #   name                 keys   ts   ttl    ttl_type
 --- -------------------- ------ ---- ------ ---------------
  1   INDEX_0_1657327593   col0   -    0min   kAbsoluteTime
 --- -------------------- ------ ---- ------ ---------------
 --------------
  storage_mode
 --------------
  Memory
 --------------
```

**示例5：基于 Hive 表创建新表**

首先[配置OpenMLDB支持Hive](../../integration/offline_data_sources/hive.md)，然后使用以下语句。

```sql
CREATE TABLE t1 LIKE HIVE 'hive://hive_db.t1';
-- SUCCEED
```

**示例6：基于 Parquet 文件创建新表**

```sql
CREATE TABLE t1 LIKE PARQUET 'file://t1.parquet';
-- SUCCEED
```

### 列索引ColumnIndex（可选）

```sql
ColumnIndex ::= 
    'INDEX' <OptionalIndexName> '(' IndexOptionList ')' 
 
IndexOptionList ::= 
    IndexOption ( ',' IndexOption )*
   
IndexOption ::= 
    IndexOptionName '=' expr
```

索引可以被数据库搜索引擎用来加速数据的检索。 简单说来，索引就是指向表中数据的指针。OpenMLDB 支持的索引配置项（`IndexOptionName`）有索引`KEY`，索引时间列`TS`, 最大存活时间/条数`TTL`和淘汰规则`TTL_TYPE`。其中`KEY`是必须配置的，其他配置项都为可选项。下表介绍了各索引配置项的含义、支持的表达式(`expr`)以及用法示例：

| 配置项        | 描述                                                                                                      | expr                                                                                                           | 用法示例                                                                                   |
|------------|---------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `KEY`      | 索引列（必选）。OpenMLDB支持单列索引，也支持联合索引。当`KEY`后只有一列时，仅在该列上建立索引。当`KEY`后有多列时，建立这几列的联合索引：将多列按顺序拼接成一个字符串作为索引。        | 支持单列索引：`ColumnName`<br/>或联合索引：<br/>`(ColumnName (, ColumnName)* ) `                                            | 单列索引：`INDEX(KEY=col1)`<br />联合索引：`INDEX(KEY=(col1, col2))`                             |
| `TS`       | 索引时间列（可选）。同一个索引上的数据将按照时间索引列排序。当不显式配置`TS`时，使用数据插入的时间戳作为索引时间。                                             | `ColumnName`                                                                                                   | `INDEX(KEY=col1, TS=std_time)`。索引列为col1,col1相同的数据行按std_time排序。                         |
| `TTL_TYPE` | 淘汰规则（可选）。包括四种类型，当不显式配置`TTL_TYPE`时，默认使用`ABSOLUTE`过期配置。                                                   | 支持的expr如下：`ABSOLUTE` <br/> `LATEST`<br/>`ABSORLAT`<br/> `ABSANDLAT`。                                           | 具体用法可以参考下文“TTL和TTL_TYPE的配置细则”                                                          |
| `TTL`      | 最大存活时间/条数（可选）。依赖于`TTL_TYPE`，不同的`TTL_TYPE`有不同的`TTL` 配置方式。当不显式配置`TTL`时，`TTL=0`，表示不设置淘汰规则，OpenMLDB将不会淘汰记录。 | 支持数值：`int_literal`<br/>  或数值带时间单位(`S,M,H,D`)：`interval_literal`<br/>或元组形式：`( interval_literal , int_literal )` |具体用法可以参考下文“TTL和TTL_TYPE的配置细则” |

**TTL和TTL_TYPE的配置细则：**

| TTL_TYPE    | TTL                                                          | 描述                                                 | 用法示例                                                     |
| ----------- | ------------------------------------------------------------ | ---------------------------------------------------- | ------------------------------------------------------------ |
| `ABSOLUTE`  | TTL的值代表过期时间。配置值为时间段如`100m, 12h, 1d, 365d`。最大可以配置的过期时间为`15768000m`(即30年) | 当记录过期时，会被淘汰。                             | `INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=100m)`<br />OpenMLDB将会删除100分钟之前的数据。 |
| `LATEST`    | TTL的值代表最大存活条数。即同一个索引下面，最大允许存在的数据条数。最大可以配置1000条 | 记录超过最大条数时，会被淘汰。                       | `INDEX(KEY=col1, TS=std_time, TTL_TYPE=LATEST, TTL=10)`。OpenMLDB只会保留最近10条记录，删除以前的记录。 |
| `ABSORLAT`  | 配置过期时间和最大存活条数。配置值是一个2元组，形如`(100m, 10), (1d, 1)`。最大可以配置`(15768000m, 1000)`。 | 当且仅当记录过期**或**记录超过最大条数时，才会淘汰。 | `INDEX(key=c1, ts=c6, ttl=(120min, 100), ttl_type=absorlat)`。当记录超过100条，**或者**当记录过期时，会被淘汰 |
| `ABSANDLAT` | 配置过期时间和最大存活条数。配置值是一个2元组，形如`(100m, 10), (1d, 1)`。最大可以配置`(15768000m, 1000)`。 | 当记录过期**且**记录超过最大条数时，记录会被淘汰。   | `INDEX(key=c1, ts=c6, ttl=(120min, 100), ttl_type=absandlat)`。当记录超过100条，**而且**记录过期时，会被淘汰 |

```{note}
最大过期时间和最大存活条数的限制，是出于性能考虑。如果你一定要配置更大的TTL值，请使用UpdateTTL来增大（可无视max限制），或者调整nameserver配置`absolute_ttl_max`和`latest_ttl_max`，重启生效。
```
#### Example
**示例1：创建一张带单列索引的表**

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

**示例2：创建一张带联合列索引的表**

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

**示例3：创建一张带单列索引+时间列的表**

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


**示例4：创建一张带单列索引+时间列的TTL type为abusolute表，并配置ttl为30天**

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

**示例5：创建一张带单列索引+时间列的TTL type为latest表，并配置ttl为1**
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

**示例6：创建一张带单列索引+时间列的TTL type为absANDlat表，并配置过期时间为30天，最大留存条数为10条**

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

**示例7：创建一张带单列索引+时间列的TTL type为absORlat表，并配置过期时间为30天，最大留存条数为10条**

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

**示例8：创建一张多索引的表**
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

### 表属性TableOptions（可选）

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
```



| 配置项            | 描述                                                                                                                                                               | 用法示例                                                                          |
|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `PARTITIONNUM` | 配置表的分区数。OpenMLDB将表分为不同的分区块来存储。分区是OpenMLDB的存储、副本、以及故障恢复相关操作的基本单元。不显式配置时，`PARTITIONNUM`默认值为8。                                                                      | `OPTIONS (PARTITIONNUM=8)`                                                    |
| `REPLICANUM`   | 配置表的副本数。请注意，副本数只有在集群版中才可以配置。                                                                                                                                     | `OPTIONS (REPLICANUM=3)`                                                      |
| `DISTRIBUTION` | 配置分布式的节点endpoint。一般包含一个Leader节点和若干Follower节点。`(leader, [follower1, follower2, ..])`。不显式配置时，OpenMLDB会自动根据环境和节点来配置`DISTRIBUTION`。                                  | `DISTRIBUTION = [ ('127.0.0.1:6527', [ '127.0.0.1:6528','127.0.0.1:6529' ])]` |
| `STORAGE_MODE` | 表的存储模式，支持的模式有`Memory`、`HDD`或`SSD`。不显式配置时，默认为`Memory`。<br/>如果需要支持非`Memory`模式的存储模式，`tablet`需要额外的配置选项，具体可参考[tablet配置文件 conf/tablet.flags](../../../deploy/conf.md)。 | `OPTIONS (STORAGE_MODE='HDD')`                                                |

#### 磁盘表与内存表区别
- 磁盘表对应`STORAGE_MODE`的取值为`HDD`或`SSD`。内存表对应的`STORAGE_MODE`取值为`Memory`。
- 目前磁盘表不支持GC操作
- 磁盘表插入数据，同一个索引下如果（`key`, `ts`）相同，会覆盖旧的数据；内存表则会插入一条新的数据
- 磁盘表不支持`addindex`和`deleteindex`操作，所以创建磁盘表的时候需要定义好所有需要的索引
（`deploy`命令会自动添加需要的索引，所以对于磁盘表，如果创建的时候缺失对应的索引，则`deploy`会失败）

#### Example
创建一张表，配置分片数为8，副本数为3，存储模式为HDD
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
 --------------
  storage_mode
 --------------
  HDD
 --------------
```
创建一张表，指定分片的分布状态
```sql
create table t1 (col0 string, col1 int) options (DISTRIBUTION=[('127.0.0.1:30921', ['127.0.0.1:30922', '127.0.0.1:30923']), ('127.0.0.1:30922', ['127.0.0.1:30921', '127.0.0.1:30923'])]);
--SUCCEED
```

## 相关SQL

[CREATE DATABASE](../ddl/CREATE_DATABASE_STATEMENT.md)

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)