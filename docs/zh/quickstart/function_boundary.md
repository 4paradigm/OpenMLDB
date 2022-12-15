# 功能边界

## 概述

本文我们将介绍OpenMLDB的功能边界：
1. 系统配置
1. DDL边界，包括创建表、创建索引、deploy等
1. DML边界，包括导入数据、存储数据等
1. DQL边界，包括批查询、实时请求等

```{caution}
如果对SQL语句的编写和详情有疑问，请参考[sql reference](../openmldb_sql/)，或搜索关键字。本文只重点描述功能边界。
```

## 系统配置

### taskmanager

通过配置taskmanager可以决定离线存储地址`offline.data.prefix`、离线job计算所需spark模式`spark.master`等。

- `offline.data.prefix`：可配置为file或hdfs。生产环境建议配置hdfs路径，测试环境（特指onebox型，例如openmldb docker容器内启动的集群版）可以配置file本地存储。file作为offline存储，将无法支持多taskmanager分布式部署（taskmanager之间不会传输数据）。如果想在多台主机上部署taskmanager，请使用hdfs等多机可同时访问到的存储介质。如果想测试多taskmanager工作协同，可以在一台主机上部署多个taskmanager，此时可以使用file作为offline存储。

- `spark.master=local[*]` ：spark默认配置为local[*]模式（自动绑定cpu core数，如果发现离线任务比较慢，建议使用yarn模式，改变配置后重启taskmanager生效。更多配置可参考[master-urls](https://spark.apache.org/docs/3.1.2/submitting-applications.html#master-urls)

#### spark.default.conf

更多可选配置，可以写在`spark.default.conf`中，格式为`k1=v1;k2=v2`，分号分隔。例如：
```
spark.default.conf=spark.port.maxRetries=32;foo=bar
```

- `spark.port.maxRetries`: 默认16，参考[configuration](https://spark.apache.org/docs/3.1.2/configuration.html)。每个离线job都会绑定spark ui，对应一个port。每次port都是从默认初始端口开始尝试绑定，retry即绑定下一个端口port+1，如果同时运行的job大于`spark.port.maxRetries`，retry次数也就大于`spark.port.maxRetries`，job就会启动失败，也就会job failed。如果你需要更大的job并发，请配置更大的`spark.port.maxRetries`，重启taskmanager生效。

## DDL边界

### Deploy

通过`DEPLOY <deploy_name> <sql>`可以上线SQL，这个操作不仅是上线SQL，也会自动解析SQL帮助创建索引（可以通过`DESC <table_name>`查看索引详情），详情见[DEPLOY_STATEMENT](../openmldb_sql/deployment_manage/DEPLOY_STATEMENT.md)。

`DEPLOY`操作是否成功，跟表的**在线数据**有一定关系。

#### 长窗口SQL

长窗口SQL,即`DEPLOY`时带有`OPTIONS(long_windows=...)`，语法详情见[长窗口](../openmldb_sql/deployment_manage/DEPLOY_STATEMENT.md#长窗口优化)。长窗口SQL的`DEPLOY`条件比较严格，必须保证SQL中使用的表没有在线数据。如果表已有数据，即使`DEPLOY`和之前一致的SQL，也会操作失败。

#### 普通SQL

- 如果`DEPLOY`之前已存在相关的索引，那么这一次`DEPLOY`操作不会创建索引。无论表中有无在线数据，`DEPLOY`操作成功。

- 如果`DEPLOY`需要创建新的索引，而此时表中已有在线数据，那么`DEPLOY`操作将失败。

解决方案可以选择其一：
- 严格保持先`DEPLOY`再导入在线数据，不要在表中有在线数据后做`DEPLOY`。
- `CRATE INDEX`是可以在创建新索引时，自动导入已存在的在线数据（**已有索引**里的数据）。如果一定需要在**表已有在线数据**的情况下`DEPLOY`，可以先手动`CRATE INDEX`创建需要的索引（新索引就有数据了），再`DEPLOY`（这时的`DEPLOY`不会创建新索引，计算时直接使用手动创建的那些索引）。

```{tip}
如何知道应该创建哪些索引？

目前只有JAVA SDK支持，可以通过`SqlClusterExecutor.genDDL`获取需要创建的所有索引。（但`genDDL`是获得建表语句，所以需要手动转换为`CREATE INDEX`。）

未来我们将支持**直接获取创建索引语句**，或支持`DEPLOY`**自动导入数据到新索引**。
```

## DML边界

### LOAD DATA

#### 集群版LOAD DATA

集群版LOAD DATA无论导入到在线或离线，都是离线job。源数据的格式规则，离线在线没有区别。

推荐使用hdfs文件作为源数据，无论taskmanager是local/yarn模式，还是taskmanager在别的主机上运行，都可以导入。如果源数据为`file`本地文件，那么需要考虑taskmanager模式和运行主机，是否可以顺利进行导入。
```{seealso}
1. taskmanager是local模式，`file`的本地就是指taskmanager进程所在的主机。只有将源数据放在taskmanager进程的主机上才能顺利导入。请注意你的taskmanager是本机运行，还是分布在别处主机。
1. taskmanager是yarn(client and cluster)模式时，由于不知道运行容器是哪台主机，不可使用`file`作为源数据地址。
```

#### 单机版LOAD DATA

单机版LOAD DATA与集群版不同，它从客户端本地获取源数据，更类似`LOAD DATA LOCAL INFILE`。功能上仅支持csv，且支持的格式与集群版有些差别。

### DELETE

在线存储的表有多索引，DELETE可能无法删除所有索引中的对应数据，所以，可能出现删除了数据，却能查出已删除数据的情况。
举例说明：
```
create database db;
use db;
create table t1(c1 int, c2 int,index(key=c1),index(key=c2));
desc t1;
set @@execute_mode='online';
insert into t1 values (1,1),(2,2);
delete from t1 where c2=2;
select * from t1;
select * from t1 where c2=2;
```
输出结果为
```
 --- ------- ------ ------ ---------
  #   Field   Type   Null   Default
 --- ------- ------ ------ ---------
  1   c1      Int    YES
  2   c2      Int    YES
 --- ------- ------ ------ ---------
 --- -------------------- ------ ---- ------ ---------------
  #   name                 keys   ts   ttl    ttl_type
 --- -------------------- ------ ---- ------ ---------------
  1   INDEX_0_1668504212   c1     -    0min   kAbsoluteTime
  2   INDEX_1_1668504212   c2     -    0min   kAbsoluteTime
 --- -------------------- ------ ---- ------ ---------------
 --------------
  storage_mode
 --------------
  Memory
 --------------
 ---- ----
  c1   c2
 ---- ----
  1    1
  2    2
 ---- ----

2 rows in set
 ---- ----
  c1   c2
 ---- ----

0 rows in set
```
表t1有多个索引（DEPLOY也可能自动创建出多索引），`delete from t1 where c2=2`实际只删除了第二个index的数据，第一个index数据没有被影响。所以`select * from t1`使用第一个索引，结果会有两条数据，并没有删除，`select * from t1 where c2=2`使用第二个索引，结果为空，数据已被删除。

## DQL边界

Query语句首先要分执行模式，离线模式中query只有批查询，在线模式中分为批查询（又称在线预览模式，仅支持部分SQL）和请求查询（又称在线请求模式）

### 在线模式Query

集群版的在线模式Query（即SELECT语句），分为在线预览模式和在线请求模式。

#### 集群版在线预览模式

通过执行SQL，例如，CLI中在线模式执行SQL，均为在线预览模式。在线预览模式支持有限，详细支持情况请参考[SELECT STATEMENT](../openmldb_sql/dql/SELECT_STATEMENT)，不是所有SQL都可执行。

在线预览模式主要目的为预览，
- 如果你希望能运行复杂SQL，请使用离线模式。
- 如果你希望查询所有在线数据，**请不要依赖`SELECT * FROM table`的结果**。目前没有简单手段获得在线的全部数据，可以考虑使用导出工具导出在线数据。

```{caution}
在线数据通常是分布式存储的，`SELECT * FROM table`从各个tablet server中获取结果，但并不会做全局排序，且server顺序有一定的随机性。所以每次执行`SELECT * FROM table`的结果不能保证数据顺序一致。

如果在线表数据量过大，还可能触发"数据截断"，详情见[SELECT STATEMENT](../openmldb_sql/dql/SELECT_STATEMENT)，`SELECT * FROM table`的结果会少于实际存储。如果发现SELECT的条数少于你导入的条数，那很可能是数据截断，而非导入丢失了数据。
```

### 离线模式与在线请求模式

在[特征工程开发上线全流程](../tutorial/modes.md#11-特征工程开发上线全流程)中，我们主要是使用离线模式和在线请求模式。

- 离线模式的批查询：离线特征生成
- 在线请求模式的请求查询：实时特征计算

两种模式虽然不同，但是用的是同一SQL，且计算结果一致。但由于实时特征计算有性能要求与架构限制，不是所有SQL都能用于在线请求。（在线请求模式可执行的SQL是离线SQL的子集）**在实际开发中，你需要在完成离线SQL开发后`DEPLOY` SQL，来测试SQL是否可上线。**

```{tip}
批查询和请求查询，实际上是SQL的两种编译模式，会生成不同的计划。所以，当你在做离线SQL开发时，请使用离线模式进行开发验证，不要使用`DEPLOY`做验证。同样，需要上线的SQL需要通过`DEPLOY`验证，离线SQL可执行并不代表可以上线。
```
