# 集群版 vs 单机版

## 1. 安装和部署的区别

集群版和单机版有各自的部署方式，具体可以参见[安装部署详细说明](../deploy/install_deploy.md)。概括来说，主要的区别表现为:

- 集群版需要安装和部署zookeeper
- 集群版需要安装task-manager

## 2. 使用方式的区别

### 2.1 工作流程不同

| 集群版工作流     | 单机版工作流     | 区别描述                                                     |
| ---------------- | ---------------- | ------------------------------------------------------------ |
| 建立数据库和表   | 建立数据库和表   | 无                                                           |
| 离线数据准备     | 数据准备         | 集群版OpenMLDB需要分别准备离线数据和在线数据。<br />单机版既可以使用同一份数据，也可以准备不同数据，用于离线和在线特征计算。 |
| 离线特征计算     | 离线特征计算     | 无                                                           |
| SQL 方案上线     | SQL 方案上线     | 无                                                           |
| 在线数据准备     | 无               | 集群版OpenMLDB需要分别准备离线数据和在线数据。<br />单机版既可以使用同一份数据，也可以准备不同数据，用于离线和在线特征计算。 |
| 在线实时特征计算 | 在线实时特征计算 | 无                                                           |

### 2.2 执行模式

集群版支持系统变量`execute_mode`，支持配置执行模式。单机版并没有执行模式的区别。

当在集群版命令行下执行：

```sql
> SET @@execute_mode = "offline"
```

OpenMLDB切换到离线执行模式。在该模式下，只会导入/插入以及查询离线数据。

离线模式下，默认任务是异步模式，可以设置为同步模式，这样命令行会阻塞等待直到离线任务完成。

```sql
set @@sync_job = true;
```

当在集群版命令行下执行：

```sql
> SET @@execute_mode = "online"
```

OpenMLDB切换到在线执行模式。在该模式下，只会导入/插入以及查询在线数据。

### 2.3 离线任务管理

离线任务管理是集群版特有的功能。

单机版`LOAD DATA`数据，`SELECT INTO`命令是阻塞式的，集群版会提交一个任务，并提供`SHOW JOBS`, `SHOW JOB`命令查看离线任务。具体可以参见[离线任务管理](../openmldb_sql/task_manage/reference.md)。

### 2.4 SQL边界

集群版和单机版可以支持的SQL查询能力区别包括：

- [离线任务管理语句](../openmldb_sql/task_manage/reference.md)
  - 单机版OpenMLDB不支持
  - 集群版OpenMLDB支持离线任务管理语句，包括:`SHOW JOBS`, `SHOW JOB`等
- 执行模式
  - 单机版OpenMLDB不支持
  - 集群版OpenMLDB可以配置执行模式: `SET @@execute_mode = ...`
- `CREAT TABLE`[建表语句](../openmldb_sql/ddl/CREATE_TABLE_STATEMENT.md)的使用
  - 单机版OpenMLDB不支持配置分布式的属性
  - 集群版OpenMLDB支持配置分布式属性：包括`REPLICANUM`, `DISTRIBUTION`, `PARTITIONNUM`
- `SELECT INTO` 语句的使用
  - 单机版下执行`SELECT INTO`，输出是文件
  - 集群版OpenMLDB执行`SELECT INTO`，输出是目录
- 集群版在线执行模式下，只能支持简单的单表查询语句：
  - 仅支持列，表达式，以及单行处理函数（Scalar Function)以及它们的组合表达式运算
  - 单表查询不包含[GROUP BY子句](../openmldb_sql/dql/JOIN_CLAUSE.md)，[HAVING子句](../openmldb_sql/dql/HAVING_CLAUSE.md)以及[WINDOW子句](../openmldb_sql/dql/WINDOW_CLAUSE.md)
  - 单表查询只涉及单张表的计算，不设计[JOIN](../openmldb_sql/dql/JOIN_CLAUSE.md)多张表的计算

### 2.5 SDK 支持

OpenMLDB 的 Python 和 Java SDK 均支持集群版和单机版。