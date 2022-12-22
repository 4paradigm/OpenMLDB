# 集群版和单机版区别

OpenMLDB 有两种部署模式：集群版 (cluster) 和单机版 (standalone)。集群版适合涉及到大规模数据计算的生产环境，提供良好的可扩展性和高可用性；单机版适合于小数据场景或者试用目的，更加方便部署和使用。集群版和单机版在功能上完全一致，但是在某些方面上会有区别，本文将介绍集群版和单机版的各方面区别。

## 安装和部署的区别

集群版和单机版有各自的部署方式，详情参考[安装部署文档](../deploy/install_deploy)。概括来说，主要的区别表现为：

- 集群版需要安装和部署 zookeeper
- 集群版需要安装 task-manager

## 使用方式区别

### 工作流程不同

| 集群版工作流     | 单机版工作流     | 区别描述                                                     |
| ---------------- | ---------------- | ------------------------------------------------------------ |
| 建立数据库和表   | 建立数据库和表   | 无                                                           |
| 离线数据准备     | 数据准备         | 集群版需要分别准备离线数据和在线数据。<br />单机版既可以使用同一份数据，也可以准备不同数据，用于离线和在线特征计算。 |
| 离线特征计算     | 离线特征计算     | 无                                                           |
| SQL 方案上线     | SQL 方案上线     | 无                                                           |
| 在线数据准备     | 无               | 集群版需要分别准备离线数据和在线数据。<br />单机版既可以使用同一份数据，也可以准备不同数据，用于离线和在线特征计算。 |
| 在线实时特征计算 | 在线实时特征计算 | 无                                                           |                                                         |

### 执行模式配置

集群版支持设置系统变量 `execute_mode`，配置执行模式为离线 (offline)/在线 (online) 模式。单机版不支持配置执行模式。

在离线执行模式下，只能导入/插入以及查询离线数据，集群版命令行下执行以下命令切换到离线执行模式：

```SQL
> SET @@execute_mode = "offline"
```

离线模式下，默认任务是**非阻塞模式**，执行以下命令可以设置为**阻塞模式**，这样命令行会阻塞等待离线任务完成。

```SQL
> SET @@sync_job = true;
```

集群版命令行下执行以下命令切换到在线执行模式。在该模式下，只能导入/插入以及查询在线数据。

```SQL
> SET @@execute_mode = "online"
```

### 阻塞/非阻塞命令

单机版 `LOAD DATA`、`SELECT INTO` 命令是阻塞式的，集群版的部分命令是非阻塞任务，如：在线/离线模式的 `LOAD DATA`、`SELECT`、`SELECT INTO` 命令。

### SQL 查询能力区别

集群版和单机版可以支持的 SQL 查询能力区别包括：

- [离线任务管理语句](../openmldb_sql/task_manage/SHOW_JOB.md)
  - 单机版不支持
  - 集群版提交任务以后可以使用相关的离线任务管理命令如 `SHOW JOBS`, `SHOW JOB` 来查看任务进度

- 配置执行模式为离线/在线
  - 单机版不支持
  - 集群版可以配置执行模式: `SET @@execute_mode = "online"/"offline"`

- [建表语句 `CREAT TABLE`](../openmldb_sql/ddl/CREATE_TABLE_STATEMENT.md)
  - 单机版不支持配置分布式的属性
  - 集群版支持配置分布式属性：包括 `REPLICANUM`, `DISTRIBUTION`, `PARTITIONNUM`

- `SELECT INTO` 语句
  - 单机版下执行 `SELECT INTO`，输出是文件
  - 集群版执行 `SELECT INTO`，输出是目录
  
- 集群版在线模式下，只能支持简单的单表查询语句
  - 仅支持列、表达式，以及单行处理函数（Scalar Function) 以及它们的组合表达式运算
  - 单表查询不包含 [`GROUP BY` 子句](../openmldb_sql/dql/JOIN_CLAUSE.md)、[`HAVING` 子句](../openmldb_sql/dql/HAVING_CLAUSE.md)以及 [`WINDOW` 子句](../reference/sql/dql/WINDOW_CLAUSE.md)
  - 单表查询只涉及单张表的计算，不涉及 [JOIN](../openmldb_sql/dql/JOIN_CLAUSE.md) 多张表的计算

### **SDK** **支持**

OpenMLDB 的 Python SDK、Java SDK 均支持集群版和单机版。
