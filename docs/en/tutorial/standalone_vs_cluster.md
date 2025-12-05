# Cluster vs. Standalone Versions

OpenMLDB has two deployment modes: the Cluster mode and the Standalone mode. The Cluster mode is suitable for production environments involving large-scale data computation, offering good scalability and high availability. On the other hand, the Standalone mode is suitable for small-scale data scenarios or trial purposes, providing more convenient deployment and usage. Both modes offer identical functionalities, but they differ in certain aspects. This article will introduce and outline the differences between the Cluster and Standalone modes.

## Installation and Deployment

Please refer to this document for details: [Install and Deploy](../deploy/install_deploy.md). In general, the differences are
- Cluster version requires installation and deployment of ZooKeeper
- Cluster version requires installation of TaskManager

## Usage

### Workflows

| Cluster ver. Workflow     | Standalone ver. Workflow     | Difference                              |
| ---------------- | ---------------- | ------------------------------------------------------------ |
| Database and table creation | Database and table creation   | None                                 |
| Offline data preparation    | Data preparation              | Cluster ver. requires separate preparation of offline and online data. <br /> Standalone ver can utilize the same dataset for both offline and online purposes. You can also prepare different datasets for offline and online feature computations|
| Offline feature extraction  | Offline feature extraction    | None                                 |
| SQL deployment              | SQL deployment                | None                                 |
| Online data preparation     | None                          |  Cluster ver. requires separate preparation of offline and online data. <br /> Standalone ver can utilize the same dataset for both offline and online purposes. You can also prepare different datasets for offline and online feature computations|
|  Online real-time feature extraction |  Online real-time feature extraction | None                 |          


### Execution Modes

The cluster version supports the system variable `execute_mode`, which supports configuring the execution mode to `offline` and `online` execution modes. For the standalone version, there is no such concept of "execution mode".

In offline execution mode, only import/insert and query of offline data is supported. For the cluster version, you can execute the below command in CLI to set the execution mode to offline:
```sql
> SET @@execute_mode = "offline"
```

In offline execution mode, it is **asynchronous** by default. We can set it as **synchronous** so that the command will be blocked until the job has been finished.
```sql
set @@sync_job = true;
```

For the cluster version, you can execute the below command in CLI to set the execution mode to online. In online mode, you can only import/insert and query online data.
```sql
> SET @@execute_mode = "online"
```

### Synchronous/Asynchronous Commands

`LOAD DATA` and `SELECT INTO` commands are synchronous in standalone version. In cluster version, some commands are asynchronous, such as, `LOAD DATA`, `SELECT`, `SELECT INTO` in online/offline mode.

### SQL

The differences in SQL query capabilities supported by the cluster version and the standalone version include:

- [Offline task management statement](../reference/sql/task_manage/SHOW_JOB.md)
  - Standalone version of OpenMLDB is not supported
  - The cluster version of OpenMLDB supports offline task management statements, including: `SHOW JOBS`, `SHOW JOB`, etc.
    
- Set execution mode to offline/online
  - The Standalone version of OpenMLDB does not support setting execution mode.
  - The cluster version of OpenMLDB can configure the execution mode: `SET @@execute_mode = "online"/"offline"`
    
- Use of [`CREATE TABLE`](../openmldb_sql/ddl/CREATE_TABLE_STATEMENT.md)
  - The Standalone version does not support configuring distributed properties."
  - The cluster version supports the properties related to distributed computing and storage, including `REPLICANUM`, `DISTRIBUTION`, `PARTITIONNUM`
    
- Use of `SELECT INTO` 
  - The output of `SELECT INTO` for the standalone version is a file.
  - The output of `SELECT INTO` for the cluster version is a directory.
    
- In the online execution mode of the cluster version, only simple single-table based query statements are supported
  - It only supports column, expression, and single-row processing functions (scalar functions) and their combined operations
  - A single table query that does not contain [GROUP BY](../reference/sql/dql/JOIN_CLAUSE.md), [HAVING](../reference/sql/dql/HAVING_CLAUSE.md) and [WINDOW](../reference/sql/dql/WINDOW_CLAUSE.md).
  - A single table query that only involves the query on a single table, but no [JOIN](../reference/sql/dql/JOIN_CLAUSE.md) of multiple tables.

### SDK Support

OpenMLDB Python SDK and Java SDK both support the use of the standalone version and cluster version.
