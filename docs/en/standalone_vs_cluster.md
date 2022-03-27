# Difference between OpenMLDB cluster version and standalone version

## 1. The difference between installation and deployment

The cluster version and the standalone version have their own deployment methods. For details, see [Installation and Deployment Details](../deploy/install_deploy.md). All in all, the main differences are:

- The cluster version needs to install and deploy zookeeper
- The cluster version needs to install task-manager

## 2. Differences in usage

### 2.1 Different workflow

| Cluster Edition Workflow | Standalone Edition Workflow | Difference Description |
| ---------------- | ---------------- | --------------- --------------------------------------------- |
| Create Database and Tables | Create Database and Tables | None |
| Offline data preparation | Data preparation | The cluster version of OpenMLDB needs to prepare offline data and online data separately. <br />The stand-alone version can use the same data or prepare different data for offline and online feature calculation. |
| Offline Feature Computation | Offline Feature Computation | None |
| SQL Plan Online | SQL Plan Online | None |
| Online data preparation | None | The cluster version of OpenMLDB needs to prepare offline data and online data separately. <br />The stand-alone version can use the same data or prepare different data for offline and online feature calculation. |
| Online real-time feature calculation | Online real-time feature calculation | None |



### 2.2 Execution mode

The cluster version supports the system variable `execute_mode`, which supports configuring the execution mode.

When executed on the cluster version command line:

```sql
> SET @@execute_mode = "offline"
````

OpenMLDB switches to offline execution mode. In this mode, only offline data will be imported/inserted and queried.

When executed on the cluster version command line:

```sql
> SET @@execute_mode = "online"
````

OpenMLDB switches to online execution mode. In this mode, only online data will be imported/inserted and queried.

### 2.3 Offline task management

Offline task management is a unique feature of the cluster version.

The standalone version `LOAD DATA` data, `SELECT INTO` command is blocking, the cluster version will submit a task, and provide `SHOW JOBS`, `SHOW JOB` commands to view offline tasks. For details, see [Offline Task Management](../reference/sql/task_manage/reference.md).

### 2.4 SQL Boundaries

The differences in SQL query capabilities supported by the cluster version and the standalone version include:

- [Offline task management statement](../reference/sql/task_manage/reference.md)
  - Standalone version of OpenMLDB does not support
  - The cluster version of OpenMLDB supports offline task management statements, including: `SHOW JOBS`, `SHOW JOB`, etc.
- Execution mode
  - Standalone version of OpenMLDB does not support
  - Clustered OpenMLDB can configure the execution mode: `SET @@execute_mode = ...`
- Use of `CREAT TABLE`[create table statement](../reference/sql/ddl/CREATE_TABLE_STATEMENT.md)
  - The standalone version of OpenMLDB does not support configuring distributed properties
  - Cluster version of OpenMLDB supports configuring distributed properties: including `REPLICANUM`, `DISTRIBUTION`, `PARTITIONNUM`
- Use of the `SELECT INTO` statement
  - Execute `SELECT INTO` in the stand-alone version, the output is a file
  - The cluster version of OpenMLDB executes `SELECT INTO`, the output is a directory
- In the online execution mode of the cluster version, only simple single-table query statements are supported:
  - Only supports column, expression, and single-row processing functions (Scalar Function) and their combined expression operations
  - Single table query does not contain [GROUP BY clause](../reference/sql/dql/JOIN_CLAUSE.md), [HAVING clause](../reference/sql/dql/HAVING_CLAUSE.md) and [WINDOW subclause] sentence](../reference/sql/dql/WINDOW_CLAUSE.md)
  - Single table query only involves the calculation of a single table, and does not design the calculation of multiple tables [JOIN](../reference/sql/dql/JOIN_CLAUSE.md)

### 2.5 SDK Support

Currently, the Python and Java SDKs of OpenMLDB only support the cluster version, and the next version v0.5.0 will plan to support both the cluster and standalone versions.
