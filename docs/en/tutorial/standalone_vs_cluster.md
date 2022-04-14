# Cluster vs. Standalone Versions

## 1. Installation and Deployment

The cluster version and the standalone version have their own deployment methods. For details, see [Installation and Deployment Details](../deploy/install_deploy.md). In summary, the main differences are:

- The cluster version needs to install and deploy zookeeper
- The cluster version needs to install task-manager

## 2. Usage

### 2.1 Workflows

| Workflow of Cluster Version         | Workflow of Standalone Version      | Difference                                                   |
| ----------------------------------- | ----------------------------------- | ------------------------------------------------------------ |
| Database and table creation         | Database and table creation         | None                                                         |
| Offline data preparation            | Data preparation                    | The cluster version of OpenMLDB needs to prepare offline data and online data separately. <br />The standalone version can use the same data or prepare different data for offline and online feature extraction. |
| Offline feature extraction          | Offline feature extraction          | None                                                         |
| SQL deployment                      | SQL deployment                      | None                                                         |
| Online data preparation             | Data preparation (optional)         | The cluster version of OpenMLDB needs to prepare offline data and online data separately. <br />The standalone version can use the same data or prepare different data for offline and online feature extraction. |
| Online real-time feature extraction | Online real-time feature extraction | None                                                         |

### 2.2 Execution Modes

The cluster version supports the system variable `execute_mode`, which supports configuring the execution mode. In the cluster version, the `offline` and `online` execution modes correspond to the offline and online databases, respectively. For the standalone version, there is no such a concept of "execution mode".

For the cluster verion, you can execute the below command in CLI to set the execution mode:

```sql
> SET @@execute_mode = "offline" | "online"
````


### 2.3 Offline Task Management

Offline task management is a unique feature of the cluster version.

The `LOAD DATA` and `SELECT INTO` command are blocking in the standalone version. However, the cluster version submits a task for those commands, and provides the commands ``SHOW JOBS`` and `SHOW JOB` to investigate the status of offline tasks. For details, see [Offline Task Management](../reference/sql/task_manage/reference.md).

### 2.4 SQL Functionalities

The differences in SQL query capabilities supported by the cluster version and the standalone version include:

- [Offline task management statement](../reference/sql/task_manage/reference.md)
  - Standalone version of OpenMLDB does not support
  - The cluster version of OpenMLDB supports offline task management statements, including: `SHOW JOBS`, `SHOW JOB`, etc.
- Execution mode
  - The Standalone version of OpenMLDB does not support setting execution mode.
  - Clustered OpenMLDB can configure the execution mode: `SET @@execute_mode = ...`
- Use of `CREAT TABLE`[create table statement](../reference/sql/ddl/CREATE_TABLE_STATEMENT.md)
  - The standalone version of OpenMLDB does not support configuring distributed properties
  - Cluster version of OpenMLDB supports configuring distributed properties: including `REPLICANUM`, `DISTRIBUTION`, `PARTITIONNUM`
- Use of the `SELECT INTO` statement
  - The output of `SELECT INTO` in the standalone version is a file.
  - The output of `SELECT INTO` in the cluster version is a directory.
- In the online execution mode of the cluster version, only simple single-table query statements are supported:
  - Only supports column, expression, and single-row processing functions (Scalar Function) and their combined expression operations
  - A single table query does not contain [GROUP BY clause](../reference/sql/dql/JOIN_CLAUSE.md), [HAVING clause](../reference/sql/dql/HAVING_CLAUSE.md) and [WINDOW subclause] sentence](../reference/sql/dql/WINDOW_CLAUSE.md).
  - A single table query only involves the computation on a single table, but no [JOIN](../reference/sql/dql/JOIN_CLAUSE.md) based multiple table computation.

### 2.5 SDK Support

Currently, the Python and Java SDKs of OpenMLDB only support the cluster version, and the next version v0.5.0 will plan to support both the cluster and standalone versions.
