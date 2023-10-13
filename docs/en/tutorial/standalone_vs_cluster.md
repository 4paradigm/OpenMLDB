# Cluster vs. Standalone Versions

## 1. Installation and Deployment

Please refer to this document for detail: [Install and Deploy](../deploy/install_deploy.md). 

## 2. Usage

### 2.1. Workflows

| Steps                               | Difference                                                   |
| ----------------------------------- | ------------------------------------------------------------ |
| Database and table creation         | None                                                         |
| Offline data preparation            | None                                                         |
| Offline feature extraction          | None                                                         |
| SQL deployment                      | None                                                         |
| Online data preparation             | - For the cluster version, because the offline and online storage engines are separated, the online data has to be imported explicitly. <br />- For the standalone version, the same storage engine is used for both offline and online, the same data set can be shared between the offline and online feature extraction. |
| Online real-time feature extraction | None                                                         |

### 2.2. Execution Modes

The cluster version supports the system variable `execute_mode`, which supports configuring the execution mode. In the cluster version, the `offline` and `online` execution modes correspond to the offline and online databases, respectively. For the standalone version, there is no such concept of "execution mode".

For the cluster version, you can execute the below command in CLI to set the execution mode:

```sql
> SET @@execute_mode = "offline" | "online"
```

In offline execution mode, it is asynchronous by default. We can set as synchronous so that the command will be blocked util the job has been finished.

```sql
set @@sync_job = true;
```

### 2.3. Offline Task Management

Offline task management is a unique feature of the cluster version.

The `LOAD DATA` and `SELECT INTO` command are blocking in the standalone version. However, the cluster version submits a task for those commands, and provides the commands ``SHOW JOBS`` and `SHOW JOB` to investigate the status of offline tasks. For details, see [Offline Task Management](../reference/sql/task_manage/reference.md).

### 2.4. SQL

The differences in SQL query capabilities supported by the cluster version and the standalone version include:

- [Offline task management statement](../reference/sql/task_manage/reference.md)
  - Standalone version of OpenMLDB is not supported
  - The cluster version of OpenMLDB supports offline task management statements, including: `SHOW JOBS`, `SHOW JOB`, etc.
- Execution mode
  - The Standalone version of OpenMLDB does not support setting execution mode.
  - Clustered OpenMLDB can configure the execution mode: `SET @@execute_mode = ...`
- Use of `CREATE TABLE`
  - The cluster version supports the properties related to distributed computing and storage, including `REPLICANUM`, `DISTRIBUTION`, `PARTITIONNUM`; the standalone version does not support such properties.
- Use of `SELECT INTO`
  - The output of `SELECT INTO` for the standalone version is a file.
  - The output of `SELECT INTO` for the cluster version is a directory.
- In the online execution mode of the cluster version, only simple single-table based query statements are supported:
  - It only supports column, expression, and single-row processing functions (scalar functions) and their combined expression operations
  - A single table query does not contain [GROUP BY clause](../reference/sql/dql/JOIN_CLAUSE.md), [HAVING clause](../reference/sql/dql/HAVING_CLAUSE.md) and [WINDOW sub-clause](../reference/sql/dql/WINDOW_CLAUSE.md).
  - A single table query only involves the query on a single table, but no [JOIN](../reference/sql/dql/JOIN_CLAUSE.md) based multiple table computation.

### The Supporting SDK

OpenMLDB Python SDK and Java SDK both support the use of the standalone version and cluster version.
