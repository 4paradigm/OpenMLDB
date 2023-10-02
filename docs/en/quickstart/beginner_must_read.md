# Essential Reading for Getting Started

As OpenMLDB is a distributed system with various modes and extensive client functionality, users may encounter numerous questions and operational challenges, especially when using it for the first time. This article aims to guide beginners on diagnosing and debugging issues and providing effective information when seeking technical assistance.

## Create OpenMLDB and connection

To begin, we recommend that newcomers who are not well-versed in distributed multi-process management use Docker to set up OpenMLDB. This approach offers convenience and expedites the initial learning process. Once you have become acquainted with the various components of OpenMLDB, you can explore distributed deployment options.

You can create an OpenMLDB instance using Docker by following the instructions in the [Quickstart guide](https://chat.openai.com/c/openmldb_quickstart.md). Please note that the guide presents two versions: standalone and cluster. Ensure clarity regarding the version you intend to create and avoid mixing them.

A successful startup is indicated by your ability to connect to the OpenMLDB server using the CLI (Command Line Interface). In both standalone and cluster setups, you can use `/work/openmldb/bin/openmldb` to connect to OpenMLDB and execute `show components;` to check the running status of OpenMLDB server components.

If you encounter difficulties connecting via the CLI, first verify whether the processes are running as expected. You can confirm the presence of nameserver and tabletserver processes using `ps f|grep bin/openmldb`. In the case of the cluster version, ensure that the ZooKeeper service is running by using `ps f | grep zoo.cfg`, and confirm the existence of the taskmanager process with `ps f | grep TaskManagerServer`.

If all service processes are running correctly, but the CLI still cannot connect to the server, double-check the parameters for CLI operation. If issues persist, don't hesitate to contact us and provide the error information from the CLI.

```{seealso}
If further configuration and server logs from OpenMLDB are required, you can use diagnostic tools to obtain them, as detailed in the section below: Providing Configuration and Logging for Technical Support.
```

## Source data

### LOAD DATA

When importing data from a file into OpenMLDB, the typical command used is `LOAD DATA`. For detailed information, please refer to [LOAD DATA INFILE](https://chat.openai.com/openmldb_sql/dml/LOAD_DATA_STATEMENT.md). The data sources and formats that can be employed with `LOAD DATA` are contingent on several factors, including the OpenMLDB version (standalone or cluster), execution mode, and import mode (i.e., the `LOAD DATA` configuration item, `load_mode`). Specifically:

In the cluster version, the default `load_mode` is set to either "cluster" or "local". Meanwhile, in the standalone version, the default `load_mode` is "local," and it is not supported in the cluster version. Consequently, we discuss these scenarios in three distinct contexts:

| LOAD DATA Type                    | Support execution mode (import to destination) | Supports asynchronous/ synchronous | Supporting data sources                                      | Support data formats                           |
| :-------------------------------- | :--------------------------------------------- | :--------------------------------- | :----------------------------------------------------------- | :--------------------------------------------- |
| Cluster version load_mode=cluster | Online, offline                                | asynchronous, synchronous          | File (with conditional restrictions, please refer to specific documentation) /hdfs/ hive | CSV/ parquet (HIVE source unrestricted format) |
| Cluster version load_mode=local   | Online                                         | synchronous                        | Client Local File                                            | csv                                            |
| Standalone version（only local)   | Online                                         | synchronous                        | Client Local File                                            | csv                                            |

When the source data for `LOAD DATA` is in CSV format, it's essential to pay special attention to the timestamp column's format. Timestamps can be in "int64" format (referred to as int64 type) or "yyyy-MM-dd'T'HH:mm:ss.SSS" format (referred to as date type). Therefore, we have three types of discussions accordingly:

| LOAD DATA Type                    | Support int64 | Support date |
| :-------------------------------- | :------------ | :----------- |
| Cluster version load_mode=cluster | **``✓``**     | **``✓``**    |
| Cluster version load_mode=local   | **``✓``**     | **``X``**    |
| Standalone version（only local)   | **``✓``**     | **``X``**    |

```{hint}
The CSV file format can be inconvenient in certain situations, and we recommend considering the use of the Parquet format. This approach requires the OpenMLDB cluster version to be in use and necessitates the taskmanager component to be up and running.
```

## SQL restriction

OpenMLDB does not offer full compatibility with standard SQL, which means that certain SQL queries may not yield the expected results. If you encounter a situation where the SQL execution does not align with your expectations, it's advisable to initially verify whether the SQL adheres to the [Functional Boundary](https://chat.openai.com/c/function_boundary.md) guidelines.

## SQL Execution

All commands within OpenMLDB are SQL-based. If you experience SQL execution failures or encounter interaction issues (where it's unclear whether the command was executed successfully), consider the following checks:

1. **SQL Accuracy**: Examine whether there are errors in the SQL syntax. Syntax errors can lead to unsuccessful SQL execution. You can refer to the [SQL Reference](https://chat.openai.com/openmldb_sql/) to correct any errors.
2. **Execution Status**: Determine if the command has progressed to the execution phase or if it failed to execute. This distinction is crucial for troubleshooting.

For instance, if you encounter a syntax error prompt, it indicates a problem with the SQL writing, and you should consult the [SQL Reference](https://chat.openai.com/openmldb_sql/) for guidance on correcting it.

```
127.0.0.1:7527/db> create table t1(c1 int;
Error: Syntax error: Expected ")" or "," but got ";" [at 1:23]
create table t1(c1 int;
                      ^
```

If the command has entered the execution phase but fails or experiences interaction issues, you should clarify the following details:

- **OpenMLDB Version**: Is OpenMLDB being used in standalone mode or clustered mode?
- **Execution Mode**: What is the execution mode? You can use the `show variable` command in the CLI to retrieve this information. Note that the execution mode of the standalone version may not be meaningful.

Special attention should be given to specific usage logic when working with the cluster version of OpenMLDB.

### Cluster version SQL execution

#### Offline

For cluster offline commands, when operating in the default asynchronous mode, sending the command will yield a job ID as a return value. You can utilize `show job <id>` to inquire about the execution status of the job.

If the offline job is an asynchronous SELECT query (without saving results using INTO), the results will not be displayed on the client (synchronous SELECT queries do display results). Instead, you can retrieve the results through `show joblog <id>`, which comprises two sections: `stdout` and `stderr`. `stdout` contains the query results, while `stderr` contains the job's runtime log. If you discover that the job has failed or its status doesn't align with your expectations, it's essential to carefully review the job operation log.

```{note}
The location of these logs is determined by the job.log.path configuration in taskmanager.properties. If you have altered this configuration, you will need to search in the specified destination for the logs. By default, the stdout log can be found at /work/openmldb/taskmanager/bin/logs/job_x.log, while the job execution log is situated at /work/openmldb/taskmanager/bin/logs/job_x_error.log (note the "error" suffix).

If the task manager operates in YARN mode rather than local mode, the information in job_x_error.log may be more limited, and detailed error information about the job might not be available. In such instances, you will need to employ the YARN application ID recorded in job_x_error.log to access the actual error details within the YARN system.
```

#### Online

In the online mode of the cluster version, we typically recommend using the `DEPLOY` command to create a deployment, and for real-time feature computations on the deployment, it's advisable to utilize HTTP access to APIServer. Performing a SELECT query directly online in the CLI or other clients is referred to as "online preview." It's essential to be aware that online preview comes with several limitations, which are outlined in detail in the [Functional Boundary - Cluster Version Online Preview Mode](https://chat.openai.com/function_boundary.md#clusterversiononlinepreviewmode) document. Please avoid executing unsupported SQL queries in this context.

### Provide replication scripts

If you find yourself unable to resolve an issue through self-diagnosis, please provide us with a replication script. A comprehensive replication script should include the following components:

```
create database db;
use db;
-- create youer table
create table xx ();

-- offline or online
set @@execute_mode='';

-- load data or online insert
-- load data infile '' into table xx;
-- insert into xx values (),();

-- query / deploy ...

```

- **Data Requirements**: If your question necessitates data for replication, please include the data. For offline data that doesn't support inserting offline, kindly provide a CSV or Parquet data file. If it pertains to online data, you can either provide a data file or directly insert it within the script.
- **Execution Instructions**: These data scripts should be capable of executing SQL commands in bulk by using redirection symbols.

```
/work/openmldb/bin/openmldb --host 127.0.0.1 --port 6527 < reproduce.sql
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < reproduce.sql
```

Ensure that the replication script is functional locally to reproduce the issue, and then document the issue or forward it to us for further assistance.

```{caution}
Please be aware that offline jobs default to asynchronous mode. If you intend to import and query offline, remember to set it to synchronous mode. For additional information, consult Offline Command Configuration Details. Without this adjustment, querying before the import is completed will not yield meaningful results.
```

## Provide configuration and logs for technical support

If your SQL execution issue cannot be replicated through replication scripts or if it's not related to SQL execution but rather a cluster management problem, we kindly request that you provide us with configuration details and logs from both the client and server for further investigation.

Whether you are using Docker or a local cluster setup (where all processes are on the same server), you can swiftly gather configuration, log files, and other information using diagnostic tools.

You can initiate the OpenMLDB server using either the `init.sh`/`start-all.sh` command for clustered versions or the `init.sh standalone`/`start-standalone.sh` command for standalone versions. After starting the server, you can employ the following commands, which correspond to clustered and standalone versions, respectively.

```
openmldb_tool --env=onebox --dist_conf=cluster_dist.yml
openmldb_tool --env=onebox --dist_conf=standalone_dist.yml
```

For clustered versions:

- Utilize `cluster_dist.yml`, which can be located in the `/work/` directory within the Docker container.
- Alternatively, you can copy the yml file from the [GitHub directory](https://github.com/4paradigm/OpenMLDB/tree/main/demo) for your use.

If you are working with a distributed cluster, it's essential to have SSH password-free configuration in place for smooth usage of the diagnostic tools. Please refer to the [Diagnostic Tool documentation](https://chat.openai.com/maintain/diagnose.md) for guidance on setting this up.

If your environment doesn't allow for SSH password-free configuration, please manually collect the configuration details and logs as needed.