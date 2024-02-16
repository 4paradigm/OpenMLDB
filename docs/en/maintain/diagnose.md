# Diagnostic Tool

## Overview

OpenMLDB provides a diagnostic tool to diagnose problems conveniently for users. 

## Usage

Installation and usage:
```bash
pip install openmldb-tool # openmldb-tool[pb]
openmldb_tool # note the underscore
```
The following subcommands can be used:
```bash
usage: openmldb_tool [-h] [--helpfull] {status,inspect,rpc,test,static-check} ...
```
Note, for `-c/--cluster`, the format is`<zk_cluster>/<zk_root_path>`, by default is `127.0.0.1:2181/openmldb`. If a different OpenMLDB cluster is used, please specify this parameter. Other settings are different as respect to each sub-command, you can use `-h` to check the detailed documentation. 

### One-click Inspect
The openmldb_tool inspect [--cluster=0.0.0.0:2181/openmldb] command provides a comprehensive cluster status report in a single query. If a more specific perspective or additional diagnostic functionality is needed, additional subcommands can be used.

The report is divided into several sections. The "Ops and Partitions" sections will not be displayed if all tables are healthy. Users should first check the "Summary & Hint" at the end of the report. If there are instances marked as "server offline" (in red), it is necessary to restart the servers, especially TabletServers, to ensure they are online. After restarting the servers, the cluster may attempt automatic repairs, but this process could fail. Therefore, it is advisable to wait for some time before running the inspection again. If there are still unhealthy tables, users can check their status. Fatal tables need immediate attention as they may experience read and write failures. For Warn tables, users may consider postponing repairs. The repair procedures are detailed in the documentation provided at the end of the report.

The inspect command supports various configuration parameters. In addition to `--cluster/-c`, it can be configured to disable color display using `--nocolor/-noc` for easy copying. The `--table_width/-tw n` parameter allows configuring the table width, and `--offset_diff_thresh/-od n` sets the threshold for offset diff alarms.

```
diagnosing cluster xxx


Server Detail
{server map}
{server online/offline report}


Table Partitions Detail
tablet server order: {tablet ip -> idx}
{partition tables of unhealthy tables}
Example:
{a detailed description of partition table}


Ops Detail
> failed ops do not mean cluster is unhealthy, just for reference
last one op(check time): {}
last 10 ops != finished:
{op list}



==================
Summary & Hint
==================
Server:

{online | offline servers ['[tablet]xxx'], restart them first}

Table:
{all healthy | unhealthy tables desc}
[]Fatal/Warn table, {read/write may fail or still work}, {repair immediatly or not}
{partition detail: if leader healthy, if has unhealthy replicas, if offset too large, related ops}

    Make sure all servers online, and no ops for the table is running.
    Repair table manually, run recoverdata, check https://openmldb.ai/docs/zh/main/maintain/openmldb_ops.html.
    Check 'Table Partitions Detail' above for detail.
```

### Other Common Commands

In addition to the one-click inspect command, in the following scenarios, we recommend using diagnostic tool subcommands to help users assess cluster status and simplify operations:

- After deploying the cluster, you can use `test` to check if the cluster is working correctly without the need for manual testing. If issues are identified, then use `inspect` for further diagnosis.
- If all components are online but timeouts or error messages indicate a specific component cannot connect, use `status --conn` to check the connections to each component, printing out the simple access time. It can also be used to test the connection between the client host and the cluster to promptly detect network isolations.
- When encountering issues with offline jobs, `SHOW JOBLOG id` can be used to view logs. However, users with limited experience may find irrelevant information in the logs. In such cases, `inspect job` can be used to extract key information from job logs.
- When there are too many offline jobs and the display in the CLI becomes difficult to read, use `inspect offline` to filter all failed jobs or `inspect job --state <state>` to filter jobs with a specific state.
- In more challenging problems, users may need to obtain information through RPC to help identify issues. `openmldb_tool rpc` can assist users in quickly and easily invoking RPC, reducing the operational threshold.
- Without Prometheus monitoring, use `inspect online --dist` to obtain data distribution information.
- If your operational nodes have passwordless SSH access to machines hosting various components, you can use `static-check` to verify the correctness of configuration files, ensure version uniformity, and avoid deployment failures. It can also collect logs from the entire cluster with one command, making it convenient to package and provide to developers for analysis.

## Subcommand Details

### `status`
The `status` command is used to view the status of the OpenMLDB cluster, including the addresses, roles, connection times, and states of the service components. It is equivalent to `SHOW COMPONENTS`. If you notice abnormal behavior in the cluster, check the real-time status of each service component using this command first.

```
openmldb_tool status -h
usage: openmldb_tool status [-h] [--helpfull] [--diff]

optional arguments:
  -h, --help  show this help message and exit
  --helpfull  show full help message and exit
  --diff      check if all endpoints in conf are in cluster. If set, need to set `-f,--conf_file`
  --conn                check network connection of all servers
```

- Simple query for cluster status:
  ```
  openmldb_tool status [--cluster=...]
  ```
  The output will be similar to the table below:
  ```
  +-----------------+-------------+---------------+--------+---------+
  |     Endpoint    |     Role    |  Connect_time | Status | Ns_role |
  +-----------------+-------------+---------------+--------+---------+
  | localhost:10921 |    tablet   | 1677398926974 | online |   NULL  |
  | localhost:10922 |    tablet   | 1677398926978 | online |   NULL  |
  |  localhost:7527 |  nameserver | 1677398927985 | online |  master |
  |  localhost:9902 | taskmanager | 1677398934773 | online |   NULL  |
  +-----------------+-------------+---------------+--------+---------+
  ```

- Check and test the cluster connections and versions:
  ```
  openmldb_tool status --conn
  ```

#### Check if Configuration Files Match Cluster State
If the `--diff` parameter is specified, it will check if all nodes in the configuration file are part of the already started cluster. If there are nodes in the configuration file that are not in the cluster, it will output exception information. If there are nodes in the cluster that are not in the configuration file, it will not output exception information. You need to specify `-f, --conf_file`. For example, you can perform this check in the image as follows:
```bash
openmldb_tool status --diff -f=/work/openmldb/conf/hosts
```

### `inspect`
If you want to check the cluster status, it is recommended to use the one-click `inspect` command to obtain a comprehensive cluster inspection report. The `inspect` subcommands are more targeted for specific checks.

```
openmldb_tool inspect -h
usage: openmldb_tool inspect [-h] [--helpfull] {online,offline,job} ...

positional arguments:
  {online,offline,job}
    online              only inspect online table.
    offline             only inspect offline jobs.
    job                 show jobs by state, show joblog or parse joblog by id.
```

#### `online`
The `inspect online` command checks the health status of online tables and outputs tables with anomalies, including table status, partition information, replica information, etc. It is equivalent to `SHOW TABLE STATUS` and filters out tables with anomalies.

##### Check Online Data Distribution

You can use `inspect online --dist` to check the distribution of online data. By default, it checks all databases, and you can use `--db` to specify the database to check. To query multiple databases, use ',' to separate the database names. It will output the data distribution across nodes for each database.

#### `offline`

The `inspect offline` command checks for tasks with a final status of failure (it does not check tasks in "running" status). It is equivalent to `SHOW JOBS` and filters out failed tasks. More features to be added.

#### JOB Inspection

Job inspection is a more flexible offline task inspection command that allows filtering jobs based on conditions or analyzing logs for individual jobs.

##### Filter by state

You can use `inspect job` or `inspect job --state all` to query all tasks, equivalent to `SHOW JOBS` and sorted by job_id. Using `inspect job --state <state>` allows you to filter logs for specific states, and you can use ',' to separate and query logs for different states simultaneously. For example, `inspect offline` is equivalent to `inspect job --state failed,killed,lost`, filtering out all failed tasks.


Here are some common states:

| state    | Description           |
| -------- | --------------------- |
| finished | Successfully completed task |
| running  | Task currently running      |
| failed   | Failed task                 |
| killed   | Terminated task             |

For more information on state, please check[Spark State]( https://spark.apache.org/docs/3.2.1/api/java/org/apache/spark/launcher/SparkAppHandle.State.html), [Yarn State](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/yarn/api/records/YarnApplicationState.html).

##### Individual Job Logs

Use `inspect job --id <job_id>` to query the logs of a specific task. The result will filter out the main error messages using the configuration file.

Parsing relies on the configuration file, which is automatically downloaded by default. If you need to update the configuration file, you can use `--conf-update`, which will force a download of the configuration file before parsing. If the default download source is not suitable, you can also configure `--conf-url` to set the mirror source, for example, using `--conf-url https://openmldb.ai/download/diag/common_err.yml` to configure a domestic mirror.

If you only need the complete log information without parsing the results, you can use `--detail` to get detailed information without printing the parsing results.

### `test` 

The `test` command executes some test SQL statements, including: creating a database, creating a table, inserting data online, querying data online, deleting a table, and deleting a database. If the TaskManager component is present, it will also execute offline query tasks. Since no offline data import is performed, the query results should be empty.

You can use `test` to check if the cluster is working properly, especially after setting up a new cluster or if you notice abnormal cluster behavior.

### `static-check` 

The `static-check` command performs a static check based on the cluster deployment configuration file (specified by the `-f, --conf_file` parameter). It logs in to the deployment addresses of various service components, collects version information, configuration files, and log files, checks if the versions are consistent, and analyzes the collected configuration and log files. This check can be performed before deploying the cluster to avoid cluster deployment failures due to program version or configuration file errors. It can also be used when the cluster is in an abnormal state to collect distributed log files for easier troubleshooting.

```bash
openmldb_tool static-check -h
usage: openmldb_tool static-check [-h] [--helpfull] [--version] [--conf] [--log]

optional arguments:
  -h, --help     show this help message and exit
  --helpfull     show full help message and exit
  --version, -V  check version
  --conf, -C     check conf
  --log, -L      check log
```

#### Deployment Configuration File
The `-f, --conf_file` deployment configuration file can be in either hosts or YAML style, describing which components are in the cluster, on which nodes they are distributed, and in which deployment directories.
- Hosts Style (refer to the conf/hosts file in the release package)
```
[tablet]
localhost:10921 /tmp/openmldb/tablet-1
localhost:10922 /tmp/openmldb/tablet-2

[nameserver]
localhost:7527

[apiserver]
localhost:9080

[taskmanager]
localhost:9902

[zookeeper]
localhost:2181:2888:3888 /tmp/openmldb/zk-1
```
- Cluster yaml
```yaml
mode: cluster
zookeeper:
  zk_cluster: 127.0.0.1:2181
  zk_root_path: /openmldb
nameserver:
  -
    endpoint: 127.0.0.1:6527
    path: /work/ns1
tablet:
  -
    endpoint: 127.0.0.1:9527
    path: /work/tablet1
  -
    endpoint: 127.0.0.1:9528
    path: /work/tablet2
taskmanager:
  -
    endpoint: 127.0.0.1:9902
    path: /work/taskmanager1
```
If it's a distributed deployment, the diagnostic tool needs to fetch files from the deployment nodes, so it requires setting up passwordless SSH between machines. You can follow the instructions [here](https://www.itzgeek.com/how-tos/linux/centos-how-tos/ssh-passwordless-login-centos-7-rhel-7.html).

If some components in the hosts/yaml file are not configured with a path, the tool will use `--default_dir` as the deployment directory, with a default value of `/work/openmldb`. If your deployment directory is different, you can specify it using `--default_dir`.

For onebox deployment, you can specify `--local`, and all nodes will be treated as local nodes without attempting SSH login. If only some nodes are local, you can only use the YAML format for the deployment configuration file, and configure local nodes with `is_local: true`. For example:

```yaml
nameserver:
  -
    endpoint: 127.0.0.1:6527
    path: /work/ns1
    is_local: true
```

#### Check Content

The check can be specified by combining flags to indicate which content to check. For example, `-V` checks only the version, `-CL` checks only the configuration files and logs, and `-VCL` checks everything.

- `-V, --version`: Checks the version, ensuring that the versions of various components are consistent. If inconsistent, it will output information about the inconsistent components and versions (due to the higher complexity, the address of the openmldb-batch package may not be found and will be ignored in the check; checking the batch package can be postponed, and replacement is straightforward).
- `-C, --conf`: Collects configuration files, checking if the ZooKeeper addresses in the configuration files of various components are consistent, etc.
- `-L, --log`: Collects logs, outputting logs with WARNING and above.

If checking configuration files or logs, the collected files will be saved in `--collect_dir`, which defaults to `/tmp/diag_collect`. You can also access this directory to view the collected configurations or logs for further analysis.

#### Check Example

In a containerized image, you can perform static checks as follows:
```bash
openmldb_tool static-check --conf_file=/work/openmldb/conf/hosts -VCL --local
```

### RPC Interface

`openmldb_tool` also provides an RPC interface, making it easier to send RPCs without needing to locate the server's IP, concatenate RPC method URL paths, and obtain information about all RPC methods and their input structures. The usage is `openmldb_tool rpc`, for example, `openmldb_tool rpc ns ShowTable --field '{"show_all":true}'` can invoke the `ShowTable` interface of the `nameserver` to get information about the table's state.

In the case of components, you can use role names directly instead of IP addresses. There is only one active NameServer and TaskManager, so we use `ns` and `tm` to represent these two components. However, there are multiple TabletServers, and we can use `tablet1`, `tablet2`, etc., to specify a particular TabletServer, starting from 1. The order can be viewed through `openmldb_tool rpc` or `openmldb_tool status`.

If you are not familiar with the methods or input parameters of the RPC service, you can use `openmldb_tool rpc <component> [method] --hint` to view the help information. However, this is an additional component and needs to be installed through `pip install openmldb-tool[pb]`. The hint also requires additional pb files for parsing input parameters, which are read from `/tmp/diag_cache` by default. If it does not exist, it will be automatically downloaded. If you already have the corresponding files or have manually downloaded them, you can specify the directory with `--pbdir`. For self-compilation of pb files, refer to the [OpenMLDB tool development documentation](https://github.com/4paradigm/OpenMLDB/blob/main/python/openmldb_tool/README.md#rpc).

For example:
```bash
$ openmldb_tool rpc ns ShowTable --hint
...
server proto version is 0.7.0-e1d35fcf6
hint use pb2 files from /tmp/diag_cache
You should input json like this, ignore round brackets in the key and double quotation marks in the value: --field '{
    "(optional)name": "string",
    "(optional)db": "string",
    "(optional)show_all": "bool"
}'
```

## Additional Information

You can use `openmldb_tool --helpfull` to view all configuration options. For example, `--sdk_log` can print SDK logs (zk, glog) for debugging purposes.