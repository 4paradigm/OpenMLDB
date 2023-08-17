# Offline data synchronization

Offline data synchronization involves taking online data and synchronizing it with an offline location. The offline location refers to a persistent storage location with a large capacity, which users can specify according to their needs. It is not limited to the offline data location in the OpenMLDB table, which only supports writing to an HDFS cluster.

To enable the offline synchronization functionality, two types of components need to be deployed, DataCollector and SyncTool. In Phase 1, only a single SyncTool is supported, while DataCollector needs to be deployed on at least one TabletServer per machine. It is possible to have multiple TabletServers on a single machine, and the synchronization task will utilize the DataCollector on that particular machine. If additional DataCollectors are added, they will not function until the currently running DataCollector is offline, at which they will take over its role and continue the synchronization process.

Although SyncTool supports single-unit operations, it can resume the work progress without requiring any additional actions when it restarts.

## Deployment method

To ensure proper functionality, it is important to note that SyncTool might attempt to allocate synchronization tasks even if there is no active DataCollector if it is started first. Therefore, it is recommended to start all the DataCollectors before starting SyncTool.

To begin, download and extract the deployment package or version 0.7.3 from the GitHub release or image website. It is crucial not to use an older version of TabletServer for synchronization purposes.

### DataCollector

#### Configure (Important)

To make the necessary updates, locate the `<package>/conf/data_collector.flags` configuration document. Ensure that you enter the correct ZooKeeper (zk) location and path in the configuration. Additionally, make sure that the 'endpoint' configuration does not conflict with any ports. It is crucial for the endpoint to be consistent with the TabletServer configuration. If the TabletServer uses the local public IP and the DataCollector endpoint uses the 127.0.0.1 address, automatic conversion will not occur.

Pay close attention when selecting the `collector_datadir` parameter. Since we will be hard linking the disk data of the TabletServer during synchronization, the `collector_datadir` should point to the data location of the TabletServer's `hdd_root_path`/`ssd_root_path` on the same disk. Otherwise, an error message stating `Invalid cross-device link` will be displayed.

The default value for `max_pack_size` is set to 1MB. If there are numerous synchronization tasks, you may encounter the error message `[E1011]The server is overcrowded`. In such cases, it is recommended to appropriately reduce this configuration. Additionally, you can adjust the `socket_max_unwritten_bytes` parameter to increase the write cache tolerance.

#### Start

```
./bin/start.sh start data_collector
```
#### Confirm state

Use the command after starting to get the real-time DataCollector RPC status page. If it fails to show, please check the log.
```
curl http://<data_collector>/status
```
Currently, it is not possible to directly query the list of `DataCollector` instances. However, support for this functionality will be included in future updates of the relevant tools.

### SyncTool

#### Configuration
- To perform the necessary updates, please modify the `<package>/conf/synctool.properties` configuration file. This will override the settings specified in `<package>/synctool/conf/synctool.properties` when starting the tool.
- Currently, the SyncTool only supports direct writing to HDFS. To configure the HDFS connections, you can modify the `hadoop.conf.dir` parameter in the properties file or set the `HADOOP_CONF_DIR` environment variable. Ensure that the OS startup user of SyncTool has write access to the HDFS path specified when creating each synchronization task.

  Once the configurations have been updated, you can proceed with starting the tool.

#### Start
```
./bin/start.sh start synctool
```

Currently, SyncTool operates as a single process. If multiple instances are started, they operate independently of each other. It is the user's responsibility to ensure that there are no duplicate synchronization tasks among them. SyncTool saves the progress of the processes in real-time, allowing for immediate recovery if a process goes offline.

SyncTool manages synchronization tasks, collects data, and writes it to offline locations. Let's start with the task relationship. When a user submits a "table synchronization task," SyncTool divides it into multiple "sharded synchronization tasks" (referred to as subtasks) for creation and management.

In task management, if a DataCollector instance goes offline or encounters an error, SyncTool will request it to recreate the task. If a suitable DataCollector is not available for task reassignment, the task will be marked as failed. SyncTool will continue to attempt task recreation, which can result in slow synchronization progress and make it difficult to identify errors. Therefore, in order to better identify errors, this scenario is marked as a failed task.

Since the creation of a table synchronization task does not support starting from a specific point, it is not advisable to delete and recreate the task. Doing so may result in duplicate data if the destination is the same. Instead, consider changing the synchronization destination or restarting SyncTool. During the recovery process, SyncTool does not check if the subtask has previously failed, but rather starts the task in an "init" state.

#### SyncTool Helper

For create, delete, and query synchronizing tasks, please use `<package>/tools/synctool_helper.py`.

```bash
# create 
python tools/synctool_helper.py create -t db.table -m 1 -ts 233 -d /tmp/hdfs-dest [ -s <sync tool endpoint> ] 
# delete
python tools/synctool_helper.py delete -t db.table [ -s <sync tool endpoint> ] 
# task status
python tools/synctool_helper.py status [ -s <sync tool endpoint> ] 
# sync tool status for dev
python tools/synctool_helper.py tool-status [ -f <properties path> ]
```


To configure the Mode setting, you can choose between 0, 1, or 2. Each mode corresponds to a different type of synchronization:

- Mode 0: Full synchronization (FULL)
- Mode 1: Incremental synchronization based on time (INCREMENTAL_BY_TIMESTAMP). Use `-ts` to configure the start time, and data prior to this time will not be synchronized.
- Mode 2: Full and continuous incremental synchronization (FULL_AND_CONTINUOUS)

In Mode 0, there is no strict ending time point. Each subtask will synchronize the current data and then terminate, deleting the entire table task. If a helper is used to query the status and no synchronization task is found for the table, it indicates that the synchronization task for that table has been completed. However, Mode 1 and 2 will continue running indefinitely.

When executing the `status` command, the status of each partition task will be displayed. If you only need an overview, you can focus on the content after the "table scope," which shows the synchronization task status at the table level. If there are any `FAILED` subtasks for a table, it will be indicated.

Pay attention to the `status` field for each subtask. If it has just started and hasn't received the first data synchronization from the DataCollector, it will be in the INIT state. Once it receives the first data synchronization, it will change to the RUNNING state. When SyncTool restarts, if the synchronization task resumes, it will enter the RUNNING state directly. The task may enter the REASSIGNING state temporarily during the process, which is an intermediate state and doesn't mean the task is no longer available. In Mode 0, a SUCCESS status may appear, indicating that the task has been completed. Once all subtasks for a table are completed, SyncTool will automatically clean up the tasks for that table. If tasks cannot be found in the table when using a helper to query, it means they have been removed.

Only the `FAILED` status indicates that the subtask has failed and will not be retried or deleted automatically. After identifying the cause of the failure and fixing it, you can delete and rebuild the synchronization task. If you don't want to lose the progress that has been imported, you can restart SyncTool to allow it to continue the recovery task and resume synchronization. However, be aware that this may result in duplicate data.

## Functional boundaries


The snapshot progress of the table in the DataCollector does not have a unique marker. If a subtask shuts down midway and creates a task with the current progress, it may result in a segment of duplicate data.

In SyncTool, the progress is first written to HDFS and then persisted. If SyncTool shuts down during this process, and the progress has not been updated, a segment of data may be repeatedly written. Due to the inherent possibility of duplication in this scenario, no additional actions will be taken at this time.
