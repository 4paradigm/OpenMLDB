# Online-offline data synchronization

Online-offline data synchronization involves taking online data and synchronizing it to an offline location. The offline location refers to a large capacity persistent storage location, which users can specify, and is not limited to the offline data location of OpenMLDB table. Currently, we only support disk table synchronization and support only writing to the HDFS cluster.

To enable online-offline synchronization, DataCollector and SyncTool are required to be deployed. For SyncTool, only a single deployment is supported. For DataCollector, it is required to be deployed on all the machines that have TabletServer deployed. For example, It is possible to have multiple TabletServers on a single machine, and the synchronization task will utilize the DataCollector deployed on that particular machine. If additional DataCollectors are deployed, they will not be in function until the currently running DataCollector is offline, after which another one will take over.

Although SyncTool supports single deployment, it can resume the work progress without requiring any additional actions when it restarts.

## Deployment method

To ensure proper functionality, it is important to note that SyncTool might attempt to allocate synchronization tasks even if there is no active DataCollector if it is started first. Therefore, it is recommended to start all the DataCollectors before starting SyncTool.

Online-offline synchronization requires the version of TabletServer to be newer than 0.7.3. It is crucial not to use an older version of TabletServer for synchronization purposes. DataCollector is in `bin` and SyncTool is in `synctool`. Both can be started by `bin/start.sh`.

### DataCollector

#### Configuration (Important)

Update `<package>/conf/data_collector.flags` configuration file. Ensure that you have entered the correct ZooKeeper (zk) location and path in the configuration. Additionally, make sure that the 'endpoint' configuration does not conflict with any ports. It is crucial for the endpoint to be consistent with the TabletServer configuration. If the TabletServer uses the local public IP and the DataCollector endpoint uses 127.0.0.1, automatic conversion will not occur.

Pay close attention when selecting the `collector_datadir`. Since we will be setting up hard link to the disk data of the TabletServer during synchronization, the `collector_datadir` should be on the same disk as the data location of the TabletServer's `hdd_root_path`/`ssd_root_path`. Otherwise, an error `Invalid cross-device link` will occur.

The default value for `max_pack_size` is set to 1M. If there are too many synchronization tasks, you may encounter the error message `[E1011]The server is overcrowded`. In such cases, it is recommended to reduce this parameter. Additionally, you can adjust the `socket_max_unwritten_bytes` parameter to increase the write cache tolerance.

#### Start

```
./bin/start.sh start data_collector
```
#### Check status

Run the command after starting to get the real-time DataCollector RPC status. If it fails to show, please check the log.
```
curl http://<data_collector>/status
```
Currently, it is not possible to directly query the list of `DataCollector` instances. However, support for this functionality will be included in future updates of the relevant tools.

### SyncTool

#### Configuration
- To update, please modify the `<package>/conf/synctool.properties` configuration file. This will overwrite `<package>/synctool/conf/synctool.properties` when start.
- Currently, the SyncTool only supports direct writing to HDFS. To configure the HDFS connections, you can modify the `hadoop.conf.dir` parameter in the properties file or set the `HADOOP_CONF_DIR` environment variable. Ensure that the OS startup user of SyncTool has write access to the HDFS path specified when creating each synchronization task.

  Once the configurations have been updated, you can proceed with starting the tool.

#### Start
```
./bin/start.sh start synctool
```

Currently, SyncTool operates as a single process. If multiple instances are started, they operate independently of each other. It is the user's responsibility to ensure that there are no duplicated synchronization tasks. SyncTool saves the progress of the processes in real-time, allowing for immediate recovery if a process goes offline.

SyncTool manages synchronization tasks, collects data, and writes it to offline locations. When a user submits a "table synchronization task", SyncTool divides it into multiple "sharded synchronization tasks" (referred to as subtasks) for creation and management.

In task management, if a DataCollector instance goes offline or encounters an error, SyncTool will request it to recreate the task. If a suitable DataCollector is not available for task reassignment, the task will be marked as failed. If we don't do this, SyncTool will continue to attempt task recreation, which can result in slow progress and make it difficult to identify errors. Therefore, in order to better identify errors, we mark the task as failed.

Since the creation of a table synchronization task does not support starting from a specific point, it is not advisable to delete and recreate the task. Doing so may result in duplicated data if the destination is the same. Instead, consider changing the synchronization destination or restarting SyncTool. During the recovery process, SyncTool does not check if the subtask has previously failed, but rather starts the task in an "init" state.

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

In Mode 0, there is no strict end time. Each subtask will synchronize the current data and then terminate, deleting the entire table task. If a helper is used to query the status and no synchronization task is found for the table, it indicates that the synchronization task for that table has been completed. However, Mode 1 and 2 will continue running indefinitely.

When executing the `status` command, the status of each partition task will be displayed. If you only need an overview, you can focus on the content after the `table scope`, which shows the synchronization task status at the table level. If there are any `FAILED` subtasks for a table, it will be indicated.

Pay attention to the `status` field for each subtask. If it has just started and hasn't received the first data synchronization from the DataCollector, it will be in the INIT state. Once it receives the first data synchronization, it will change to the RUNNING state. When SyncTool restarts, if the synchronization task resumes, it will enter the RUNNING state directly. The task may enter the REASSIGNING state temporarily during the process, which is an intermediate state and doesn't mean the task is no longer available. In Mode 0, a SUCCESS status may appear, indicating that the task has been completed. Once all subtasks for a table are completed, SyncTool will automatically remove the tasks, and the tasks of the table will not be found when using a helper to query.

Only the `FAILED` status indicates that the subtask has failed and will not be retried or deleted automatically. After identifying the cause of the failure and fixing it, you can delete and restart the synchronization task. If you don't want to lose the progress, you can restart SyncTool to allow it to continue the recovery task and resume synchronization. However, be aware that this may result in duplicated data.

## Functional boundaries
 
DataCollector does not mark the progress (snapshot progress) uniquely. If a subtask shuts down midway and a new task is created with the current progress, it may result in duplicated data.
In SyncTool, data is first written to HDFS and then the progress is saved. If SyncTool shuts down during this process, and the progress has not been updated, it may result in duplicated data.
