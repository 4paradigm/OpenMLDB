# High Availability and Recovery

## Best Practice

For production environment, the following configurations is recommended for high availability:

| Components / Parameters   | Deployment Count | Description                                                         |
| ----------- | ---- | ------------------------------------------------------------ |
| Nameserver  | = 2  | Determined at deployment time                                                   |
| TaskManager | = 2  | Determined at deployment time                                                   |
| Tablet      | >= 3 | Determined at deployment time, can be scaled                                        |
| Physical nodes    | >= 3 | Determined at deployment time, recommend to be the same as tablet, can bot scaled                 |
| Replica count    | = 3  | Specified when `CREATE TABLE`, (if tablet count >= 3，replica count defaults to be 3) |
| Daemon    | in use | Determined at deployment time (see [Install and Deploy](../deploy/install_deploy.md) - Daemon Startup Method) |

Best practice illustration:

- Nameserver and TaskManager each require only one instance during system runtime, and instances that go offline unexpectedly will be automatically restarted by the daemon process. Therefore, deploying two instances is generally sufficient.
- The number of Tablets and replicas together determine the high availability of data and services. In typical scenarios, recommending three replicas is sufficient. Increasing the number of replicas can increase the probability of data integrity but is not recommended due to the corresponding increase in storage resources. The number of Tablets should ideally match the number of physical nodes, with at least three of each.
- OpenMLDB's high availability mechanism is based on data shards as the smallest unit. In the optimal configuration, each shard has three replicas, distributed across three different Tablets, including one primary shard (leader) and two secondary shards (followers). The impact on high availability for a shard with offline tablets can vary, as summarized in the table below:

| Number of Offline Tablets for the Shard | Service Availability | Data Integrity of the Shard                                           | Automatic Recovery of Offline Tablets (non-machine restart) |
| ---------------------------- | ---------- | ------------------------------------------------------------ | ---------------------------------- |
| 1                            | ✓          | - If the offline tablet is a follower, data integrity is maintained<br />- If the offline tablet is the primary and there is write traffic, a small amount of data loss may occur (some written data may not be synchronized to followers in time) | ✓                                  |
| 2                            | ✓          | - If all offline tablets are followers, data integrity is maintained<br />- If one offline tablet is the primary and there is write traffic, a small amount of data loss may occur | ✘                                  |
| 3                            | ✘          | ✘                                                            | ✘                                  |

- Tablet offline situations can include machine restarts, network disconnections, process terminations, etc. If it's a machine restart, manual execution of the OpenMLDB server startup script is required for recovery. For other situations, automatic recovery can be determined based on the table above.
- **Since a tablet generally stores multiple data shards and must have a primary shard, summarizing for the entire database: (1) when one tablet goes offline, the service is not affected, but if there is write traffic, there may be a small amount of data loss, and the offline tablet can recover automatically; (2) if two tablets go offline simultaneously, the service and data integrity cannot be guaranteed.**
- Regarding the concepts and architectural design of tablets, replicas, and shards, refer to the [Online Module Architecture documentation](../reference/arch/online_arch.md).

Additionally, if high availability requirements are low, and machine resources are limited, reducing the number of replicas and tablets can be considered:

- 1 replica, 1 tablet: No high availability mechanism.
- 2 replicas, 2 tablets: (1) when one tablet goes offline, service is not affected, and there may be a small amount of data loss, but the offline tablet can recover automatically; (2) if both tablets go offline simultaneously, neither data integrity nor service can be guaranteed.

## Node Recovery

OpenMLDB's high availability can be configured in automatic mode or manual mode. In automatic mode, the system automatically performs failover and data recovery when a node goes down and recovers. Otherwise, manual intervention is required. The configuration mode can be switched by modifying the `auto_failover` setting, which is enabled by default. You can check the configuration status and modify it using the following methods:

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> confget
  key                 value
-----------------------------
  auto_failover       true
> confset auto_failover false
set auto_failover ok
>confget
  key                 value
-----------------------------
  auto_failover       false
```

**Note, when auto_failover is enabled, if a node goes offline, the is_alive status of the showtable command will change to no. If the node contains a leader of a partition, the partition will re-select the leader. **

```
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  leader    0min       no        kNoCompress    0        0           0.000
  flow    4   0    172.27.128.33:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       no        kNoCompress    0        0           0.000
```

When auto_failover is turned off, manual operations are required for node offline and recovery. There are two commands `offlineendpoint` and `recoverendpoint`.

### Node Offline Command `offlineendpoint`

If the node fails, you need to execute `offlineendpoint` to offline the node, the command format is:

```
offlineendpoint $endpoint
```
`$endpoint` is the endpoint of the failed node. This command will offline the node and perform the following operations on all partitions under the node:

* If it is the leader, execute the re-election of the leader
* If it is a follower, find the leader and delete the current endpoint replica from the leader

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> showtablet
  endpoint            state           age
-------------------------------------------
  172.27.128.31:8541  kTabletHealthy  3h
  172.27.128.32:8541  kTabletOffline  7m
  172.27.128.33:8541  kTabletHealthy  3h
> offlineendpoint 172.27.128.32:8541
offline endpoint ok
>showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  leader    0min       no        kNoCompress    0        0           0.000
  flow    4   0    172.27.128.33:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       no        kNoCompress    0        0           0.000
```

After executing `offlineendpoint`, each partition will be assigned a new leader. If a partition fails to execute, you can execute `changeleader` on this partition alone. The command format is: `changeleader $table_name $pid` (refer to [documentation](cli.md#changeleader))

### Node Recovery Command `recoverendpoint`

If the node has recovered, you can execute `recoverendpoint` to recover the data. Command format:

```
recoverendpoint $endpoint
```

`$endpoint` is the endpoint of the node whose status has changed to `kTabletHealthy`.

```
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> showtablet
  endpoint            state           age
-------------------------------------------
  172.27.128.31:8541  kTabletHealthy  3h
  172.27.128.32:8541  kTabletHealthy  7m
  172.27.128.33:8541  kTabletHealthy  3h
> recoverendpoint 172.27.128.32:8541
recover endpoint ok
> showopstatus
  op_id  op_type              name  pid  status  start_time      execute_time  end_time        cur_task
-----------------------------------------------------------------------------------------------------------
  54     kUpdateTableAliveOP  flow  0    kDone   20180824195838  2s            20180824195840  -
  55     kChangeLeaderOP      flow  0    kDone   20180824200135  0s            20180824200135  -
  56     kOfflineReplicaOP    flow  1    kDone   20180824200135  1s            20180824200136  -
  57     kUpdateTableAliveOP  flow  0    kDone   20180824200212  0s            20180824200212  -
  58     kRecoverTableOP      flow  0    kDone   20180824200623  1s            20180824200624  -
  59     kRecoverTableOP      flow  1    kDone   20180824200623  1s            20180824200624  -
  60     kReAddReplicaOP      flow  0    kDoing  20180824200624  4s            -               kLoadTable
  61     kReAddReplicaOP      flow  1    kDoing  20180824200624  4s            -               kLoadTable
```

Execute `showopstatus` to view the running progress of the task. If the status is `kDoing`, the task has not been completed.


```
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> showopstatus
  op_id  op_type              name  pid  status  start_time      execute_time  end_time        cur_task
---------------------------------------------------------------------------------------------------------
  54     kUpdateTableAliveOP  flow  0    kDone   20180824195838  2s            20180824195840  -
  55     kChangeLeaderOP      flow  0    kDone   20180824200135  0s            20180824200135  -
  56     kOfflineReplicaOP    flow  1    kDone   20180824200135  1s            20180824200136  -
  57     kUpdateTableAliveOP  flow  0    kDone   20180824200212  0s            20180824200212  -
  58     kRecoverTableOP      flow  0    kDone   20180824200623  1s            20180824200624  -
  59     kRecoverTableOP      flow  1    kDone   20180824200623  1s            20180824200624  -
  60     kReAddReplicaOP      flow  0    kDone   20180824200624  9s            20180824200633  -
  61     kReAddReplicaOP      flow  1    kDone   20180824200624  9s            20180824200633  -
> showtable
>showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.33:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       yes       kNoCompress    0        0           0.000
```

If `showtable` turns to yes, it means that the recovery has been successful. If some partitions fail to recover, you can execute `recovertable` separately. The command format is: `recovertable $table_name $pid $endpoint` (refer to [documentation](cli.md#recovertable)).

**Note: `offlineendpoint` must be executed once before `recoverendpoint` is executed**

## One-click recovery

If the cluster is restarted after being offline at the same time, autofailover cannot automatically restore data, and a one-click recovery script needs to be executed, to execute command `recoverdata`. More details refer [here](./openmldb_ops.md). An example is shown as follows:

```bash
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=recoverdata
```

## Manual data recovery

Applicable scenario: The nodes where all replicas of a partition are located are down.
If one-click recovery has failed, try manual data recovery.

### 1 Start the process of each node
### 2 Turn off autofailover

Execute the confset command on the ns client
```
confset auto_failover false
```

### 3 Recover data

When restoring data manually, it is necessary to restore the data partition by partition. The recovery steps are as follows:

1. Execute showtable table name with ns client
2. Modify the table partition alive status to no, execute it with nsclient. updatetablealive table_name pid endppoint is_alive
    ```
    updatetablealive t1 * 172.27.2.52:9991 no
    ```
3. Perform the following steps for each partition in the table:
    * Restore the leader. If there are multiple leaders, choose the one with the larger offset
        * Use the tablet client to connect to the node where the leader is located, and execute the loadtable command. loadtable tablename tid pid ttl seg_cnt, seg_cnt is 8
            ```
            loadtable test 1 1 144000 8
            ```
        * To check the load progress, execute gettablestatus to check the load progress. gettablestatus tid pid. Wait for stat to change from TableLoading state to TableNormal state
            ```
            gettablestatus 1 1
            ```
        * Modify the leader alive status to yes. Execute with nsclient
            ```
            updatetablealive table_name pid 172.27.2.52:9991 yes
            ```
    * Use recovertable other replicas, execute recovertable table_name pid endpoint with nsclient
        ```
        recovertable table1 1 172.27.128.31:9527 
        ```

### 4 Restore autofailover
Execute `confset` command on the ns client
```
confset auto_failover true
```