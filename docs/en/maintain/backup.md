# Backup and Restore

## High Availability Description

* The nameserver nodes cannot all fail. Failover and recovery cannot be done if all fail.
* High availability is not guaranteed in the case of no secondary replica
* In the case of two replicas and two nodes, only one node of tablet can fail.
* In the case of two replicas and multiple nodes, the two tablet nodes where the same partition is located cannot fail at the same time.
* In the case of three replicas and three nodes, a tablet failure node can automatically perform failover and data recovery. Failure of two tablet nodes does not guarantee automatic failover and data recovery, but reads and writes can return to normal within minutes.
* In the case of three replicas and multiple nodes, if the two tablets where the same partition is located hangs, the automatic failover and data recovery of the partition are not guaranteed, but the read and write can return to normal within minutes.
* If there is a leader partition in the failed node and there is always write traffic, a small amount of data may be lost.

## Downtime and Recovery

OpenMLDB high availability can be configured in automatic mode and manual mode. In automatic mode, if the node fails and recovers, the system will automatically perform failover and data recovery, otherwise manual processing is required

The mode can be switched by modifying the auto_failover configuration. The default is to open the automatic mode. You can get the configuration status and modify the configuration in the following ways

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

**When auto_failover is enabled, if a node goes offline, the is_alive status of the showtable command will change to no. If the node contains a leader of a partition, the partition will re-select the leader. **

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

When auto_failover is turned off, manual operations are required for node offline and recovery. There are two commands offlineendpoint and recoverendpoint

If the node fails, you need to execute offlineendpoint to offline the node

Command format: offlineendpoint endpoint

endpoint is the endpoint of the failed node. This command will offline the node and perform the following operations on all partitions under the node:

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

After executing offlineendpoint, each partition will be assigned a new leader. If a partition fails to execute, you can execute changeleader on this partition alone. The command format is: changeleader table_name pid

If the node has recovered, you can execute recoverendpoint to recover the data

Command format: recoverendpoint endpoint

endpoint is the endpoint of the node whose status has changed to healthy

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

Execute showopstatus to view the running progress of the task. If the status is in the doing state, the task has not been completed.


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

If showtable turns to yes, it means that the recovery has been successful. If some partitions fail to recover, you can execute recovertable separately. The command format is: recovertable table_name pid endpoint

**Note: offlineendpoint must be executed once before recoverendpoint is executed**

### One-click recovery

If the cluster is restarted after being offline at the same time, autofailover cannot automatically restore data, and a one-click recovery script needs to be executed. More details refer [here](./openmldb_ops.md)

### Manual data recovery

Applicable scenario: The nodes where all replicas of a partition are located are down.

#### 1 Start the process of each node
#### 2 Turn off autofailover

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

#### 4 Restore autofailover
Execute the confset command on the ns client
```
confset auto_failover true
```