# Operations CLI

* [NS Client](#ns-client)
* [Tablet Client](#tablet-client)

## NS Client

Connecting to the NS Client requires specifying zk\_cluster, zk\_root\_path and role. Where zk\_cluster is the zk address, zk\_root\_path is the root path of the cluster in zk and role is the role to be started, and needs to be specified as ns_client

```bash
$ ./bin/openmldb --zk_cluster=172.27.2.52:12200 --zk_root_path=/onebox --role=ns_client
```

### use

The `use` command can switch to a database

```
> use demodb
```

### showtable

View all tables or specify a table

Command format: `showtable [table_name]`

```
172.24.4.55:6531 demo_db > showtable
   name tid pid endpoint role is_alive offset record_cnt memused diskused
-------------------------------------------------- -------------------------------------------------- --
  aaa 22 0 172.24.4.55:9971 leader yes 0 0 0.000 8.646 K
  auto_VCeOIIKA 25 0 172.24.4.55:9971 leader yes 8 4 498.000 9.128 K
  t1 21 0 172.24.4.55:9971 leader yes 1 1 301.000 9.353 K
172.24.4.55:6531 demo_db> showtable auto_VCeOIIKA
   name tid pid endpoint role is_alive offset record_cnt memused diskused
-------------------------------------------------- -------------------------------------------------- --
  auto_VCeOIIKA 25 0 172.24.4.55:9971 leader yes 8 4 498.000 9.128 K
```

### showtablet

View tablet information(if the serverName is specified and the local IP auto-configuration is enabled, then the endpoint is shown as the serverName and the real_endpoint is shown as "-")

```
> showtablet
  endpoint real_endpoint state age
-------------------------------------------------- ------------
  6534708411798331392 172.17.0.12:9531 kTabletHealthy 1d
  6534708415948603392 172.17.0.13:9532 kTabletHealthy 1d
  6534708420092481536 172.17.0.14:9533 kTabletHealthy 14h
```

### addreplica

Add replicas

Command format: `addreplica table_name pid_group endpoint`

* table\_name: the table name
* pid\_group: the collection of shard IDs. There can be the following situations
    * A single shard
    * Multiple shard IDs, separated by commas. Such as 1,3,5
    * A range of shard IDs with a closed interval; For example, 1-5 means shard 1, 2, 3, 4, 5
* endpoint: the endpoint of the node to be added as a replica

```
> showtable test1
  name tid pid endpoint role ttl is_alive compress_type offset record_cnt memused
-------------------------------------------------- -------------------------------------------------- --------
  test1 13 0 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 1 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 2 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 3 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
> addreplica test1 0 172.27.128.33:8541
AddReplica ok
> showtable test1
  name tid pid endpoint role ttl is_alive compress_type offset record_cnt memused
-------------------------------------------------- -------------------------------------------------- --------
  test1 13 0 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 0 172.27.128.33:8541 follower 0min yes kNoCompress 0 0 0.000
  test1 13 1 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 2 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 3 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
> addreplica test1 1,2,3 172.27.128.33:8541
AddReplica ok
> addreplica test1 1-3 172.27.128.33:8541
AddReplica ok
```

### delreplica

Delete replicas

Command format: `delreplica table_name pid_group endpoint`

* table\_name: the table name
* pid\_group: the collection of shard IDs. There can be the following situations
    * A single shard
    * Multiple shard IDs, separated by commas. Such as 1,3,5
    * A range of shard IDs with a closed interval; for example, 1-5 means shard 1, 2, 3, 4, 5
* endpoint: the endpoint of node to be deleted as a replica

```
> showtable test1
  name tid pid endpoint role ttl is_alive compress_type offset record_cnt memused
-------------------------------------------------- -------------------------------------------------- --------
  test1 13 0 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 0 172.27.128.33:8541 follower 0min yes kNoCompress 0 0 0.000
  test1 13 1 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 1 172.27.128.33:8541 follower 0min yes kNoCompress 0 0 0.000
  test1 13 2 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 2 172.27.128.33:8541 follower 0min yes kNoCompress 0 0 0.000
  test1 13 3 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 3 172.27.128.33:8541 follower 0min yes kNoCompress 0 0 0.000
> delreplica test1 0 172.27.128.33:8541
DelReplica ok
> showtable test1
  name tid pid endpoint role ttl is_alive compress_type offset record_cnt memused
-------------------------------------------------- -------------------------------------------------- --------
  test1 13 0 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 1 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 1 172.27.128.33:8541 follower 0min yes kNoCompress 0 0 0.000
  test1 13 2 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 2 172.27.128.33:8541 follower 0min yes kNoCompress 0 0 0.000
  test1 13 3 172.27.128.31:8541 leader 0min yes kNoCompress 0 0 0.000
  test1 13 3 172.27.128.33:8541 follower 0min yes kNoCompress 0 0 0.000
> delreplica test1 1,2,3 172.27.128.33:8541
DelReplica ok
> delreplica test1 1-3 172.27.128.33:8541
DelReplica ok
```

### migrate

Replicas migration

Command format: `migrate src_endpoint table_name pid_group des_endpoint`

* src\_endpoint: the endpoint of the node that needs to be checked out
* table\_name: the table name
* pid\_group:  the collection of shard IDs. There can be the following situations
    * A single shard
    * Multiple shard IDs, separated by commas. Such as 1,3,5
    * A range of shard IDs with a closed interval; for example, 1-5 means shard 1, 2, 3, 4, 5
* des\_endpoint: The endpoint of the destination node for migration

```
> migrate 172.27.2.52:9991 table1 1 172.27.2.52:9992
partition migrate ok
> migrate 172.27.2.52:9991 table1 1-5 172.27.2.52:9992
partition migrate ok
> migrate 172.27.2.52:9991 table1 1,2,3 172.27.2.52:9992
partition migrate ok
```

**Note**: A leader partition cannot be migrated. If you would like to do so, you have to use the command `changeleader` first to change the role of the leader to a follower.

### confget

Get configuration information, currently only supports auto\_failover

Command format: `confget [conf_name]`

* conf\_name: the configuration item name, optional

```
> confget
  key value
-----------------------------
  auto_failover false
> confget auto_failover
  key value
------------------------
  auto_failover false
```

### confsets

Modify configuration information, currently only supports auto\_failover

Command format: `confset conf_name value`

* conf\_name: the configuration item name
* value: the value set by the configuration item

```
> confset auto_failover true
set auto_failover ok
```

### offlineendpoint

Make a node offline. This command is asynchronous. After it succeeds, you can view the running status through `showopstatus`

Command format: `offlineendpoint endpoint [concurrency]`

* endpoint: this is the given endpoint to go offline. This command will perform the following operations on all shards under the node:
  * If it is a master, execute the re-election of the master
  * If it is a slave, find the master node and delete the current endpoint copy from the master node
  * Modify is_alive status to no
* concurrency: the concurrent number of task execution. This configuration is optional, the default is 2 (name_server_task_concurrency configuration can be configured), and the maximum value is the value configured by name_server_task_max_concurrency

```bash
> offlineendpoint 172.27.128.32:8541
offline endpoint ok
>showtable
  name tid pid endpoint role ttl is_alive compress_type offset record_cnt memused
-------------------------------------------------- -------------------------------------------------- ------------------
  flow 4 0 172.27.128.32:8541 leader 0min no kNoCompress 0 0 0.000
  flow 4 0 172.27.128.33:8541 follower 0min yes kNoCompress 0 0 0.000
  flow 4 0 172.27.128.31:8541 follower 0min yes kNoCompress 0 0 0.000
  flow 4 1 172.27.128.33:8541 leader 0min yes kNoCompress 0 0 0.000
  flow 4 1 172.27.128.31:8541 follower 0min yes kNoCompress 0 0 0.000
  flow 4 1 172.27.128.32:8541 follower 0min no kNoCompress 0 0 0.000
```

After the command is executed successfully, all shards will have leaders in the yes state

### recoverendpoint

Restore node data. This command is asynchronous and after the successful return, you can view the running status through showopstatus

Command format: `recoverendpoint endpoint [need_restore] [concurrency]`

* endpoint: the endpoint of the node to restore
* need_restore: whether the table topology is to be restored to the original state, this configuration is optional, the default is false. If set to true, a shard is the leader under this node, and it is still the leader after recoverendpoint is executed to restore data
* concurrency: the concurrent number of task execution. This configuration is optional, the default is 2 (name_server_task_concurrency configuration can be configured), and the maximum value is the value configured by name_server_task_max_concurrency

```
> recoverendpoint 172.27.128.32:8541
recover endpoint ok
> recoverendpoint 172.27.128.32:8541 true
recover endpoint ok
> recoverendpoint 172.27.128.32:8541 true 3
recover endpoint ok
```

**Notice:** Make sure the node is online before executing this command \(showtablet command to view\)

### changeleader

Perform a master-slave switchover for a specified shard. This command is asynchronous and after the successful return, you can view the running status through showopstatus

Command format: `changeleader table_name pid [candidate_leader]`

* table\_name: the Table name
* pid: the shard ID
* candidate\_leader: candidate leader. This parameter is optional. If this parameter is not added, it is required that there is no leader whose alive is yes in the shard. If it is set to auto, it can switch even if the alive status of other nodes is yes.

```
> changeleader flow 0
change leader ok
> changeleader flow 0 172.27.128.33:8541
change leader ok
> changeleader flow 0 auto
change leader ok
```

### recovertable

Restore a shard data. This command is asynchronous and after the successful return, you can view the running status through showopstatus

Command format: `recovertable table_name pid endpoint`

* table\_name: the table name
* pid: the shard ID
* endpoint: the endpoint to restore the node endpoint where the shard is located

```
> recovertable flow 1 172.27.128.31:8541
recover table ok
```

### cancelop

Cancel an ongoing or pending operation. After cancellation, the state of the task changes to kCanceled

Command format: `cancelop op\_id`

* op\_id: the operation ID to cancel

```
> cancelop 5
Cancel op ok!
```

### deleteop

Delete one or more op from nameserver

Command format: deleteop op\_id / op\_status

* op\_id: the operation ID to delete
* op\_status: specify the status of the op that needs to be deleted. The statuses that can be specified are done, failed and canceled

```
> deleteop 5
Delete op ok!
> deleteop done
Delete op ok!
```

### showopstatus

Display operation execution information

Command format: `showopstatus [table_name pid]`

* table\_name: the table name
* pid: the shard ID

```
> showopstatus
  op_id op_type name pid status start_time execute_time end_time cur_task
-------------------------------------------------- -------------------------------------------------- --------
  51 kMigrateOP flow 0 kDone 20180824163316 12s 20180824163328 -
  52 kRecoverTableOP flow 0 kDone 20180824195252 1s 20180824195253 -
  53 kRecoverTableOP flow 1 kDone 20180824195252 1s 20180824195253 -
  54 kUpdateTableAliveOP flow 0 kDone 20180824195838 2s 20180824195840 -
  55 kChangeLeaderOP flow 0 kDone 20180824200135 0s 20180824200135 -
  56 kOfflineReplicaOP flow 1 kDone 20180824200135 1s 20180824200136 -
  57 kReAddReplicaOP flow 0 kDone 20180827114331 12s 20180827114343 -
  58 kAddReplicaOP test1 0 kDone 20180827205907 8s 20180827205915 -
  59 kDelReplicaOP test1 0 kDone 20180827210248 4s 20180827210252 -
> showopstatus flow
  op_id op_type name pid status start_time execute_time end_time cur_task
-------------------------------------------------- -------------------------------------------------- -------
  51 kMigrateOP flow 0 kDone 20180824163316 12s 20180824163328 -
  52 kRecoverTableOP flow 0 kDone 20180824195252 1s 20180824195253 -
  53 kRecoverTableOP flow 1 kDone 20180824195252 1s 20180824195253 -
  54 kUpdateTableAliveOP flow 0 kDone 20180824195838 2s 20180824195840 -
  55 kChangeLeaderOP flow 0 kDone 20180824200135 0s 20180824200135 -
  56 kOfflineReplicaOP flow 1 kDone 20180824200135 1s 20180824200136 -
  57 kUpdateTableAliveOP flow 0 kDone 20180824200212 0s 20180824200212 -
>showopstatus flow 1
  op_id op_type name pid status start_time execute_time end_time cur_task
-------------------------------------------------- -------------------------------------------------- -------
  53 kRecoverTableOP flow 1 kDone 20180824195252 1s 20180824195253 -
  56 kOfflineReplicaOP flow 1 kDone 20180824200135 1s 20180824200136 -
```

### updatetablealive

Modify shard alive state

Command format: `updatetablealive table_name pid endpoint is_alive`

* table\_name: the table name
* pid: the fragment ID. If you want to modify all fragments of a table, specify pid as *
* endpoint: the endpoint of the node
* is\_alive: the  node status, can only fill in yes or no

```
> updatetablealive test * 172.27.128.31:8541 no
update ok
> updatetablealive test 1 172.27.128.31:8542 no
update ok
```

**Notice:** This command cannot be used for failure recovery. It is generally used to cut traffic. The operation method is to change the alive state of a node table to no and read requests will not fall on the node

### showns

Display the nameserver node and its role (if the serverName and automatic local IP function are used, the endpoint is the serverName, and the real_endpoint is "-")

Command format: `showns`

```
>showns
  endpoint real_endpoint role
-------------------------------------------------------
  172.24.4.55:6531 - leader
```

### exit

Exit the client

```
> exit
bye
```

### quit

Quit the client

```
> quit
bye
```

## Tablet Client

To connect to the tablet client, you need to specify the endpoint and role. The endpoint is the endpoint that needs to be connected to the tablet, and the role is the role to start, which needs to be specified as client

```bash
$ ./openmldb --endpoint=172.27.2.52:9520 --role=client
```

### loadtable

Load an existing table, only support memory table

Command format: `loadtable table_name tid pid ttl segment_cnt`

* table\_name: the table name
* tid: the ID of the table
* pid: the shard ID of the table
* ttl: set the ttl value
* segment\_cnt: set the segment count, generally set to 8

```
> loadtable table1 1 0 144000 8
```
loadtable will fail if existing table is in memory

### changerole

Change the leader role of the table

Command format: `changerole tid pid role \[term\]`

* tid: the id of the table
* pid: the shard ID of the table
* role: the role to be modified, the value is \[leader, follower\]
* term: set the term's value, this item is optional, the default is 0

```
> changerole 1 0 followers
ChangeRole ok
> changerole 1 0 leader
ChangeRole ok
> changerole 1 0 leader 1002
ChangeRole ok
```

### gettablestatus

Get table information

Command format: `gettablestatus \[tid pid\]`

* tid: the id of the table
* pid: the shard id of the table

```
> gettablestatus
  tid pid offset mode state enable_expire ttl ttl_offset memused compress_type
-------------------------------------------------- -------------------------------------------------- ------------------------
  1 0 4 kTableLeader kSnapshotPaused true 144000min 0s 1.313 K kNoCompress
  2 0 4 kTableLeader kTableNormal false 0min 0s 689.000 kNoCompress
> gettablestatus 2 0
  tid pid offset mode state enable_expire ttl ttl_offset memused compress_type
-------------------------------------------------- -------------------------------------------------- -----
  2 0 4 kTableLeader kTableNormal false 0min 0s 689.000 kNoCompress
```

### getfollower

View slave node information

Command format: `getfollower tid pid`

* tid: the id of the table
* pid: the shard id of the table

```
> getfollower 4 1
  # tid pid leader_offset follower offset
-------------------------------------------------- ---------
   0 4 1 5923724 172.27.128.31:8541 5920714
   1 4 1 5923724 172.27.128.32:8541 5921707
```

### exit

Exit the client

```
> exit
bye
```

### quit

Quit the client

```
> quit
bye
```
