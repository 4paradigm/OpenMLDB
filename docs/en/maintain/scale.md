# EEnlarge shrinks capacity

## Expansion

With the development of business, dynamic capacity expansion is required if the current cluster topology cannot meet the requirements. Capacity expansion is to migrate some shards from existing tablet nodes to new tablet nodes to reduce memory usage

### 1 Start a new tablet node

After startup, check whether the newly added node joins the cluster. If the showtablet command is executed and the new node endpoint is listed, it means that it has joined the cluster

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> showtablet
  endpoint            state           age
-------------------------------------------
  172.27.128.31:8541  kTabletHealthy  15d
  172.27.128.32:8541  kTabletHealthy  15d
  172.27.128.33:8541  kTabletHealthy  15d
  172.27.128.37:8541  kTabletHealthy  1min
```

### 2 Migrate the copy

The command used for replica migration is migrate。command format: migrate src\_endpoint table\_name partition des\_endpoint  

**Once the table is created, shards cannot be added or removed, only shards can be migrated. When migrating, only slave shards can be migrated, not primary shards**

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> use demo_db
> showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.33:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       yes       kNoCompress    0        0           0.000
> migrate 172.27.128.33:8541 flow 0 172.27.128.37:8541
> showopstatus flow
  op_id  op_type     name  pid  status  start_time      execute_time  end_time        cur_task
------------------------------------------------------------------------------------------------
  51     kMigrateOP  flow  0    kDone   20180824163316  12s           20180824163328  -
> showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.37:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       yes       kNoCompress    0        0           0.000
```
**illustrate**: Migrating replicas can also be done by deleting replicas and then adding new ones.

## Shrinkage capacity

Capacity reduction refers to reducing the number of nodes in an existing cluster.

### 1 Select the node that you want to go offline
### 2 Migrate shards on nodes that need to be taken offline to other nodes
* Run the showtable command to view the fragment distribution of the table
* Run the Migrage command to migrate to another node. If the leader exists on the offline node, you can run the Changeleader command to switch the leader to another node
### 3 Offline node
Execute stop command
```bash
sh bin/start.sh stop tablet
```
If nameserver is deployed on the node, disable nameserver
```bash
sh bin/start.sh stop nameserver
```
**note**：At least two Nameserver nodes are required to maintain high availability