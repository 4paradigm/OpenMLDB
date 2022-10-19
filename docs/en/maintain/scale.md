# Scale-Out and Scale-In

## Scale-Out

With the development of business, dynamic scale-out is required if the current cluster topology cannot meet the requirements. Scale-out is to migrate some partitions from existing tablet nodes to new tablet nodes.

### Step 1. Starting a new tablet node

You need to first start a new tablet node as following steps, please refer to the [deploy doc](../deploy/install_deploy.md) for details:
- Check time and zone settings, disable `THP` and `swap`
- Download the package
- Modify the configuration file: conf/tablet.flags
- Start a new tablet
  ```bash
    bash bin/start.sh start tablet
  ```

After startup, you need to check whether the new node has joined the cluster. If the `showtablet` command is executed and the new node endpoint is listed, it means that it has joined the cluster

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

### Step 2. Migrating replications

The command used for replication migration is `migrate`. The command format is: `migrate src_endpoint table_name partition des_endpoint` 

**Once the table is created, partitions cannot be added or removed, only can be migrated. When migrating, only leader partitions can be migrated, not follower partitions**

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
Note that, migrating replicatins can also be done by deleting replicatins and then adding new ones.

## Scale-In

Scaling in your cluster is to reduce the number of nodes in the cluster.

### Step 1. Selecting the node that you want to go offline
### Step 2. Migrating partitions on nodes that need to be taken offline to other nodes
* Run the `showtable` command to view the partitions of a table
* Run the `migrage` command to migrate the targeted partitions to another node. If the leader exists on the offline node, you can run the `changeleader` command to switch the leader to another node
### Step 3. Making the targeted node offline
- Execute `stop` command
```bash
bash bin/start.sh stop tablet
```
- If nameserver is deployed on the node, you need to disable the nameserver.
```bash
bash bin/start.sh stop nameserver
```
Note that, at least two Nameserver nodes are required to maintain high availability
