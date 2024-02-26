# Update Configuration

```{note}
We'll use the normal mode(background) to restart the components. If you want to restart them in daemon mode, please use `bash bin/start.sh restart <component> mon`. In daemon mode, `bin/<component>.pid` is the mon pid，`bin/<component>.pid.child` is the component pid. The mon process is not the system service, if the mon process crashed, the component process becomes the normal background process.
```

## Update Nameserver Configuration
* Backup configuation
    ```bash
    cp conf/nameserver.flags conf/nameserver.flags.bak
    ```
* Update configuation
* Restart the nameserver
    ```bash
    bash bin/start.sh restart nameserver
    ```
* Repeat the above steps for the remaining nameservers

## Update Tablet Configuration

```{important}
If there are multiple tablets, be sure to perform sequential operations on each tablet. Do not simultaneously update the configuration of multiple tablets. That is, complete the configuration update for one tablet, confirm the result, and then proceed with the next tablet's configuration update. Otherwise, it may lead to abnormal cluster status. If misoperations result in a cluster abnormality, you can try using the [operations tool](openmldb_ops.md) `recoverdata` for recovery.
```

Here is the impact when update the configuration of tablet:

* If the table is single-replica, users can choose:
    - add an extra replica before restarting and delete it afterwards (achieved by `pre-upgrade` and `post-upgrade`). Then it has the same behavior as the multi-replica case
    - if it is acceptable that the table may be unavailable during the update, users can specify `--allow_single_replica` during `pre-upgrade`, which can avoid OOM caused by adding a replica if memory is limited
* If the table is multi-replica, we will migrate the leader partitions in the tablet to be updating to other tablets, and migrate back after the upgrade. If there is write traffic during the update, there may be data loss.

Steps for updating are as follows:
* Backup configuation
    ```bash
    cp conf/tablet.flags conf/tablet.flags.bak
    ```
* Update configuation
* `pre-upgrade`: to reduce the interruption to the online service before the restart (refer to [Operation Tool](./openmldb_ops.md))
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=pre-upgrade --endpoints=127.0.0.1:10921
    ```
  If the unavailability of single-replica tables is ok, users can add `--allow_single_replica` to avoid adding a new replica.
* Restart tablet
    ```bash
    bash bin/start.sh restart tablet
    ```
* If `auto_failover` is off, we have to manually `recoverdata` to restore data.
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=recoverdata
    ```
* `post-upgrade`: revert all the actions done in `pre-upgrade`
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=post-upgrade --endpoints=127.0.0.1:10921
    ```

### Confirmation of Restart Result
* `showopstatus` command checks whether there are operations that are `kFailed`, and check the log to troubleshoot the cause
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=showopstatus --filter=kFailed
    ```
* `showtablestatus` to see if the statuses of all tables are ok
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=showtablestatus
    ```
After a tablet node is restarted, repeat the above steps for other tablets.

After all tablets are restarted, resume write operations, and run the `showtablestatus` command to check whether the `Rows` number has increased.

## Update APIServer Configuration
* Backup configuation
    ```bash
    cp conf/apiserver.flags conf/apiserver.flags.bak
    ```
* Update configuation
* Restart the apiserver
    ```bash
    bash bin/start.sh restart apiserver
    ```
## Update TaskManager Configuration
* Backup configuation
    ```bash
    cp conf/taskmanager.properties conf/taskmanager.properties.bak
    ```
* Update configuation
* Restart the taskmanager
    ```bash
    bash bin/start.sh restart taskmanager
    ```
    