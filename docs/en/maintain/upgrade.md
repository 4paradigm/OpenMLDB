# Upgrade

Here is the impact when upgrading OpenMLDB:
* If the table is single-replica, we will add an extra replica before upgrading and delete it afterwards
* If the table is multi-replica, we will migrate the leader partitions in the tablet to be upgraded
to other tablets, and migrate back after the upgrade.
If there is write traffice during the upgrade, there may be data loss.

## 1. Upgrade Nameserver

* Stop nameserver
    ```bash
    bash bin/start.sh stop nameserver
    ```
* Backup the old `bin` directory
* Replace with the new bin
* Start the new nameserver
    ```bash
    bash bin/start.sh start nameserver
    ```
* Repeat the above steps for the remaining nameservers

## 2. Upgrade Tablets

### 2.1. Steps of Upgrading Tablets

* `pre-upgrade`: to reduce the interruption to the online service before the upgrade (refer to [Operation Tool](./openmldb_ops.md))
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=pre-upgrade --endpoints=127.0.0.1:10921
    ```
* Stop tablet
    ```bash
        bash bin/start.sh stop tablet
    ```
* Backup the old `bin` directory
* Replace with the new bin
* Start tablet
    ```bash
    bash bin/start.sh start tablet
    ```
* If `auto_failover` is off, we have to manually `recoverdata` to restore data.
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=recoverdata
    ```
* `post-upgrade`: revert all the actions done in `pre-upgrade`
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=post-upgrade --endpoints=127.0.0.1:10921
    ```
### 2.2. Confirmation of Upgrade Result
* `showopstatus` command checks whether there are operations that are `kFailed`, and check the log to troubleshoot the cause
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=showopstatus --filter=kFailed
    ```
* `showtablestatus` to see if the statuses of all tables are ok
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=showtablestatus
    ```
After a tablet node is upgraded, repeat the above steps for other tablets.

After all tablets are upgraded, resume write operations, and run the `showtablestatus` command to check whether the `Rows` number has increased.

## 3. Upgrade taskmanager

* Stop taskmanager
    ```bash
    bash bin/start.sh stop taskmanager
    ```
* Backup the old `bin` and `taskmanager` directories
* Replace with the new `bin` and `taskmanager` directories
* Start the new taskmanager
    ```bash
    bash bin/start.sh start taskmanager
    ```
## 4. Upgrade apiserver

* Stop apiserver
    ```bash
    bash bin/start.sh stop apiserver
    ```
* Backup the old `bin` directory
* Replace with the new `bin` directory
* Start the new apiserver
    ```bash
    bash bin/start.sh start apiserver
    ```

## 5. Upgrade the SDKs

### Upgrade Java SDK
* Update the java sdk version number in the pom file, including `openmldb-jdbc` and `openmldb-native`

### Upgrade Python SDK
* install the new python sdk
  ```bash
  pip install openmldb=={NEW_VERSION}
  ```