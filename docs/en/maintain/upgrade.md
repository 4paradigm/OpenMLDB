# Upgrade

Here is the impact when upgrading OpenMLDB:
* If the created table is a single copy, it is not readable and writable during the upgrade process
* If the created table is multi-replica, the read request on the upgraded node will fail briefly, and the write request will have a small amount of data loss.If short read failures are not tolerated, execute offlineendpoint before stopping each tablet node. If a small amount of write loss cannot be tolerated, write operations need to be stopped during the upgrade process

## 1. Upgrade Nameserver

* Stop nameserver
    ```bash
    bash bin/start.sh stop nameserver
    ```
* Backup the old versions directories `bin` and `conf`
* Download new version bin and conf
* Compare the configuration file diff and modify the necessary configuration, such as endpoint, zk_cluster, etc
* Start nameserver
    ```bash
    bash bin/start.sh start nameserver
    ```
* Repeat the above steps for the remaining nameservers

## 2. Upgrade Tablets

### 2.1. Steps of Upgrading Tablets

* Stop tablet
    ```bash
        bash bin/start.sh stop tablet
    ```
* Backup the old versions directories `bin` and `conf`
* Download new version bin and conf
* Compare the configuration file diff and modify the necessary configuration, such as endpoint, zk_cluster, etc
* Start nameserver
    ```bash
    bash bin/start.sh start tablet
    ```
* If auto_failover is closed, you must connect to the ns client and perform the following operations to restore data. **The endpoint after the command is the endpoint of the restarted node**
  * offlineendpoint endpoint 
  * recoverendpoint endpoint

```
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> offlineendpoint 172.27.128.32:8541
offline endpoint ok
> recoverendpoint 172.27.128.32:8541
recover endpoint ok
```

### 2.2. Confirmation of Upgrade Result
* `showopstatus` command checks whether all operations are `kDone`, and if there is a `kFailed` task, check the log to troubleshoot the cause
* `showtable` to see if the status of all partitions is yes

After a tablet node is upgraded, repeat the above steps for other tablets. \(**You must wait until the data is synchronized before upgrading the next node**\)

After all nodes are upgraded, resume write operations, and run the showtable command to check whether the master-slave offset has increased

## 3. Upgrade the Java Client

* Update the java client version number in the pom file
* Update dependencies
