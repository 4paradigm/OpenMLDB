# Install and Deploy

## Software and Hardware Requirements

* Operating system: CentOS 7, Ubuntu 20.04, macOS >= 10.15. Where Linux glibc version >= 2.17. Other operating system versions have not been fully tested and cannot be guaranteed to be fully compatible.
* Memory: Depends on the amount of data, 8 GB and above is recommended.
* CPU:
  * Currently only the x86 architecture is supported, and architectures such as ARM are currently not supported.
  * The number of cores is recommended to be no less than 4 cores. If the CPU does not support the AVX2 instruction set in the Linux environment, the deployment package needs to be recompiled from the source code.

## Deployment Package
The precompiled OpenMLDB deployment package is used by default in this documentation ([Linux](https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.2/openmldb-0.7.2-linux.tar.gz) , [macOS](https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.2/openmldb-0.7.2-darwin.tar.gz)), the supported operating system requirements are: CentOS 7, Ubuntu 20.04, macOS >= 10.15. If the user wishes to compile by himself (for example, for OpenMLDB source code development, the operating system or CPU architecture is not in the support list of the precompiled deployment package, etc.), the user can choose to compile and use in the docker container or compile from the source code. For details, please refer to our [compile documentation](compile.md).

## Configure Environment (Linux)

### configure the size limit of coredump file and open file limit
```bash
ulimit -c unlimited
ulimit -n 655360
```

The configurations set by `ulimit` command only take effect in the current session.
If you want to persist the configurations, add the following configurations in `/etc/security/limits.conf`:
```bash
*       soft    core    unlimited
*       hard    core    unlimited
*       soft    nofile  655360
*       hard    nofile  655360
```

### Disable system swap

Check the status of the swap area.

```bash
$ free
              total used free shared buff/cache available
Mem: 264011292 67445840 2230676 3269180 194334776 191204160
Swap: 0 0 0
```

If the swap item is all 0, it means it has been closed, otherwise run the following command to disable all swap.

```
swapoff -a
```

### Disable THP (Transparent Huge Pages)

Check the status of THP.

```
$ cat /sys/kernel/mm/transparent_hugepage/enabled
always [madvise] never
$ cat /sys/kernel/mm/transparent_hugepage/defrag
[always] madvise never
```

If "never" is not surrounded by square brackets in the above two configurations, it needs to be set.

```bash
echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled
echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag
```

Check whether the setting is successful. If "never" is surrounded by square brackets, it means that the setting has been successful, as shown below:

```bash
$ cat /sys/kernel/mm/transparent_hugepage/enabled
always madvise [never]
$ cat /sys/kernel/mm/transparent_hugepage/defrag
always madvise [never]
```

Note: You can also use script to modify the above configurations, refer [here](#configure-node-environment-optional)

### Time and zone settings

The OpenMLDB data expiration deletion mechanism relies on the system clock. If the system clock is incorrect, the expired data will not be deleted or the data that has not expired will be deleted.

```bash
$date
Wed Aug 22 16:33:50 CST 2018
```
Please make sure the time is correct.

## Deploy Standalone Version

OpenMLDB standalone version needs to deploy a nameserver and a tablet. The nameserver is used for table management and metadata storage, and the tablet is used for data storage. APIServer is optional. If you want to interact with OpenMLDB using REST APIs, you need to deploy this module.

### Download the OpenMLDB Release Package

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.2/openmldb-0.7.2-linux.tar.gz
tar -zxvf openmldb-0.7.2-linux.tar.gz
cd openmldb-0.7.2-linux
```

### Configuration
If OpenMLDB is only accessed locally, can skip this step.
#### 1. Configure tablet: conf/standalone_tablet.flags

* Modify `endpoint`. The endpoint is the deployment machine ip/domain name and port number separated by colons.

```
--endpoint=172.27.128.33:9527
```

**Notice:**

* The endpoint cannot use 0.0.0.0 and 127.0.0.1.
* If the domain name is used here, all the machines where the client using OpenMLDB is located must be equipped with the corresponding host. Otherwise, it will not be accessible.

#### 2. Configure nameserver: conf/standalone_nameserver.flags

* Modify `endpoint`. The endpoint is the deployment machine ip/domain name and port number separated by colons.
* The `tablet` configuration item needs to be configured with the address of the tablet that was started earlier.

```
--endpoint=172.27.128.33:6527
--tablet=172.27.128.33:9527
```

**Notice**: The endpoint cannot use 0.0.0.0 and 127.0.0.1.


#### 3. Configure apiserver: conf/standalone_apiserver.flags
APIServer is responsible for receiving http requests, forwarding them to OpenMLDB and returning results. It is stateless and is not a must-deploy component of OpenMLDB.
Before starting the APIServer, make sure that the OpenMLDB cluster has been started, otherwise APIServer will fail to initialize and exit the process.

* Modify `endpoint`. The endpoint is the deployment machine ip/domain name and port number separated by colons.
* Modify `nameserver` to be the address of Nameserver.

```
--endpoint=172.27.128.33:8080
--nameserver=172.27.128.33:6527
```

**Notice:**

* The endpoint cannot use 0.0.0.0 and 127.0.0.1. You can also choose not to set `--endpoint`, and only configure the port number `--port`.

### Start All the Service
Make sure `OPENMLDB_MODE=standalone` in the `conf/openmldb-env.sh` (default)
```
sbin/start-all.sh
```

After the service is started, the standalone_tablet.pid, standalone_nameserver.pid and standalone_apiserver.pid files will be generated in the `bin` directory, and the process numbers at startup will be saved in them. If the pid inside the file is running, the startup will fail.

## Deploy Cluster Version (Auto)
OpenMLDB cluster version needs to deploy Zookeeper, Nameserver, Tablet, TaskManager and other modules. Among them, Zookeeper is used for service discovery and saving metadata information. The Nameserver is used to manage the tablet, achieve high availability and failover. Tablets are used to store data and synchronize data between master and slave. APIServer is optional. If you want to interact with OpenMLDB in http, you need to deploy this module.

Notice: It is best to deploy different components in different directories for easy upgrades individually. If multiple tablets are deployed on the same machine, they also need to be deployed in different directories.

Environment Requirements:
- the deploy node has password-free login to other nodes
- `rsync` is required
- Python3 is required
- JRE (Java Runtime Environment) is required on the node where Zookeeper and TaskManager are deployed


### Download the OpenMLDB Deployment Package

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.2/openmldb-0.7.2-linux.tar.gz
tar -zxvf openmldb-0.7.2-linux.tar.gz
cd openmldb-0.7.2-linux
```

### Configuration
#### Configure the environment
The environment variables are defined in `conf/openmldb-env.sh`,
which are listed below.

| Environment Variables             | Default Values                                          | Definitons                                                                                            |
|-----------------------------------|---------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| OPENMLDB_VERSION                  | 0.7.2                                                   | OpenMLDB version                                                                                      |
| OPENMLDB_MODE                     | standalone                                              | standalone or cluster mode                                                                            |
| OPENMLDB_HOME                     | root directory of the release folder                    | openmldb root path                                                                                    |
| SPARK_HOME                        | $OPENMLDB_HOME/spark                                    | the root path of openmldb spark release. if not exists, download from online                          |
| OPENMLDB_TABLET_PORT              | 10921                                                   | the default port for tablet                                                                           |
| OPENMLDB_NAMESERVER_PORT          | 7527                                                    | the default port for nameserver                                                                       |
| OPENMLDB_TASKMANAGER_PORT         | 9902                                                    | the default port for taskmanager                                                                      |
| OPENMLDB_APISERVER_PORT           | 9080                                                    | the default port for apiserver                                                                        |
| OPENMLDB_USE_EXISTING_ZK_CLUSTER  | false                                                   | whether use an existing zookeeper cluster. If `false`, start its own zk cluster in the deploy scripts |
| OPENMLDB_ZK_HOME                  | $OPENMLDB_HOME/zookeeper                                | the root path of zookeeper release                                                                    |
| OPENMLDB_ZK_CLUSTER               | auto derived from `[zookeeper]` section in `conf/hosts` | the zookeeper cluster address                                                                         |
| OPENMLDB_ZK_ROOT_PATH             | /openmldb                                               | zookeeper root path                                                                                   |
| OPENMLDB_ZK_CLUSTER_CLIENT_PORT   | 2181                                                    | zookeeper client port, i.e., clientPort in zoo.cfg                                                    |
| OPENMLDB_ZK_CLUSTER_PEER_PORT     | 2888                                                    | zookeeper peer port, which is the first port in this config server.1=zoo1:2888:3888 in zoo.cfg        |
| OPENMLDB_ZK_CLUSTER_ELECTION_PORT | 3888                                                    | zookeeper election port, which is the second port in this config server.1=zoo1:2888:3888 in zoo.cfg   |

#### Configure the deployment node

The node list is defined in `conf/hosts`, for example:
```bash
[tablet]
node1:10921 /tmp/openmldb/tablet
node2:10922 /tmp/openmldb/tablet

[nameserver]
node3:7527

[apiserver]
node3:9080

[taskmanager]
localhost:9902

[zookeeper]
node3:2181:2888:3888 /tmp/openmldb/zk-1
```

The configuration file is divided into four sections, identified by `[]`:
- `[tablet]`：tablet node list
- `[nameserver]`：nameserver node list
- `[apiserver]`：apiserver node list
- `[taskmanager]`：taskmanager node list
- `[zookeeper]`：zookeeper node list

For each node list, one line represents one node, with the format of `host:port WORKDIR`.
Specially, for the `[zookeeper]`, its format is `host:port:zk_peer_port:zk_election_port WORKDIR`,
where there are extra port parameters,
including `zk_peer_port` for the connection between followers and leader,
and `zk_election_port` for the leader election.

Note that for every node configuration, only the `host` is required, while others are optional.
If the optional parameters are not provided,
default values are used, which are defined in `conf/openmldb-env.sh`.

```{warning}
If multiple TaskManager instances are deployed in different machines，the `offline.data.prefix` should be configured to be globally accessabile by these machines (e.g., hdfs path).
```

### Configure Node Environment (Optional)
```
bash sbin/init_env.sh
```
Note:
- The script needs to be executed by the `root` user.
- The script will modify the `limit`, disable the `swap` and `THP`. 


### Deployment
```bash
sbin/deploy-all.sh
```
It will distribute all the required files to the nodes defined in `conf/hosts`,
and conduct custom configuration based on `conf/hosts` and `conf/openmldb-env.sh`.

If users want to do other custom configurations, users can modify the `conf/xx.template`
before running the `deploy-all.sh` script,
so that the updated configurations will be reflected in the deployed nodes.

Repeated execution of `sbin/deploy-all.sh` will overwrite the last deployment.

### Start the Services
```bash
sbin/start-all.sh
```
This script will launch all the services defined in `conf/hosts`.

Users can use `sbin/openmldb-cli.sh` to connect the cluster and verify
the services are launched successfully.

#### Stop the Services
If users want to stop all the services, can execute the following script:
```bash
sbin/stop-all.sh
```

## Deploy Cluster Version (Manual)

OpenMLDB cluster version needs to deploy Zookeeper, Nameserver, Tablet, TaskManager and other modules. Among them, Zookeeper is used for service discovery and saving metadata information. The Nameserver is used to manage the tablet, achieve high availability and failover. Tablets are used to store data and synchronize data between master and slave. APIServer is optional. If you want to interact with OpenMLDB in http, you need to deploy this module.

**Notice:** It is best to deploy different components in different directories for easy upgrades individually. If multiple tablets are deployed on the same machine, they also need to be deployed in different directories.

### Deploy Zookeeper

The required zookeeper version is >= 3.4 and <= 3.6, it is recommended to deploy version 3.4.14. You can skip this step if there is an available zookeeper cluster.

#### 1. Download the Zookeeper Installation Package

```
wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
tar -zxvf zookeeper-3.4.14.tar.gz
cd zookeeper-3.4.14
cp conf/zoo_sample.cfg conf/zoo.cfg
```

#### 2. Modify the Configuration File

Open the file `conf/zoo.cfg` and modify `dataDir` and `clientPort`.

```
dataDir=./data
clientPort=7181
```

#### 3. Start Zookeeper

```
bash bin/zkServer.sh start
```

Deploy the Zookeeper cluster [refer to here](https://zookeeper.apache.org/doc/r3.4.14/zookeeperStarted.html#sc_RunningReplicatedZooKeeper).

### Deploy Tablet

#### 1. Download the OpenMLDB Deployment Package

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.2/openmldb-0.7.2-linux.tar.gz
tar -zxvf openmldb-0.7.2-linux.tar.gz
mv openmldb-0.7.2-linux openmldb-tablet-0.7.2
cd openmldb-tablet-0.7.2
```

#### 2. Modify the Configuration File: conf/tablet.flags
```bash
# can update the config based on the template provided
cp conf/tablet.flags.template conf/tablet.flags
```

* Modify `endpoint`. The endpoint is the deployment machine ip/domain name and port number separated by colons.
* Modify `zk_cluster` to the already started zk cluster address.
* If you share zk with other OpenMLDB, you need to modify `zk_root_path`.

```
--endpoint=172.27.128.33:9527
--role=tablet

# If tablet run as cluster mode zk_cluster and zk_root_path should be set:
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
```

**Notice:**

* The endpoint cannot use 0.0.0.0 and 127.0.0.1.
* If the domain name is used here, all the machines with OpenMLDB clients must be configured with the corresponding host. Otherwise, it will not be accessible.
* The configuration of `zk_cluster` and `zk_root_path` is consistent with that of Nameserver.

#### 3. Start the Service

```
bash bin/start.sh start tablet
```

Repeat the above steps to deploy multiple tablets.

**Notice:**

* After the service is started, the tablet.pid file will be generated in the bin directory, and the process number at startup will be saved in it. If the pid inside the file is running, the startup will fail.
* Cluster version needs to deploy at least 2 tablets.
* If you need to deploy multiple tablets, deploy all the tablets before deploying the Nameserver.

### Deploy Nameserver

#### 1. Download the OpenMLDB Deployment Package

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.2/openmldb-0.7.2-linux.tar.gz
tar -zxvf openmldb-0.7.2-linux.tar.gz
mv openmldb-0.7.2-linux openmldb-ns-0.7.2
cd openmldb-ns-0.7.2
```

#### 2. Modify the Configuration File: conf/nameserver.flags
```bash
# can update the config based on the template provided
cp conf/nameserver.flags.template conf/nameserver.flags
```

* Modify `endpoint`. The endpoint is the deployment machine ip/domain name and port number separated by colons.
* Modify `zk_cluster` to the address of the zk cluster that has been started. IP is the machine address where zk is located, and port is the port number configured by clientPort in the zk configuration file. If zk is in cluster mode, separate it with commas, and the format is ip1:port1,ip2:port2, ip3:port3.
* If you share zk with other OpenMLDB, you need to modify `zk_root_path`.

```
--endpoint=172.27.128.31:6527
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
```

**Notice:** The endpoint cannot use 0.0.0.0 and 127.0.0.1.

#### 3. Start the Service

```
bash bin/start.sh start nameserver
```

Repeat the above steps to deploy multiple nameservers.

#### 4. Verify the Running Status of the Service

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:7181,172.27.128.32:7181,172.27.128.33:7181 --zk_root_path=/openmldb_cluster --role=ns_client
> shown
  endpoint role
-----------------------------
  172.27.128.31:6527 leader
```

### Deploy APIServer

APIServer is responsible for receiving http requests, forwarding them to OpenMLDB and returning results. It is stateless and is not a must-deploy component of OpenMLDB.
Before running, make sure that the OpenMLDB cluster has been started, otherwise APIServer will fail to initialize and exit the process.

#### 1. Download the OpenMLDB Deployment Package

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.2/openmldb-0.7.2-linux.tar.gz
tar -zxvf openmldb-0.7.2-linux.tar.gz
mv openmldb-0.7.2-linux openmldb-apiserver-0.7.2
cd openmldb-apiserver-0.7.2
```

#### 2. Modify the Configuration File: conf/apiserver.flags
```bash
# can update the config based on the template provided
cp conf/apiserver.flags.template conf/apiserver.flags
```

* Modify `endpoint`. The endpoint is the deployment machine ip/domain name and port number separated by colons.
* Modify ``zk_cluster`` to the zk cluster address of OpenMLDB to be forwarded to.

```
--endpoint=172.27.128.33:8080
--role=apiserver
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
--openmldb_log_dir=./logs
```

**Notice:**

* The endpoint cannot use 0.0.0.0 and 127.0.0.1. You can also choose not to set `--endpoint`, and only configure the port number `--port`.
* You can also configure the number of threads of APIServer, `--thread_pool_size`, the default is 16.

#### 3. Start the Service

```
bash bin/start.sh start apiserver
```

**Notice:** If the program crashes when starting the nameserver/tablet/apiserver using the OpenMLDB release package, it is very likely that the instruction set is incompatible, and you need to compile OpenMLDB through the source code. For source code compilation, please refer to [here](./compile.md), you need to use method 3 to compile the complete source code.

### Deploy TaskManager

TaskManager can be deployed in single server. You can deploy multiple instances for high availability. If the master server of TaskManagers fails, the slaves will replace the master for failover  and the client will reconnect automatically.

#### 1. Download the OpenMLDB Spark Distribution that is Optimized for Feature Engineering

```
wget https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.7.2/spark-3.2.1-bin-openmldbspark.tgz
tar -zxvf spark-3.2.1-bin-openmldbspark.tgz
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.2/openmldb-0.7.2-linux.tar.gz
tar -zxvf openmldb-0.7.2-linux.tar.gz
mv openmldb-0.7.2-linux openmldb-taskmanager-0.7.2
cd openmldb-taskmanager-0.7.2
```

#### 2. Modify the Configuration File conf/taskmanager.properties
```bash
# can update the config based on the template provided
cp conf/taskmanager.properties.template conf/taskmanager.properties
```

* Modify `server.host`. The host is the ip/domain name of the deployment machine.
* Modify `server.port`. The port is the port number of the deployment machine.
* Modify `zk_cluster` to the address of the zk cluster that has been started. IP is the address of the machine where zk is located, and port is the port number configured by clientPort in the zk configuration file. If zk is in cluster mode, it is separated by commas, and the format is ip1:port1,ip2:port2,ip3:port3.
* If you share zk with other OpenMLDB, you need to modify zookeeper.root_path.
* Modify `batchjob.jar.path` to the BatchJob Jar file path. If it is set to empty, it will search in the upper-level lib directory. If you use Yarn mode, you need to modify it to the corresponding HDFS path.
* Modify `offline.data.prefix` to the offline table storage path. If Yarn mode is used, it needs to be modified to the corresponding HDFS path.
* Modify `spark.master` to run in offline task mode, currently supports local and yarn modes.
* Modify `spark.home` to the Spark environment path. If it is not configured or the configuration is empty, the configuration of the SPARK_HOME environment variable will be used. It needs to be set as the directory where the spark-optimized package is extracted in the first step, and the path is an absolute path.

```
server.host=0.0.0.0
server.port=9902
zookeeper.cluster=172.27.128.31:7181,172.27.128.32:7181,172.27.128.33:7181
zookeeper.root_path=/openmldb_cluster
batchjob.jar.path=
offline.data.prefix=file:///tmp/openmldb_offline_storage/
spark.master=local
spark.home=
```

#### 3. Start the Service

```bash
bash bin/start.sh start taskmanager
```

#### 4. Verify the Running Status of the Service

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:7181,172.27.128.32:7181,172.27.128.33:7181 --zk_root_path=/openmldb_cluster --role=sql_client
> show jobs;
---- ---------- ------- ------------ ---------- ------- ---- --------- ---------------- -------
 id job_type state start_time end_time parameter cluster application_id error
---- ---------- ------- ------------ ---------- ------- ---- --------- ---------------- -------
```
