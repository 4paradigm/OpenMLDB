# Install and Deploy

## Software and Hardware Requirements

* Operating system: CentOS 7, Ubuntu 20.04, macOS >= 10.15. Where Linux glibc version >= 2.17. Other operating system versions have not been fully tested and cannot be guaranteed to be fully compatible.
* Memory: Depends on the amount of data, 8 GB and above is recommended.
* CPU:
  * Currently only the x86 architecture is supported, and architectures such as ARM are currently not supported.
  * The number of cores is recommended to be no less than 4 cores. If the CPU does not support the AVX2 instruction set in the Linux environment, the deployment package needs to be recompiled from the source code.

## Deployment Package
The precompiled OpenMLDB deployment package is used by default in this documentation ([Linux](https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.4/openmldb-0.6.4-linux.tar.gz) , [macOS](https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.4/openmldb-0.6.4-darwin.tar.gz)), the supported operating system requirements are: CentOS 7, Ubuntu 20.04, macOS >= 10.15. If the user wishes to compile by himself (for example, for OpenMLDB source code development, the operating system or CPU architecture is not in the support list of the precompiled deployment package, etc.), the user can choose to compile and use in the docker container or compile from the source code. For details, please refer to our [compile documentation](compile.md).

## Configure Environment (Linux)

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
$ swapoff -a
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
$ echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled
$ echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag
```

Check whether the setting is successful. If "never" is surrounded by square brackets, it means that the setting has been successful, as shown below:

```bash
$ cat /sys/kernel/mm/transparent_hugepage/enabled
always madvise [never]
$ cat /sys/kernel/mm/transparent_hugepage/defrag
always madvise [never]
```

### Time and zone settings

The OpenMLDB data expiration deletion mechanism relies on the system clock. If the system clock is incorrect, the expired data will not be deleted or the data that has not expired will be deleted.

```bash
$date
Wed Aug 22 16:33:50 CST 2018
```
Please make sure the time is correct.

## Deploy Standalone Version

OpenMLDB standalone version needs to deploy a nameserver and a tablet. The nameserver is used for table management and metadata storage, and the tablet is used for data storage. APIServer is optional. If you want to interact with OpenMLDB using REST APIs, you need to deploy this module.

**Notice:** It is best to deploy different components in different directories for easy upgrades individually.

### Deploy tablet

#### 1. Download the OpenMLDB Deployment Package

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.4/openmldb-0.6.4-linux.tar.gz
tar -zxvf openmldb-0.6.4-linux.tar.gz
mv openmldb-0.6.4-linux openmldb-tablet-0.6.4
cd openmldb-tablet-0.6.4
```

#### 2. Modify the Configuration File: conf/standalone_tablet.flags

* Modify `endpoint`. The endpoint is the deployment machine ip/domain name and port number separated by colons.

```
--endpoint=172.27.128.33:9527
```

**Notice:**

* The endpoint cannot use 0.0.0.0 and 127.0.0.1.
* If the domain name is used here, all the machines where the client using OpenMLDB is located must be equipped with the corresponding host. Otherwise, it will not be accessible.

#### 3. Start the Service

```
bash bin/start.sh start standalone_tablet
```

**Notice**: After the service is started, the standalone_tablet.pid file will be generated in the bin directory, and the process number at startup will be saved in it. If the pid inside the file is running, the startup will fail.

### Deploy Nameserver

#### 1. Download the OpenMLDB Deployment Package

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.4/openmldb-0.6.4-linux.tar.gz
tar -zxvf openmldb-0.6.4-linux.tar.gz
mv openmldb-0.6.4-linux openmldb-ns-0.6.4
cd openmldb-ns-0.6.4
```

#### 2. Modify the Configuration File: conf/standalone_nameserver.flags

* Modify `endpoint`. The endpoint is the deployment machine ip/domain name and port number separated by colons.
* The `tablet` configuration item needs to be configured with the address of the tablet that was started earlier.

```
--endpoint=172.27.128.33:6527
--tablet=172.27.128.33:9527
```

**Notice**: The endpoint cannot use 0.0.0.0 and 127.0.0.1.

#### 3. Start the Service

```
bash bin/start.sh start standalone_nameserver
```

#### 4. Verify the Running Status of the Service

```bash
$ ./bin/openmldb --host=172.27.128.33 --port=6527
> show databases;
 -------------
  Databases
 -------------
0 row in set
```

### Deploy APIServer

APIServer is responsible for receiving http requests, forwarding them to OpenMLDB and returning results. It is stateless and is not a must-deploy component of OpenMLDB.
Before starting the APIServer, make sure that the OpenMLDB cluster has been started, otherwise APIServer will fail to initialize and exit the process.

#### 1. Download the OpenMLDB Deployment Package

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.4/openmldb-0.6.4-linux.tar.gz
tar -zxvf openmldb-0.6.4-linux.tar.gz
mv openmldb-0.6.4-linux openmldb-apiserver-0.6.4
cd openmldb-apiserver-0.6.4
```

#### 2. Modify the Configuration File: conf/standalone_apiserver.flags

* Modify `endpoint`. The endpoint is the deployment machine ip/domain name and port number separated by colons.
* Modify `nameserver` to be the address of Nameserver.

```
--endpoint=172.27.128.33:8080
--nameserver=172.27.128.33:6527
```

**Notice:**

* The endpoint cannot use 0.0.0.0 and 127.0.0.1. You can also choose not to set `--endpoint`, and only configure the port number `--port`.

#### 3. Start the Service

```
bash bin/start.sh start standalone_apiserver
```

## Deploy Cluster Version

OpenMLDB cluster version needs to deploy Zookeeper, Nameserver, Tablet and other modules. Among them, Zookeeper is used for service discovery and saving metadata information. The Nameserver is used to manage the tablet, achieve high availability and failover. Tablets are used to store data and synchronize data between master and slave. APIServer is optional. If you want to interact with OpenMLDB in http, you need to deploy this module.

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
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.4/openmldb-0.6.4-linux.tar.gz
tar -zxvf openmldb-0.6.4-linux.tar.gz
mv openmldb-0.6.4-linux openmldb-tablet-0.6.4
cd openmldb-tablet-0.6.4
```

#### 2. Modify the Configuration File: conf/tablet.flags

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
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.4/openmldb-0.6.4-linux.tar.gz
tar -zxvf openmldb-0.6.4-linux.tar.gz
mv openmldb-0.6.4-linux openmldb-ns-0.6.4
cd openmldb-ns-0.6.4
```

#### 2. Modify the Configuration File: conf/nameserver.flags

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
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.4/openmldb-0.6.4-linux.tar.gz
tar -zxvf openmldb-0.6.4-linux.tar.gz
mv openmldb-0.6.4-linux openmldb-apiserver-0.6.4
cd openmldb-apiserver-0.6.4
```

#### 2. Modify the Configuration File: conf/apiserver.flags

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

#### 1. Download the OpenMLDB Spark Distribution that is Optimized for Feature Engineering

```
wget https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.6.4/spark-3.2.1-bin-openmldbspark.tgz
tar -zxvf spark-3.2.1-bin-openmldbspark.tgz
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.4/openmldb-0.6.4-linux.tar.gz
tar -zxvf openmldb-0.6.4-linux.tar.gz
mv openmldb-0.6.4-linux openmldb-taskmanager-0.6.4
cd openmldb-taskmanager-0.6.4
```

#### 2. Modify the Configuration File conf/taskmanager.properties

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
