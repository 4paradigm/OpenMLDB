# Install and Deploy

## Software and Hardware Requirements

### Operating System

The pre-compiled packages that have been released offer support for the following operating systems: CentOS 7.x, Ubuntu 20.04, SUSE 12 SP3, and macOS 12. For Linux, a glibc version of >= 2.17 is required. While pre-compiled packages for other operating system distributions have not undergone comprehensive testing and therefore cannot guarantee complete compatibility, you can explore [Compiling from Source](https://chat.openai.com/c/compile.md) to extend support to other operating systems.

````{note}
Linux users can assess their system's compatibility through the following commands:

```shell
cat /etc/os-release # most linux
cat /etc/redhat-release # redhat only
ldd --version
strings /lib64/libc.so.6 | grep ^GLIBC_
```
Generally, ldd version should be >= 2.17, and GLIBC_2.17 should be present in libc.so.6. These factors indicate compatibility with glibc 2.17 for program and dynamic library operations. If the system's glibc version falls below 2.17, compiling from source code is necessary.
````

### Third-party component dependencies

If you need to deploy ZooKeeper and TaskManager, you need a Java runtime environment.

### Hardware

Regarding hardware requirements:

- CPU:
  - X86 CPU is recommended, preferably with a minimum of 4 cores.
  - For users of pre-compiled packages, AVX2 instruction set support is required. Otherwise, consider compiling from the [Source Code](https://chat.openai.com/c/compile.md).
- Memory:
  - It is recommended to have at least 8 GB of RAM. For business scenarios involving substantial data loads, 128 GB or more is advisable.
- Other hardware components:
  - No specific requirements exist. However, disk and network performance may influence OpenMLDB's latency and throughput performance.

## Deployment Package

### Download/Source Compilation

If your operating system is capable of running pre-compiled packages directly, you can download them from the following sources:

- GitHub Release Page: https://github.com/4paradigm/OpenMLDB/releases
- Mirror Site (China): http://43.138.115.238/download/

The relationship between pre-compiled packages and compatible operating systems is as follows:

- `openmldb-x.x.x-linux.tar.gz`: Compatible with CentOS 7.x, Ubuntu 20.04, SUSE 12 SP3
- `openmldb-x.x.x-darwin.tar.gz`: Suitable for macOS 12

In cases where the user's operating system is not mentioned above or if they intend to compile from source code, please refer to our [Source Code Compilation Document](https://chat.openai.com/c/compile.md).

### Linux platform pre-testing

Due to the variations among Linux platforms, the distribution package may not be entirely compatible with your machine. Therefore, it's recommended to conduct a preliminary compatibility test. For instance, after downloading the pre-compiled package `openmldb-0.8.2-linux.tar.gz`, execute:

```
tar -zxvf openmldb-0.8.2-linux.tar.gz
./openmldb-0.8.2-linux/bin/openmldb --version
```

The result should display the version number of the program, similar to:

```
openmldb version 0.8.2-xxxx
Debug build (NDEBUG not #defined)
```

If it does not run successfully, OpenMLDB needs to be compiled through source code.

## Configure Environment

To ensure a deployment that's both correct and stable, it's advisable to perform the following system configuration steps. These operations assume that the commands are executed on a Linux system.

### Configure the size limit of coredump file and open file limit

```bash
ulimit -c unlimited
ulimit -n 655360
```

The parameters configured through the `ulimit` command are applicable solely to the current session. For persistent configuration, you should incorporate the following settings into the `/etc/security/limits.conf` file:

```bash
*       soft    core    unlimited
*       hard    core    unlimited
*       soft    nofile  655360
*       hard    nofile  655360
```

### Disable system swap

Check if the current system swap is disabled

```bash
$ free
              total        used        free      shared  buff/cache   available
Mem:      264011292    67445840     2230676     3269180   194334776   191204160
Swap:             0           0           0
```

If the swap item is all 0, it means it has been disabled, otherwise run the following command to disable all swap.

```
swapoff -a
```

### Disable THP(Transparent Huge Pages)

Use the following command to check if THP is turned off

```
$ cat /sys/kernel/mm/transparent_hugepage/enabled
always [madvise] never
$ cat /sys/kernel/mm/transparent_hugepage/defrag
[always] madvise never
```

If `never` is not written as (`[never]`) in the above two configurations, the following command needs to be used for configuration:

```bash
echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled
echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag
```

Check if the settings were successful, as shown below:

```bash
$ cat /sys/kernel/mm/transparent_hugepage/enabled
always madvise [never]
$ cat /sys/kernel/mm/transparent_hugepage/defrag
always madvise [never]
```

Note: The above three items can also be modified with one-click through a script, refer to [Modify Machine Environment Configuration](#modifymachineenvironmentconfiguration-optional)

### Time and zone settings

OpenMLDB's data expiration and deletion mechanism relies on the accuracy of the system clock. Incorrect system clock settings can result in either expired data not being removed or non-expired data being deleted.

### Network whitelist

The components comprising the OpenMLDB cluster's services require consistent network connectivity. When it comes to client communication with OpenMLDB clusters, two scenarios exist:

- To establish a connection between the client (CLI and SDKs) and the OpenMLDB cluster, it is essential to not only have connectivity with ZooKeeper but also ensure access to Nameserver/ TabletServer/ TaskManager.
- If the service solely utilizes APIServer for communication, then the client is only required to ensure access to the APIServer port.

## Deploying High Availability Clusters

In production environments, we strongly recommend deploying OpenMLDB clusters with high availability capabilities. For a high availability deployment architecture, we provide our recommended [High Availability Deployment Best Practices](https://chat.openai.com/maintain/backup.md).

## Daemon startup method

OpenMLDB offers two startup modes: Normal and Daemon. Daemon startup provides an additional layer of safeguard by automatically restarting the service process in case of unexpected termination. Regarding daemon startup:

- Daemon is not a system service; launching it unexpectedly will nullify its role as a daemon.
- Each service process corresponds to an independent daemon.
- Stopping the daemon using the `SIGKILL` signal will not lead to the exit of the associated daemon. To revert to normal startup mode for the daemon, you need to halt the associated process and then initiate it as a daemon.
- If the daemon is halted by a non-`SIGKILL` signal, the associated processes will exit upon the daemon's termination.

To commence daemon mode, use either `bash bin/start.sh start <component> mon` or `sbin/start-all.sh mon`. In daemon mode, `bin/<component>.pid` contains the PID of the mon process, while `bin/<component>.pid.child` stores the actual PID of the component.

## Deployment method 1: One-click deployment (recommended)

The OpenMLDB cluster version necessitates the deployment of modules such as ZooKeeper, NameServer, TabletServer, and TaskManager. ZooKeeper serves for service discovery and metadata preservation. NameServer manages TabletServer for achieving high availability and failover. TabletServer stores data and synchronizes master-slave data. APIServer is an optional component; if interaction with OpenMLDB via HTTP is desired, deploying this module is necessary. TaskManager oversees offline jobs. We provide a one-click deployment script to simplify the process, eliminating the need for manual downloading and configuration on each machine.

**Note**: When deploying multiple components on the same machine, it is vital to allocate them distinct directories for streamlined management. This practice is especially crucial while deploying TabletServer, ensuring avoidance of conflicts between data files and log files.

DataCollector and SyncTool currently do not support one-click deployment. Please refer to the manual deployment method for these components.

### Environment requirement

- Deployment machines (machines executing deployment scripts) should have password-less access to other deployment nodes.
- Deployment machine installation using the `rsync` tool.
- Deployment machine installation using Python 3.
- Install Java Runtime Environment (JRE) on machines designated for deploying ZooKeeper and TaskManager.

### Download the OpenMLDB distribution

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.8.2/openmldb-0.8.2-linux.tar.gz
tar -zxvf openmldb-0.8.2-linux.tar.gz
cd openmldb-0.8.2-linux
```

### Environmental configuration

The environment variables are defined in `conf/openmldb-env.sh`, as shown in the following table:

| Environment Variable              | Default Value                                           | 定义                                                         |
| --------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------ |
| OPENMLDB_VERSION                  | 0.8.2                                                   | OpenMLDB版本                                                 |
| OPENMLDB_MODE                     | standalone                                              | standalone或者cluster                                        |
| OPENMLDB_HOME                     | root directory of the release folder                    | openmldb发行版根目录                                         |
| SPARK_HOME                        | $OPENMLDB_HOME/spark                                    | openmldb spark发行版根目录，如果该目录不存在，自动从网上下载 |
| OPENMLDB_TABLET_PORT              | 10921                                                   | TabletServer默认端口                                         |
| OPENMLDB_NAMESERVER_PORT          | 7527                                                    | NameServer默认端口                                           |
| OPENMLDB_TASKMANAGER_PORT         | 9902                                                    | taskmanager默认端口                                          |
| OPENMLDB_APISERVER_PORT           | 9080                                                    | APIServer默认端口                                            |
| OPENMLDB_USE_EXISTING_ZK_CLUSTER  | false                                                   | 是否使用已经部署的ZooKeeper集群。如果是`false`，会在部署脚本里自动启动ZooKeeper集群 |
| OPENMLDB_ZK_HOME                  | $OPENMLDB_HOME/zookeeper                                | ZooKeeper发行版根目录                                        |
| OPENMLDB_ZK_CLUSTER               | auto derived from `[zookeeper]` section in `conf/hosts` | ZooKeeper集群地址                                            |
| OPENMLDB_ZK_ROOT_PATH             | /openmldb                                               | OpenMLDB在ZooKeeper的根目录                                  |
| OPENMLDB_ZK_CLUSTER_CLIENT_PORT   | 2181                                                    | ZooKeeper client port, 即zoo.cfg里面的clientPort             |
| OPENMLDB_ZK_CLUSTER_PEER_PORT     | 2888                                                    | ZooKeeper peer port，即zoo.cfg里面这种配置server.1=zoo1:2888:3888中的第一个端口配置 |
| OPENMLDB_ZK_CLUSTER_ELECTION_PORT | 3888                                                    | ZooKeeper election port, 即zoo.cfg里面这种配置server.1=zoo1:2888:3888中的第二个端口配置 |

### Node configuration

The node configuration file is `conf/hosts`, as shown in the following example:

```bash
[tablet]
node1:10921 /tmp/openmldb/tablet
node2:10922 /tmp/openmldb/tablet

[nameserver]
node3:7527

[apiserver]
node3:9080

[taskmanager]
node3:9902

[zookeeper]
node3:2181:2888:3888 /tmp/openmldb/zk-1
```

The configuration file is segmented into four sections, each identified by `[ ]` brackets:

- `[tablet]`: Configure the node list for deploying TabletServer.
- `[nameserver]`: Specify the node list for deploying NameServer.
- `[apiserver]`: Define the node list for deploying APIServer.
- `[taskmanager]`: List the nodes for configuring and deploying TaskManager.
- `[zookeeper]`: Indicate the node list for deploying ZooKeeper.

For each region's node list, each entry follows the format `host:port WORKDIR`.

In the case of `[zookeeper]`, additional port parameters are included: `zk_peer_port` for follower connection to the leader, and `zk_election_port` for leader election. The format is `host:port:zk_peer_port:zk_election_port WORKDIR`.

Except for the mandatory `host`, other elements in each row of the node list are optional. If omitted, default configurations will be utilized, referencing `conf/openmldb-env.sh` for defaults.

```{warning}
If multiple TaskManager instances are deployed on distinct machines, the configured paths for offline.data.prefix must be accessible across these machines. Configuring the HDFS path is recommended.
```

### Modify machine environment configuration (optional)

```
bash sbin/init_env.sh
```

Explanation:

- This script requires execution by the root user. Execution of other scripts does not necessitate root privileges.
- The script exclusively modifies limit configurations, disabling swap and THP.

### Deployment

```bash
sbin/deploy-all.sh
```

This script will distribute pertinent files to machines configured in `conf/hosts`. Simultaneously, it will incorporate updates to the configuration of related components based on `conf/hosts` and `conf/openmldb-env.sh`.

If you wish to include additional customized configurations for each node, you can modify the settings in `conf/xx.template` prior to running the deployment script. By doing so, when distributing configuration files, each node can utilize the altered configuration.

Running `sbin/deploy-all.sh` repeatedly will overwrite previous configurations.

For comprehensive configuration instructions, kindly consult the [Configuration File](https://chat.openai.com/c/conf.md). It's important to focus on the selection and detailed configuration of TaskManager Spark, as outlined in [Spark Config Details](https://chat.openai.com/c/conf.md#spark-config-details).

### Start the Services

Start normal mode:

```bash
sbin/start-all.sh
```

Alternatively, use daemon mode to start:

```bash
sbin/start-all.sh mon
```

This script will initiate all services configured in `conf/hosts`. After completion of startup, you can utilize the auxiliary script to initiate the CLI (`sbin/openmldb-cli.sh`) for verifying the cluster's normal startup.

```{tip}
start-all.sh is an immensely useful tool. Beyond the deployment phase, it can be employed during operation and maintenance to launch an offline OpenMLDB process. For instance, if a tablet process unexpectedly goes offline, you can directly execute start-all.sh. This script won't impact already initiated processes, but will automatically start configured but uninitiated processes.
```

### Stop the services

If you need to stop all services, you can execute the following script:

```bash
sbin/stop-all.sh
```


## Deployment Method 2: Manual Deployment

The OpenMLDB cluster version necessitates the deployment of modules like ZooKeeper, NameServer, TabletServer, and TaskManager. ZooKeeper serves the purposes of service discovery and metadata information preservation. NameServer is employed to manage TabletServer, ensuring high availability and facilitating failover. TabletServer is responsible for data storage and the synchronization of master-slave data. The presence of APIServer is optional; if you intend to interact with OpenMLDB via HTTP, this module must be deployed. TaskManager, on the other hand, oversees offline jobs.

**Note 1**: When deploying multiple components on the same machine, it's crucial to place them in distinct directories for streamlined management. This is particularly important while deploying TabletServer, as it's essential to avoid directory reuse to prevent conflicts between data and log files.

**Note 2**: The text below uses regular background process mode to initiate components. If you prefer to start components in daemon mode, utilize a command format like `bash bin/start.sh start <component> mon`.

(zookeeper_addr)=

### Deploy ZooKeeper

ZooKeeper requires a version between 3.4 and 3.6, with version 3.4.14 being recommended for deployment. If existing ZooKeeper clusters are available, this step can be skipped. If you're aiming to deploy a ZooKeeper cluster, refer to [here](https://zookeeper.apache.org/doc/r3.4.14/zookeeperStarted.html#sc_RunningReplicatedZooKeeper). This step primarily demonstrates standalone ZooKeeper deployment.

**1. Download the ZooKeeper installation package**

```
wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
tar -zxvf zookeeper-3.4.14.tar.gz
cd zookeeper-3.4.14
cp conf/zoo_sample.cfg conf/zoo.cfg
```

**2. Modify the configuration file**
Open the file `conf/zoo.cfg` and modify `dataDir` and `clientPort`.

```
dataDir=./data
clientPort=7181
```

**3. Start ZooKeeper**

```
bash bin/zkServer.sh start
```

The successful startup will be shown in the following figure, with a prompt of `STARTED`.

![zk started](C:\Users\65972\Documents\GitHub\fix_docs\OpenMLDB\docs\en\deploy\images\zk_started.png)

You can also see the ZooKeeper process running through `ps f|grep zoo.cfg`.

![zk ps](C:\Users\65972\Documents\GitHub\fix_docs\OpenMLDB\docs\en\deploy\images\zk_ps.png)

```{attention}
If you find that the ZooKeeper process failed to start, please check the ZooKeeper.out log in the current directory.
```

**4. Record ZooKeeper service address and connection testing**

In the subsequent stages, while connecting TabletServer, NameServer, and TaskManager to ZooKeeper, you'll need to configure the ZooKeeper service address. To enable cross-host access to the ZooKeeper service, you'll require a public IP (assuming `172.27.128.33` here; remember to input your actual ZooKeeper deployment machine IP). The ZooKeeper service address is inferred from the `clientPort` value entered in the second step, which in this case is `172.27.128.33:7181`.

To test the connection with ZooKeeper, use `zookeeper-3.4.14/bin/zkCli.sh`, and ensure you run it from within the `zookeeper-3.4.14` directory.

```
bash bin/zkCli.sh -server 172.27.128.33:7181
```

You can enter the zk client program, as shown in the following figure, with a prompt of `CONNECTED`.

![zk cli](C:\Users\65972\Documents\GitHub\fix_docs\OpenMLDB\docs\en\deploy\images\zk_cli.png)

Enter `quit` or `Ctrl+C` to exit the zk client.

### Deploy TabletServer

Note that at least two TabletServer need to be deployed, otherwise errors may occur and the deployment cannot be completed correctly.

**1. Download the OpenMLDB deployment package**

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.8.2/openmldb-0.8.2-linux.tar.gz
tar -zxvf openmldb-0.8.2-linux.tar.gz
mv openmldb-0.8.2-linux openmldb-tablet-0.8.2
cd openmldb-tablet-0.8.2
```

**2. Modify the configuration file `conf/tablet.flags`**

```bash
# Modifications can be made based on the sample configuration file
cp conf/tablet.flags.template conf/tablet.flags
```

```{attention}
Please note that the configuration file to modify is conf/tablet.flags, not any other configuration file. Even when starting multiple TabletServers (with independent, non-shareable directories for each), the same configuration file should be modified.
```

Here are the steps for modification:

- Modify the `endpoint`: The `endpoint` comprises an IP address/domain name and port number, separated by a colon (Note: endpoint cannot be set to 0.0.0.0 or 127.0.0.1; it must be a public IP).
- Update `zk_cluster` with the address of the ZooKeeper service that has already been initiated (refer to [Deploy ZooKeeper - 4. Record ZooKeeper service address and connection testing](https://chat.openai.com/c/zookeeper_addr)). If the ZooKeeper service is in a cluster, multiple addresses can be specified, separated by commas. For instance: `172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181`.
- Modify `zk_root_path` with the appropriate value; in this example, `/openmldb_cluster` is utilized. It's crucial to note that **components within the same cluster share the same `zk_root_path`**. In this deployment, all component `zk_root_path` values are set to `/openmldb_cluster`.

```
--endpoint=172.27.128.33:9527
--role=tablet

# if tablet run as cluster mode zk_cluster and zk_root_path should be set
--zk_cluster=172.27.128.33:7181
--zk_root_path=/openmldb_cluster
```

**Note:**

* If the endpoint configuration employs a domain name, all machines utilizing the OpenMLDB client must have the corresponding host configured. Otherwise, access won't be possible.

**3. Start the service**

```
bash bin/start.sh start tablet
```

After startup, there should be a `success` prompt, as shown below.

```
Starting tablet ...
Start tablet success
```

View the process status through the `ps f | grep tablet`.
![tablet ps](C:\Users\65972\Documents\GitHub\fix_docs\OpenMLDB\docs\en\deploy\images\tablet_ps.png)

Through `curl http://<tablet_ip>:<port>/status` You can also test whether TabletServer is running normally.

```{attention}
If you encounter issues like TabletServer failing to start or the process exiting after running for a while, you can examine the logs/tablet.WARNING file within the TabletServer's startup directory. For more detailed information, refer to the logs/tablet.INFO file. If the IP address is already in use, modify the TabletServer's endpoint port and restart it. Prior to starting, ensure that there are no db, logs, or recycle directories within the startup directory. You can use the command rm -rf db logs recycle to delete these directories, preventing legacy files and logs from interfering with current operations. If you are unable to resolve the issue, reach out to the development community and provide the logs for assistance.
```

**4. Repeat the above steps to deploy multiple TabletServers**

```{important}
For clustered versions, the number of TabletServers must be 2 or more. If there's only 1 TabletServer, starting the NameServer will fail. The NameServer logs (logs/nameserver.WARNING) will contain logs indicating "is less than system table replica num."
```

To start the next TabletServer on a different machine, simply repeat the aforementioned steps on that machine. If starting the next TabletServer on the same machine, ensure it's in a different directory and do not reuse a directory where TabletServer is already running.

For instance, you can decompress the compressed package again (avoid using a directory where TabletServer is already running, as files generated after startup may be affected), and name the directory `openmldb-tablet-0.8.2-2`.

```
tar -zxvf openmldb-0.8.2-linux.tar.gz
mv openmldb-0.8.2-linux openmldb-tablet-0.8.2-2
cd openmldb-tablet-0.8.2-2
```

Modify the configuration again and start the TabletServer. Note that if all TabletServers are on the same machine, use different port numbers to avoid a "Fail to listen" message in the logs (`logs/tablet.WARNING`).

**Note:**

- After the service starts, a `tablet.pid` file will be generated in the `bin` directory, storing the process number during startup. If the PID in this file is already running, starting the service will fail.

### Deploy NameServer

```{attention}
Please ensure that all TabletServer have been successfully started before deploying NameServer. The deployment order cannot be changed.
```

**1. Download the OpenMLDB deployment package**

````
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.8.2/openmldb-0.8.2-linux.tar.gz
tar -zxvf openmldb-0.8.2-linux.tar.gz
mv openmldb-0.8.2-linux openmldb-ns-0.8.2
cd openmldb-ns-0.8.2
````

**2.  Modify the configuration file conf/nameserver.flags**

```bash
# Modifications can be made based on the sample configuration file
cp conf/nameserver.flags.template conf/nameserver.flags
```

```{attention}
Please note that the configuration file is conf/nameserver.flags and not any other configuration file. When starting multiple NameServers (each in an independent directory, not shareable), you should modify the configuration file accordingly.
```

* Modify the `endpoint`. The `endpoint` consists of a colon-separated deployment machine IP/domain name and port number (endpoints cannot use 0.0.0.0 and 127.0.0.1, and must be a public IP).
* Modify `zk_cluster` to point to the address of the ZooKeeper service that has already been started (see [Deploy ZooKeeper - 4. Record ZooKeeper Service Address and Connection Testing](https://chat.openai.com/c/zookeeper_addr)). If the ZooKeeper service is a cluster, separate the addresses with commas, for example, `172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181`.
* Modify `zk_root_path`. In this example, `/openmldb_cluster` is used. Note that **components under the same cluster share the same `zk_root_path`**. So in this deployment, the `zk_root_path` for each component's configuration is `/openmldb_cluster`.

```
--endpoint=172.27.128.31:6527
--zk_cluster=172.27.128.33:7181
--zk_root_path=/openmldb_cluster
```

**3. Start the service**

```
bash bin/start.sh start nameserver
```

After startup, there should be a `success` prompt, as shown below.

```
Starting nameserver ...
Start nameserver success
```

You can also use `curl http://<ns_ip>:<port>/status` to check if NameServer is running properly.

**4. Repeat the above steps to deploy multiple NameServer**

Only one NameServer is allowed, and if you need high availability, you can deploy multiple NameServers.

To start the next NameServer on another machine, simply repeat the above steps on that machine. If starting the next NameServer on the same machine, ensure it's in a different directory and do not reuse the directory where NameServer has already been started.

For instance, you can decompress the compressed package again (avoid using the directory where NameServer is already running, as files generated after startup may be affected) and name the directory `openmldb-ns-0.8.2-2`.

```
tar -zxvf openmldb-0.8.2-linux.tar.gz
mv openmldb-0.8.2-linux openmldb-ns-0.8.2-2
cd openmldb-ns-0.8.2-2
```

Then modify the configuration and start.

**Note:**

- After the service starts, a `nameserver.pid` file will be generated in the `bin` directory, storing the process number at startup. If the PID in this file is already running, starting the service will fail.
- Please deploy all TabletServers before deploying the NameServer.

**5. Check if the service is started**

```{attention}
At least one NameServer must be deployed to query the service components that have been started by the current cluster using the following method.
```

```bash
echo "show components;" | ./bin/openmldb --zk_cluster=172.27.128.33:7181 --zk_root_path=/openmldb_cluster --role=sql_client
```

The result is **similar** to the figure below, where you can see all the TabletServer and NameServer that you have already deployed.

```
 ------------------- ------------ --------------- -------- ---------
  Endpoint            Role         Connect_time    Status   Ns_role
 ------------------- ------------ --------------- -------- ---------
  172.27.128.33:9527  tablet       1665568158749   online   NULL
  172.27.128.33:9528  tablet       1665568158741   online   NULL
  172.27.128.31:6527  nameserver   1665568159782   online   master
 ------------------- ------------ --------------- -------- ---------
```

### Deploy APIServer

APIServer is responsible for receiving HTTP requests, forwarding them to the OpenMLDB cluster, and returning the results. It operates in a stateless manner. APIServer is an optional component for OpenMLDB deployment. If you don't require the HTTP interface, you can skip this step and proceed to the next step [Deploy TaskManager](https://chat.openai.com/c/deploy_taskmanager).

Before running APIServer, ensure that the TabletServer and NameServer processes of the OpenMLDB cluster have been started (TaskManager doesn't affect the startup of APIServer). Failure to do so will result in APIServer failing to initialize and exiting the process.

**1. Download the OpenMLDB deployment package**

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.8.2/openmldb-0.8.2-linux.tar.gz
tar -zxvf openmldb-0.8.2-linux.tar.gz
mv openmldb-0.8.2-linux openmldb-apiserver-0.8.2
cd openmldb-apiserver-0.8.2
```

**2. Modify the configuration file conf/apiserver.flags**

```bash
# Modifications can be made based on the sample configuration file
cp conf/apiserver.flags.template conf/apiserver.flags
```

* Modify the `endpoint`. The `endpoint` consists of a colon-separated deployment machine IP/domain name and port number (endpoints cannot use 0.0.0.0 and 127.0.0.1, and must be a public IP).
* Modify `zk_cluster` to point to the address of the ZooKeeper service that has already been started (see [Deploy ZooKeeper - 4. Record ZooKeeper Service Address and Connection Testing](https://chat.openai.com/c/zookeeper_addr)). If the ZooKeeper service is a cluster, separate the addresses with commas, for example, `172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181`.
* Modify `zk_root_path`. In this example, `/openmldb_cluster` is used. Note that **components under the same cluster share the same `zk_root_path`**. So in this deployment, the `zk_root_path` for each component's configuration is `/openmldb_cluster`.

```
--endpoint=172.27.128.33:8080
--zk_cluster=172.27.128.33:7181
--zk_root_path=/openmldb_cluster
```

**Note**:

- If the concurrency of HTTP requests is high, you can increase the number of `--thread_pool_size` on the APIServer, set the default to 16, and restart for changes to take effect.

**3. Start the service**

```
bash bin/start.sh start apiserver
```

After startup, there should be a `success` prompt, as shown below.

```
Starting apiserver ...
Start apiserver success
```

```{attention}
APIServer is a non essential component, so it will not appear in `show components;`.
```

You can use the following command to check if APIServer is running normally. However, it is recommended to test its normal operation by executing SQL commands. To do this:

- Connect to the APIServer using a SQL client
- Execute SQL commands to verify its functionality

```
curl http://<apiserver_ip>:<port>/dbs/foo -X POST -d'{"mode":"online","sql":"show components;"}'
```

The results should include information about all TabletServer and NameServer that have been started.

(deploy_taskmanager)=

### Deploy TaskManager

For TaskManager deployment:

- There can only be one TaskManager instance.
- For high availability, multiple TaskManagers can be deployed, ensuring avoidance of IP port conflicts.
- If the master node of TaskManager fails, the slave node will automatically recover and replace the master node. Client access to the TaskManager service can continue without any modifications.

**1. Download the OpenMLDB deployment package and Spark distribution for feature engineering optimization**

Spark distribution：

```shell
wget https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.8.2/spark-3.2.1-bin-openmldbspark.tgz
# Image address (China)：http://43.138.115.238/download/v0.8.2/spark-3.2.1-bin-openmldbspark.tgz
tar -zxvf spark-3.2.1-bin-openmldbspark.tgz 
export SPARK_HOME=`pwd`/spark-3.2.1-bin-openmldbspark/
```

OpenMLDB deployment package：

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.8.2/openmldb-0.8.2-linux.tar.gz
tar -zxvf openmldb-0.8.2-linux.tar.gz
mv openmldb-0.8.2-linux openmldb-taskmanager-0.8.2
cd openmldb-taskmanager-0.8.2
```

**2. Modify the configuration file conf/taskmanager.properties**

```bash
# Modifications can be made based on the sample configuration file
cp conf/taskmanager.properties.template conf/taskmanager.properties
```

* Modify `server.host`: Set it to the IP address or domain name of the deployment machine.
* Modify `server.port`: Set it to the port number of the deployment machine.
* Modify `zk_cluster`: Set it to the address of the ZooKeeper cluster that has been started. The IP should point to the machine where ZooKeeper is located, and the port should match the `clientPort` configured in the ZooKeeper configuration file. If ZooKeeper is in cluster mode, separate addresses using commas in the format `ip1:port1,ip2:port2,ip3:port3`.
* If sharing ZooKeeper with other OpenMLDB instances, modify `zookeeper.root_path`.
* Modify `batchjob.jar.path`: Set it to the BatchJob Jar file path. If left empty, the system will search in the upper-level lib directory. In Yarn mode, modify it to the corresponding HDFS path.
* Modify `offline.data.prefix`: Set it to the storage path for offline tables. In Yarn mode, modify it to the corresponding HDFS path.
* Modify `spark.master`: Set it according to the desired mode. Currently supports local and yarn modes for running offline tasks.
* Modify `spark.home`: Set it to the Spark environment path. If not configured, the SPARK_HOME environment variable will be used. It should be the directory where the spark-optimized package was extracted in the first step, and it must be an absolute path.

```
server.host=172.27.128.33
server.port=9902
zookeeper.cluster=172.27.128.33:7181
zookeeper.root_path=/openmldb_cluster
batchjob.jar.path=
offline.data.prefix=file:///tmp/openmldb_offline_storage/
spark.master=local
spark.home=
```

For more instructions on Spark related configurations, please refer to the [Spark Config Detail](./conf.md#spark-config-detail).

```{attention}
For distributed deployment clusters, avoid using local files from clients as source data imports. It is strongly recommended to use HDFS paths for this purpose.

When spark.master=yarn, you must use HDFS paths.
When spark.master=local, if you must use a local file, you can copy the file to the host where TaskManager runs and provide the absolute path address on the TaskManager host.

For cases involving large amounts of offline data, it's also advisable to use HDFS for offline.data.prefix instead of local files.
```

**3. Start the service**

```
bash bin/start.sh start taskmanager
```

`ps f|grep taskmanager` should run normally, you can query the status of taskmanager process in `curl http://<taskmanager_ip>:<port>/status `.

```{note}
TaskManager logs are categorized into the TaskManager process logs and job logs for each offline command. These logs are located in the <startup directory>/taskmanager/bin/logs path:

taskmanager.log and taskmanager.out are the TaskManager process logs. Review these logs if the TaskManager process exits unexpectedly.
job_x_error.log contains the running log of a single job, while job_x.log contains the print log of a single job (results of asynchronous selects are printed here). In case of offline task failures, such as job 10, you can retrieve log information using the SQL command SHOW JOBLOG 10;. If your version does not support JOBLOG, locate the corresponding log job on the machine where the TaskManager is located. These logs are named job_10.log and job_10_error.log.
```

**4. Check if the service is started**

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.33:7181  --zk_root_path=/openmldb_cluster --role=sql_client
> show components;
```

The results should be similar to the following table, including all cluster components (except for APIServer).

```
 ------------------- ------------ --------------- -------- ---------
  Endpoint            Role         Connect_time    Status   Ns_role
 ------------------- ------------ --------------- -------- ---------
  172.27.128.33:9527  tablet       1665568158749   online   NULL
  172.27.128.33:9528  tablet       1665568158741   online   NULL
  172.27.128.31:6527  nameserver   1665568159782   online   master
  172.27.128.33:9902  taskmanager  1665649276766   online   NULL
 ------------------- ------------ --------------- -------- ---------
```

To test the normal functioning of the cluster in the SQL client, you can execute the following SQL commands to read and write simple tables (online portion only, for simplicity).

```
create database simple_test;
use simple_test;
create table t1(c1 int, c2 string);
set @@execute_mode='online';
Insert into t1 values (1, 'a'),(2,'b');
select * from t1;
```

### Deployed in offline synchronization tool (optional)

DataCollector in the offline synchronization tool needs to be deployed on the same machine as the TabletServer. Therefore, if offline synchronization is required, DataCollector can be deployed in all TabletServer deployment directories.

SyncTool requires a Java runtime environment and doesn't have any additional requirements. It is recommended to deploy it separately on a single machine.

SyncTool Helper, a synchronization task management tool for SyncTool, can be found in the `tools/synctool_helper.py` section of the deployment package. It requires a Python 3 runtime environment, has no additional requirements, and can be used remotely. However, viewing the debugging information of the Tool requires using Helper on the machine where SyncTool is located.

For detailed deployment instructions, refer to the [Offline Synchronization Tool](https://chat.openai.com/tutorial/online_offline_sync.md). Pay close attention to the version conditions and functional boundaries of the offline synchronization tool.