# Deploy OpenMLDB

## Deploy Zookeeper
Suggest version is `3.4.14`.

You can skip this step if you aready had a zookeeper cluster.

### 1. download zookeeper package

```
wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
tar -zxvf zookeeper-3.4.14.tar.gz
cd zookeeper-3.4.14
cp conf/zoo_sample.cfg conf/zoo.cfg
```

### 2. modify configuration file
open `conf/zoo.cfg`, modify the `dataDir` item and `clientPort` item.

```
dataDir=./data
clientPort=6181
```

### 3. start zookeeper

```
sh bin/zkServer.sh start
```

the deploy of zookeeper cluster is [here](https://zookeeper.apache.org/doc/r3.4.14/zookeeperStarted.html)

## Deploy Nameserver
### 1. download OpenMLDB package

````
wget https://github.com/4paradigm/OpenMLDB/releases/download/0.2.2/openmldb-0.2.2-linux.tar.gz
tar -zxvf openmldb-0.2.2-linux.tar.gz
mv openmldb-0.2.2-linux openmldb-ns-0.2.2
cd openmldb-ns-0.2.2
````

### 2. modify the configuration file: `conf/nameserver.flags`

* modify `endpoint` item
* set `zk_cluster` to the zookeeper cluster you started. the ip is from zookeeper machine, and the port is the value of `clientPort` item in zookeeper configuration file. if your zookeeper is a cluster, then follow this format `ip1:port1,ip2:port2,ip3:port3`
* note that if you share a zookeeper with other OpenMLDB, you must use a different `zk_root_path`

```
--endpoint=172.27.128.31:6527
--role=nameserver
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
--enable_distsql=true
```

**Note: the value of `endpoint` cann't be `0.0.0.0` or `127.0.0.1`**

### 3. start nameserver

```
sh bin/start.sh start nameserver
```

## Deploy Tablet
### 1. download OpenMLDB package

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/0.2.2/openmldb-0.2.2-linux.tar.gz
tar -zxvf openmldb-0.2.2-linux.tar.gz
mv openmldb-0.2.2-linux openmldb-tablet-2.2.0
cd openmldb-tablet-2.2.0
```

### 2. modify configuration file `conf/tablet.flags`
* modify `endpoint`
* set `zk_cluster` to the zookeeper cluster you started.
* note that if you share a zookeeper with other OpenMLDB, you must use a different `zk_root_path`

```
--endpoint=172.27.128.33:9527
--role=tablet

# if tablet run as cluster mode zk_cluster and zk_root_path should be set
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
--enable_distsql=true
```

**Note**
* the value of `endpoint` cann't be `0.0.0.0` or `127.0.0.1`
* make sure set right host for all of the machine that running the client of OpenMLDB if use domain for `endpoint` item. Otherwise, the server will not be able to access
* the value of `zk_cluster` and `zk_root_path` must be same with `nameserver`

### 3. start tablet

```
sh bin/start.sh start tablet
```

**Note: this service will create a new file named `tablet.pid` after started, the file contains a `PID` for current process. if the `PID` is running, `tablet` will failed to start**

Repeat the above steps to deploy multi `nameserver` and `tablet`.

## Deploy APIServer

`APIServer` receive a http request，then forward to `OpenMLDB` and return a response to client. It's stateless, and is not a necessary component for `OpenMLDB`.

Before start `APIServer`, Make sure `OpenMLDB` aready started, or `APIServer` will failed to initialize and exit.

### 1. download OpenMLDB package

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/0.2.2/openmldb-0.2.2-linux.tar.gzz
tar -zxvf openmldb-0.2.2-linux.tar.gz
mv openmldb-0.2.2-linux openmldb-apiserver-0.2.2
cd openmldb-apiserver-0.2.2
```

### 2. modify configuration file `conf/apiserver.flags`

* modify `endpoint`
* set `zk_cluster` to the zookeeper cluster you started.

```
./bin/openmldb --endpoint=172.27.128.33:8080
--role=apiserver
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
--openmldb_log_dir=./logs
```

**Note:**

* the value of `endpoint` cann't be `0.0.0.0` or `127.0.0.1`, you can alse choose to only use `--port` instead of use `--endpoint`.
* to set the thread number of `APIServer`, use `--thread_pool_size`, default value is 16.

### 3. start apiserver

```
sh bin/start.sh start apiserver
```
