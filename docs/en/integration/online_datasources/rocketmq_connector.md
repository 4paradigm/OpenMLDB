# RocketMQ

## 1. Why choose RocketMQ OpenMLDB Connector

- To enable efficient and stable transmission channels between OpenMLDB and RocketMQ, the RocketMQ OpenMLDB Connector offers a range of outstanding features, including but not limited to:
  - **User-friendly setup**: No coding is required; a straightforward configuration allows for seamless flow of RocketMQ messages into OpenMLDB through the RocketMQ OpenMLDB Connector. This simplified data import process significantly enhances the effective utilization of enterprise data.
  - **Flexible deployment**: Depending on the specific business requirements, the RocketMQ OpenMLDB Connector can be deployed on a **single machine or as a cluster**, enabling enterprises to build real-time data pipelines.
  - **Robust reliability**: The cluster deployment method of the RocketMQ OpenMLDB Connector incorporates Failover capability, ensuring smooth task scheduling from problematic nodes to normal nodes and maintaining cluster load balancing. This enhances the focus and efficiency of enterprises in exploring the commercial value of their data.
  - **Low latency**: With a capacity to meet real-time data and feature development scenarios with just a **second-level delay**, the RocketMQ OpenMLDB Connector facilitates timely and efficient data processing.

## 2. RocketMQ OpenMLDB Connector

### 2.1. Connector Overview

**Position**

RocketMQ Connect plays a crucial role in facilitating data integration within the RocketMQ ecosystem. It boasts several key features, including low latency, reliability, high performance, low code requirements, and strong scalability. This versatile tool enables seamless connections between various heterogeneous data systems, enabling the construction of data pipelines, ETL (Extract, Transform, Load) processes, CDC (Change Data Capture) operations, data lakes, and more.

The RocketMQ OpenMLDB Connector serves as a dependable and scalable solution for streaming data between RocketMQ and OpenMLDB. Its primary purpose is to simplify the importation of data from RocketMQ and other RocketMQ Connect components into OpenMLDB.

**Function**

It can enable RocketMQ messages to flow into OpenMLDB online storage.

![img](C:\Users\65972\Documents\GitHub\fix_docs\OpenMLDB\docs\en\integration\online_datasources\images\rocketmq_overview.png)

**Connector plugin compilation**

RocketMQ OpenMLDB Connector

```bash
$ git clone git@github.com:apache/rocketmq-connect.git
$ cd rocketmq-connect/connectors/rocketmq-connect-jdbc/
$ mvn clean package -Dmaven.test.skip=true
```

Last but not least, place the compiled plugin package at the loading address specified by RocketMQ connect.

### 2.2. Connector usage demonstration

**Process introduction**

Specifically designed for real-time data stream access in OpenMLDB's online mode, the RocketMQ OpenMLDB Connector follows a straightforward four-step usage process:

- Start OpenMLDB and create a database, such as "rocketmq_test," for testing purposes. The RocketMQ connector automatically handles table creation, eliminating the need for manual intervention.
- Start RocketMQ and create a topic to serve as the communication channel for data transfer.
- Launch the RocketMQ OpenMLDB Connector to establish the connection and enable data streaming between RocketMQ and OpenMLDB.
- Proceed with testing or commence normal use to ensure the smooth and efficient transfer of data.

![img](C:\Users\65972\Documents\GitHub\fix_docs\OpenMLDB\docs\en\integration\online_datasources\images\rocketmq_workflow.png)

**The key steps**

The key steps for using this connector is stated below:

**Step 1 | Start OpenMLDB**

By adhering to these steps, users can leverage the power of RocketMQ OpenMLDB Connector to facilitate real-time data streaming and integration in their online OpenMLDB environment.

```bash
cd /work
./init.sh
echo "create database rocketmq_test;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

**Step 2 | Start RocketMQ**

```bash
Build RocketMQ and start RocketMQ
1. Downlaod RocketMQ
$ wget https://dlcdn.apache.org/rocketmq/4.9.3/rocketmq-all-4.9.3-source-release.zip


2. Compile RocketMQ
If it has already been compiled, please directly execute Part 3 to start RocketMQ

$ unzip rocketmq-all-4.9.3-source-release.zip  
$ cd rocketmq-all-4.9.3/  
$ mvn -Prelease-all -DskipTests clean install -U  
$ cd distribution/target/rocketmq-4.9.3/rocketmq-4.9.3


3. Start RocketMQ
Start namesrv
$ nohup sh bin/mqnamesrv &  
Check if namesrv has been successfully started
$ tail -f ~/logs/rocketmqlogs/namesrv.log  
The Name Server boot success...


Start broker
$ nohup sh bin/mqbroker -n localhost:9876 &
Check if broker has been successfully started
$ tail -f ~/logs/rocketmqlogs/broker.log    
The broker[%s, 172.30.30.233:10911] boot success...
```

**Step 3 | Start RocketMQ OpenMLDB Connector**

First, built the RocketMQ connect runtime environment.

Downloading the project

```bash
$ git clone git@github.com:apache/rocketmq-connect.git
```

Build the project

```bash
$ cd rocketmq-connect
$ mvn -Prelease-connect -DskipTests clean install -U
```

Edit the configuration `connect-standalone.conf` , and the key configurations are as follows:

```bash
$ cd distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT
$ vim conf/connect-standalone.conf
```

```yaml
# Unique Id of the current node
workerId=DEFAULT_WORKER_1

# The port address of the REST API
httpPort=8081

# Local storage path
storePathRootDir=～/storeRoot

# Need to modify to the port address of your own Rocketmq NameServer
# Rocketmq namesrvAddr
namesrvAddr=127.0.0.1:9876  

# Need to modify to the location of the connector plugins folder
# Source or sink connector jar file dir
pluginPaths=/usr/local/connector-plugins/
```

We need to put the compiled package of OpenMLDB RocketMQ Connector into this directory. The command is as follows:

```bash
mkdir -p /usr/local/connector-plugins/rocketmq-connect-jdbc
cd ../../../../
cp connectors/rocketmq-connect-jdbc/target/rocketmq-connect-jdbc-0.0.1-SNAPSHOT-jar-with-dependencies.jar /usr/local/connector-plugins/rocketmq-connect-jdbc
```

Use the standalone mode to start the RocketMQ Connect Runtime environment.

```bash
$ cd distribution/target/rocketmq-connect-0.0.1-SNAPSHOT/rocketmq-connect-0.0.1-SNAPSHOT
$ sh bin/connect-standalone.sh -c conf/connect-standalone.conf &
```

The following indicates that RocketMQ connect runtime has run successfully:

![img](C:\Users\65972\Documents\GitHub\fix_docs\OpenMLDB\docs\en\integration\online_datasources\images\rocketmq_runtime.png)

**Step 4 | Test**

![img](C:\Users\65972\Documents\GitHub\fix_docs\OpenMLDB\docs\en\integration\online_datasources\images\rocketmq_test.png)

- Create a Mysql data table and initialize test data

- Create a mysql source and pull data from the test table

- Create an OpenMLDB sink and write the data pulled from the source to OpenMLDB

Initialize MySQL test data;

```sql
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;


-- ----------------------------
-- Table structure for employee_test
-- ----------------------------
DROP TABLE IF EXISTS `employee_test`;
CREATE TABLE `employee_test` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `name` varchar(128) DEFAULT NULL,
  `howold` int DEFAULT NULL,
  `male` int DEFAULT NULL,
  `company` varchar(128) DEFAULT NULL,
  `money` double DEFAULT NULL,
  `begin_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8;


-- ----------------------------
-- Records of employee_test
-- ----------------------------
BEGIN;
INSERT INTO `employee_test` VALUES (2, 'name-02', 19, 7, 'company', 32232, '2021-12-29 08:00:00');
INSERT INTO `employee_test` VALUES (4, 'gjk', 25, 8, 'company', 3232, '2021-12-24 20:43:36');
INSERT INTO `employee_test` VALUES (12, 'name-06', 19, 3, NULL, NULL, NULL);
INSERT INTO `employee_test` VALUES (14, 'name-08', 25, 15, 'company', 32255, '2022-02-08 19:06:39');
COMMIT;


SET FOREIGN_KEY_CHECKS = 1;
```

Create and start RocketMQ conect mysql source connector, the examples is as follows:

```bash
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/jdbc-mysql-source-test
-d  '{
    "connector-class":"org.apache.rocketmq.connect.jdbc.connector.JdbcSourceConnector",
    "max-task":"1",
    "connection.url":"jdbc:mysql://127.0.0.1:3306", 
    "connection.user":"*****",   
    "connection.password":"*****", 
    "table.whitelist":"test_database.employee_test",
    "mode": "incrementing",     // Incremental pull method
    "incrementing.column.name":"id",   // Specify fields for incremental pull
    "source-record-converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter"
}'st
```

Confirm task initiation and start pulling data:![img](https://pic3.zhimg.com/80/v2-80395baf4060f32bb0e86d959ad6ecae_1440w.webp)

![img](https://pic2.zhimg.com/80/v2-2e76f61a818c5bc31cdd18eca2a8b4ed_1440w.webp)

To create an OpenMLDB RocketMQ sink connector for writing data to the OpenMLDB table, please refer to the following information. (Note: The listening topic corresponds to the table name of the source pull table.)

```bash
curl -X POST -H "Content-Type: application/json" http://127.0.0.1:8082/connectors/jdbc-openmldb-sink-test
-d '{
    "connector-class":"org.apache.rocketmq.connect.jdbc.connector.JdbcSinkConnector",
    "max-task":"1",
    "connect-topicname":"employee_test",
    "connection.url":"jdbc:openmldb:///rocketmq_test?zk=127.0.0.1:2181&zkPath=/openmldb_cluster",
    "insert.mode":"INSERT",
    "db.timezone":"UTC",
    "table.types":"TABLE",
    "auto.create":"true",
    "source-record-converter":"org.apache.rocketmq.connect.runtime.converter.JsonConverter"
}'
```

By monitoring the data entry, we can verify the successful insertion into OpenMLDB. The details are as follows:

```sql
set @@execute_mode='online';
use rocketmq_test;
select * from employee_test;
```

