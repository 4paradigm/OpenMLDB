# Pulsar OpenMLDB JDBC Connector Usage Demo
We use the OpenMLDB cluster and a simple JSON message producer to show how the OpenMLDB JDBC Connector works. 
You should download the demo files pkg in release, which are needed by this demo, such as the connector nar, schema files, config files…
All steps are recorded, see https://terminalizer.com/view/987414935665 , or download the demo script [demo.yml](demo.yml).

## Start OpenMLDB Cluster
Use docker to start it simply. And create a test table. Detail in 集群版OpenMLDB 快速上手 . Note that, only OpenMLDB cluster mode can be the sink dist.
We recommend that you use ‘host network’ to run docker. And bind volume ‘files’ too. The sql scripts are in it.
```
docker run -dit --network host -v `pwd`/files:/work/taxi-trip/files --name openmldb 4pdosc/openmldb:0.4.4 bash
docker exec -it openmldb bash
```
*Even the host network, docker on macOS cannot support connecting to the container from the host. You only can connect openmldb cluster in the containers(pulsar container, or openmldb container itself).
In OpenMLDB container, start the cluster, and enter the client, and create the table:
```
./init.sh
```
We use a script to create the table, create.sql content: 
```
create database pulsar_test;
use pulsar_test;
create table connector_test(id int, time bigint, index(key=id,ts=time));
desc connector_test;
```
Run the script:
```
../openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < files/create.sql
```
Note: JSONSchema and JDBC connector can't support 'java.sql.Timestamp' now. So we use 'long' to be the timestamp column type(it works in OpenMLDB).

## Start Pulsar(standalone) 
### Pulsar docker
It’s simpler and quicker to run Pulsar in docker. 
But note that, the sink connector in pulsar must connect to the openmldb cluster. You may need to find the openmldb cluster network address(cluster in host, or cluster in container). And there are some differences about docker between linux and macos. So we recommend that you use ‘host network’ to run docker.
And we need to use pulsar-admin to create a sink, it’s in the docker container. So we should attach it, and start Pulsar later.
Don’t forget to bind the dir ‘files’.
```
docker run -dit --network host -v `pwd`/files:/pulsar/files --name pulsar apachepulsar/pulsar:2.9.1 bash
docker exec -it pulsar bash
```

If you really want to start pulsar locally, see Set up a standalone Pulsar locally .

### Connector installation(Optional)
In the previous step, we bind mount ‘files’, the connector nar is in it.
We’ll use ‘non built-in connector’ mode to set up the connector(use ‘archive’ in sink config).

If you really want the connector to be the built-in connector, copy it to ‘connectors’.
```
mkdir connectors
cp files/pulsar-io-jdbc-openmldb-2.11.0-SNAPSHOT.nar  connectors/
```
You want to change or add more connectors, you can update connectors when pulsar standalone is running:
```
bin/pulsar-admin sinks reload
```

### Start
In pulsar docker or your pulsar path, start the pulsar standalone server and check if the connector exists.
```
bin/pulsar-daemon start standalone --zookeeper-port 5181
```
* openmldb want to use the port 2181, so we should change the zk port here. We will use openmldb zk port to connect openmldb, but zk port in pulsar standalone won’t affect anything.
You can check if the pulsar runs well.
```
ps axu|grep pulsar
```
When you start a local standalone cluster, a public/default namespace is created automatically. The namespace is used for development purposes. 
We will create the sink in the namespace.

### Q&A
Q:
```
2022-04-07T03:15:59,289+0000 [main] INFO  org.apache.zookeeper.server.NIOServerCnxnFactory - binding to port 0.0.0.0/0.0.0.0:2181
2022-04-07T03:15:59,289+0000 [main] ERROR org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble - Exception while instantiating ZooKeeper
java.net.BindException: Address already in use
```
A: change the address of zk ‘zookeeperServers’ in conf/standalone.conf, Pulsar wants an unused address to start zk server.

Q: 8080 is already used.
A: change the port ‘webServicePort’ in conf/standalone.conf. Don’t forget the ‘webServiceUrl’ in conf/client.conf, pulsar-admin needs the conf.

Q: 6650 port
A: ‘brokerServicePort’ in conf/standalone.conf

## Setup sink connector
We use the ‘public/default’ namespace to create sink, and we need a sink config file, it’s files/pulsar-openmldb-jdbc-sink.yaml
content:
```
 tenant: "public"
 namespace: "default"
 name: "openmldb-test-sink"
 sinkType: "jdbc-openmldb"
 archive: "files/pulsar-io-jdbc-openmldb-2.11.0-SNAPSHOT.nar"
 inputs: ["test_openmldb"]
 configs:
     jdbcUrl: "jdbc:openmldb:///pulsar_test?zk=localhost:2181&zkPath=/openmldb"
     tableName: "connector_test"
```
Then create a sink and check it, notice that the input topic is 'test_openmldb'. No need to set the tenant or namespace when getting status.
```
./bin/pulsar-admin sinks create --sink-config-file files/pulsar-openmldb-jdbc-sink.yaml
./bin/pulsar-admin sinks status --name openmldb-test-sink
```

## Create Schema
Upload schema to topic 'test_openmldb', schema type is JSON. We’ll produce the JSON message in the same schema later. The chema file is ‘files/openmldb-table-schema’.
Schema content:
```
{
    "type": "JSON",
    "schema": "{\"type\":\"record\",\"name\":\"OpenMLDBSchema\",\"namespace\":\"com.foo\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"time\",\"type\":\"long\"}]}",
    "properties": {}
}
```

Upload schema and check it, commands:
```
./bin/pulsar-admin schemas upload test_openmldb -f ./files/openmldb-table-schema
./bin/pulsar-admin schemas get test_openmldb
```

## Produce a message
Producer JAVA code in https://github.com/vagetablechicken/pulsar-client-java . The package is in ‘files’.
TODO: make pulsar address configable
```
java -cp files/pulsar-client-java-1.0-SNAPSHOT-jar-with-dependencies.jar org.example.Client
```

Client will send 2 messages to topic ‘test_openmldb’, and then Pulsar will read the message and write it to OpenMLDB cluster online storage.
We can check the sink status:
```
./bin/pulsar-admin sinks status --name openmldb-test-sink 
```

## Check in OpenMLDB
And we can check the OpenMLDB table’s online storage now. 
The script select.sql content:
```
set @@execute_mode='online';
use pulsar_test;
select * from connector_test;
```
In OpenMLDB container, run:
```
../openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < files/select.sql
```
