# Deployment

## Environment requirements

1. `Java 11+`

## Download OpenM(ysq)LDB distribution

Download address: [open-mysql-db](https://openmldb.ai/download/openmysqldb/open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar)

```shell
wget https://openmldb.ai/download/openmysqldb/open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Configuration

Please deploy OpenMLDB Cluster in advance and create the `server.properties` configuration file in the directory of `open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependency.jar`. The contents of the `server.properties` configuration file are created as follows

```
# OpenM(ysq)LDB Service Port
server.port=3307

# OpenMLDB Cluster zk config
zookeeper.cluster=0.0.0.0:2181
zookeeper.root_path=/openmldb

# OpenMLDB Cluster username and password
openmldb.user=root
openmldb.password=root
```

## Start the OpenM(ysq)LDB service

Use the following command to start the OpenM(ysq)LDB service

```
java -jar open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

## Test verification

Use the `mysql` command to connect to the OpenM(ysq)LDB service

```
mysql -h127.0.0.1 -P3307 -uroot -proot
```

Execute any ANSI SQL or OpenMLDB SQL.

```
show databases;

insert into db1.t1 values("user1", 10);

select age from db1.t1;

show jobs;

stop job 1;
```
