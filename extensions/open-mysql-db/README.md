# OpenM(ysq)LDB

## Introduction

This is the server of OpenMLDB to be compatible with MySQL client. That means you can use `mysql` command to connect with OpenMLDB and complete any SQL operation.

## Installation

Make sure to use Java 11+ and setup `$JAVA_HOME`. Use maven to build the project or download the jar of [open-mysql-db](https://openmldb.ai/download/openmysqldb/open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar) .

```
# mvn clean package
# cd ./target/

wget https://openmldb.ai/download/openmysqldb/open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

Setup OpenMLDB cluster in advanced and create a config file named `server.properties` in the directory of jar file.

```
server.port=3307

zookeeper.cluster=0.0.0.0:2181
zookeeper.root_path=/openmldb

openmldb.user=root
openmldb.password=root
```

Use the following command to start the server.

```
java -jar open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

> If `server.properties` has been put in other directory (e.g. /home/work/config/server.properties), please use following command to start the server
> ```
> # cd <directory of open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar>
> # please replace the first classpath value /home/work/config with directory of server.properties
> java -classpath /home/work/config:open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar com._4paradigm.openmldb.mysql.server.OpenmldbMysqlServer
> ```

## Client

Use `mysql` command to connect.

```
mysql -h127.0.0.1 -P3307 -uroot -proot
```

Run adhoc ANSI SQL or even OpenMLDB SQL.

```
show databases;

insert into db1.t1 values("user1", 10);

select age from db1.t1;

show jobs;

stop job 1;
```

## Related Projects

The code of analysing MySQL Protocol are copied from [mysql-protocol](https://github.com/paxoscn/mysql-protocol) which is forked from [netty-mysql-codec](https://github.com/mheath/netty-mysql-codec) .
