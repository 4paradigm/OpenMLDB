# 安装部署

## 环境要求

1. 依赖 `Java 11+`

## 下载OpenM(ysq)LDB发行版

下载地址 [open-mysql-db](https://openmldb.ai/download/openmysqldb/open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar)

```shell
wget https://openmldb.ai/download/openmysqldb/open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 配置

请提前部署好OpenMLDB集群，并在 `open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar` 目录下创建`server.properties`配置文件，`server.properties`配置文件内容如下

```
# OpenM(ysq)LDB服务端口
server.port=3307

# OpenMLDB集群zk配置
zookeeper.cluster=0.0.0.0:2181
zookeeper.root_path=/openmldb

# OpenMLDB集群用户名密码
openmldb.user=root
openmldb.password=root
```

## 启动OpenM(ysq)LDB服务

使用如下命令启动OpenM(ysq)LDB服务

```
java -jar open-mysql-db-0.1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 测试验证

使用 `mysql` 命令连接OpenM(ysq)LDB服务

```
mysql -h127.0.0.1 -P3307 -uroot -proot
```

执行任意ANSI SQL或者OpenMLDB专有SQL。

```
show databases;

insert into db1.t1 values("user1", 10);

select age from db1.t1;

show jobs;

stop job 1;
```
