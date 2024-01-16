# 安装包

## 介绍

使用官方预编译的安装包，只需要本地有 Java 环境就可以快速部署 OpenMLDB 特征平台。

注意，需参考 [OpenMLDB 部署文档](../../../deploy/index.rst) 提前部署 OpenMLDB 集群。

## 下载

下载 Jar 文件。

```
wget https://openmldb.ai/download/feature-platform/openmldb-feature-platform-0.1-SNAPSHOT.jar
```

## 配置

参考[特征平台配置文件](./config_file.md)，创建 `application.yml` 配置文件。

```
server:
  port: 8888
 
openmldb:
  zk_cluster: 127.0.0.1:2181
  zk_path: /openmldb
  apiserver: 127.0.0.1:9080
```

## 启动

启动特征平台服务。

```
java -jar ./openmldb-feature-platform-0.1-SNAPSHOT.jar
```

