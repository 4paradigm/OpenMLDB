# Installation Package

## Introduction
You can deploy FeatInsight quickly with official pre-built installation package and Java environment.

Note that you need to deploy OpenMLDB cluster first, refer to [OpenMLDB Deployment](../../../deploy/index.rst).

## Download

Download Jar file.

```
wget https://openmldb.ai/download/featinsight/featinsight-0.1.0-SNAPSHOT.jar
```

## Configuration

Refer to [FeatInsight Configuration](./config_file.md) to create an `application.yml` configuration file.

```
server:
  port: 8888
 
openmldb:
  zk_cluster: 127.0.0.1:2181
  zk_path: /openmldb
  apiserver: 127.0.0.1:9080
```

## Start

Start FeatInsight service.

```
java -jar ./featinsight-0.1.0-SNAPSHOT.jar
```

