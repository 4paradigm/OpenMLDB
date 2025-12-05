# FeatInsight Configuration File

## Introduction

FeatInsight is developed based on Spring Boot. It uses the standard `application.yml` as configuration file.

## Example Configuration

A simplified configuration file example is as follows:

```
server:
  port: 8888
 
openmldb:
  zk_cluster: 127.0.0.1:2181
  zk_path: /openmldb
  apiserver: 127.0.0.1:9080
```

## Configuration Items


| Item                      | Definition                  | Type    | Example        |
| --------------------------| --------------------------- | ------- | -------------- |
| server.port               | port for service            | int     | 8888           |
| openmldb.zk_cluster       | ZooKeeper address           | string  | 127.0.0.1:2181 |
| openmldb.zk_path          | OpenMLDB root path          | string  | /openmldb      |
| openmldb.apiserver        | OpenMLDB APIServer address  | string  | 127.0.0.1:9080 |
| openmldb.skip_index_check | whether to skip index check | boolean | false          |