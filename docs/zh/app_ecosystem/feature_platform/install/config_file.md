# 特征平台配置文件

## 介绍

OpenMLDB 特征平台基于 Spring Boot 开发，使用 `application.yml` 规范作为配置文件。

## 配置示例

简化版配置示例如下：

```
server:
  port: 8888
 
openmldb:
  zk_cluster: 127.0.0.1:2181
  zk_path: /openmldb
  apiserver: 127.0.0.1:9080
```

## 配置项目


| 配置项目 | 介绍 | 类型 | 示例 |
| ------- | --- | --- | ---- |
| server.port | 服务端口 | int | 8888 |
| openmldb.zk_cluster | ZooKeeper 集群地址 | string | 127.0.0.1:2181 |
| openmldb.zk_path | OpenMLDB 根路径 | string | /openmldb |
| openmldb.apiserver | OpenMLDB APIServer 地址 | string | 127.0.0.1:90 |
| openmldb.skip_index_check | 是否跳过索引检查 | boolean | false |