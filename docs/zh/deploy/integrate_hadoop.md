# Hadoop 集成

## 介绍

OpenMLDB 支持集成 Hadoop 服务，用户可以配置使用 Yarn 集群来调度运行大数据离线任务，使用 HDFS 分布式存储服务来管理离线数据。

## TaskManager 配置 Yarn

TaskManager 配置文件中可以指定 Yarn 相关配置，相关配置项如下。

| Config | Type | Note |
| ------ | ---- | ---- |
| spark.master | String | Could be `yarn`, `yarn-cluster` or `yarn-client` |
| spark.home | String | The path of Spark home |
| spark.default.conf | String | The config of Spark jobs |

如果使用 Yarn 模式，用户的计算任务会运行在集群上，因此建议配置离线存储路径为 HDFS 路径，否则可能导致任务读写数据失败。配置示例如下。

```
offline.data.prefix=hdfs:///foo/bar/
```
