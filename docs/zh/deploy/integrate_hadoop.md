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

## 获取 Yarn 日志

OpenMLDB 支持 `SHOW JOBLOG` 命令可以直接获取任务日志，使用 `local` 或 `yarn-client` 模式时，driver 日志在 TaskManager 本地可以实时获得。如果使用 `yarn-cluster` 模式，可以实时获得客户端提交的日志，但 driver 日志需要在 Yarn 页面查看，也可以通过 `SHOW JOBLOG` 命令获得远端日志，目前存在以下限制。

* 只支持 Hadoop 3.x 版本，已经通过测试的版本为3.3.4。
* 用户需要在 Yarn 集群开启日志聚合功能，可在 `core-site.xml` 文件中添加配置 `yarn.log-aggregation-enable`。
* 需要等待 Yarn 任务完成后，才可以获得完整的 driver 日志。
