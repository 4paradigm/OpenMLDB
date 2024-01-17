# 常见问题

## OpenMLDB 特征平台和主流 Feature Store 有什么区别？

主流 Feature Store 包括 Feast、Tecton、Feathr 等提供了特征管理和计算能力，在线存储主要使用 Redis 等预聚合 Key-value 存储。OpenMLDB 特征平台提供的是实时计算特征的能力，特征抽取方案无论怎样修改都可以直接一键上线而不需要重新上线和同步在线数据。主要的功能对比如下。

| 特征存储系统 | Feast              | Tecton            | Feathr            | OpenMLDB 特征平台 |
| ----------------- | ------------------ | ----------------- | ----------------- | ----------------- |
| 数据源支持        | 多种数据源         | 多种数据源         | 多种数据源         | 多种数据源 |
| 可扩展性          | 高                  | 高                 | 中到高             | 高 |
| 实时特征服务      | 支持               | 支持              | 支持              | 支持 |
| 批处理特征服务    | 支持               | 支持              | 支持              | 支持 |
| 特征转换          | 支持基本转换       | 支持复杂转换和 SQL      | 支持复杂转换      | 支持复杂转换和 SQL |
| 数据存储          | 支持多种存储选项   | 主要支持云存储    | 支持多种存储选项   | 内置高性能时序数据库，支持多种存储选项 |
| 社区和支持        | 开源社区     | 商业支持          | 开源社区     | 开源社区 |
| 实时特征计算 | 不支持 | 不支持 | 不支持 | 支持 |

## 部署特征平台是否需要 OpenMLDB ？

需要，因为特征平台的云数据存储以及特征计算依赖 OpenMLDB 集群，因此部署特征平台需要提前部署 OpenMLDB 集群，也可以使用整合两者的 [Docker 镜像](./install/docker.md)一键部署。

使用特征平台后用户可以不依赖 OpenMLDB CLI 或 SDK 来实现特征的开发和上线，通过 Web 界面就可以完成特征工程的所有上线需求。

## 如何基于特征平台实现 MLOps 工作流？

使用 OpenMLDB 特征平台，可以在 Web 前端完成数据库、数据表的创建，然后提交在线数据和离线数据的导入工作。使用 OpenMLDB SQL 语法进行数据的探索以及特征的创建，然后就可以离线特征的导出以及在线特征的一键上线，从 MLOps 对离线到在线流程不需要任何额外的开发工作，具体流程可参考[快速入门](./quickstart.md)。

## 特征平台的生态集成支持如何？

OpenMLDB 特征平台依托于 OpenMLDB 生态，支持与 OpenMLDB 生态中的其他组件进行集成。

例如与 OpenMLDB 生态中的数据集成组件进行集成，支持 [Kafka](../../integration/online_datasources/kafka_connector_demo.md)、[Pulsar](../../integration/online_datasources/pulsar_connector_demo.md)、[RocketMQ](../../integration/online_datasources/rocketmq_connector.md)、[Hive](../../integration/offline_data_sources/hive.md)、[Amazon S3](../../integration/offline_data_sources/s3.md)，调度系统支持 [Airflow](../../integration/deploy_integration/airflow_provider_demo.md)、[DolphinScheduler](../../integration/deploy_integration/dolphinscheduler_task_demo.md)、[Byzer](../../integration/deploy_integration/OpenMLDB_Byzer_taxi.md) 等，对于 Spark Connector 支持的 HDFS、Iceberg 等和云计算相关的 Kubernetes、阿里云 MaxCompute 等也有一定程度的支持。

## 特征平台有什么业务价值和技术含量？

相比于使用 HDFS 存储离线数据、Redis 存储在线数据的简易版 Feature Store，OpenMLDB 特征平台的价值在于使用了 OpenMLDB SQL 这种在线离线一致性的特征抽取语言。对于特征开发的科学家，只需要编写 SQL 逻辑就可以完成特征定义，在离线场景下这个 SQL 会被翻译成分布式 Spark 应用来执行，在在线场景下同样的 SQL 会被翻译成在线时序数据库的查询语句来执行，实现特征的在线和离线一致性。

目前 SQL 编译器、在线存储引擎、离线计算引擎都是基于 C++ 和 Scala 等编程语言实现的，对于非技术背景的科学家来说，使用 SQL 语言来定义特征开发流程，可以降低学习成本，提高开发效率。所有代码都是开源可用，OpenMLDB 项目地址 https://github.com/4paradigm/openmldb ，OpenMLDB 特征平台项目地址 https://github.com/4paradigm/feature-platform 。
