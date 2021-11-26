
<div align=center><img src="./images/openmldb_logo.png" width="400"/></div>

[![build status](https://github.com/4paradigm/openmldb/actions/workflows/cicd.yaml/badge.svg)](https://github.com/4paradigm/openmldb/actions/workflows/cicd.yaml)
[![docker pulls](https://img.shields.io/docker/pulls/4pdosc/openmldb.svg)](https://hub.docker.com/r/4pdosc/openmldb)
[![slack](https://img.shields.io/badge/Slack-Join%20Slack-blue)](https://join.slack.com/t/hybridsql-ws/shared_invite/zt-ozu3llie-K~hn9Ss1GZcFW2~K_L5sMg)
[![discuss](https://img.shields.io/badge/Discuss-Ask%20Questions-blue)](https://github.com/4paradigm/OpenMLDB/discussions)
[![codecov](https://codecov.io/gh/4paradigm/OpenMLDB/branch/main/graph/badge.svg?token=OMPII8NGN2)](https://codecov.io/gh/4paradigm/OpenMLDB)
[![release](https://img.shields.io/github/v/release/4paradigm/OpenMLDB?color=lime)](https://github.com/4paradigm/OpenMLDB/releases)
[![license](https://img.shields.io/github/license/4paradigm/OpenMLDB?color=orange)](https://github.com/4paradigm/OpenMLDB/blob/main/LICENSE)
[![gitee](https://img.shields.io/badge/Gitee-mirror-lightyellow)](https://gitee.com/paradigm4/OpenMLDB)
[![maven central](https://img.shields.io/maven-central/v/com.4paradigm.openmldb/openmldb-batch)](https://mvnrepository.com/artifact/com.4paradigm.openmldb/openmldb-batch)
[![maven central](https://img.shields.io/maven-central/v/com.4paradigm.openmldb/openmldb-jdbc)](https://mvnrepository.com/artifact/com.4paradigm.openmldb/openmldb-jdbc)
[![pypi](https://img.shields.io/pypi/v/openmldb)](https://pypi.org/project/openmldb/)

[English version](./README.md)|中文版

## 1. 介绍

OpenMLDB 是一个开源机器学习数据库，为机器学习应用高效供给正确数据。 面向机器学习的数据库主要覆盖两方面功能，即特征计算和特征存取，一起来为机器学习线下模型训练和线上推理服务提供数据供给。传统上，一般会有两套分离的系统来作为线上务和线下模型训练的数据供给。因此，线上线下结果的一致性校验常常会花费大量的开发和沟通成本。与之相反，OpenMLDB 为机器学习的线上和线下的数据供给，提供了统一的 SQL 编程接口和底层执行引擎。因此，线上线下一致性在使用 OpenMLDB 后可以做到自动高效保证。另外，我们也针对线上线下的工作负载特点，特别做了系统层面的优化来保证运行效率。现在基于 OpenMLDB，开发者可以仅仅通过编写 SQL 脚本来实现高效正确的针对机器学习应用的数据供给，真正达到开发即上线的一步到位流程。

<p align="center">
 <img src="images/workflow.png" alt="image-20211103103052252" width=700 />
</p>

上图显示了基于 OpenMLDB 的一个典型的开发部署流程。开发者首先基于 SQL 脚本进行离线的特征计算和模型开发。当模型质量达到满意以后，通过实时数据接入以后，OpenMLDB 可以立即切换到线上服务数据供给模式，而不需要任何额外的开发和人力成本。因此在整体流程中，由于 OpenMLDB 天然保障了线上线下的数据一致性，耗费大量开发和人力成本的数据一致性校验就不再需要。另外，我们也做了很多系统优化来保障整体性能，比如针对离线特征计算的窗口并行以及数据倾斜优化，以及针对线上服务的内存数据索引等。总结来说，基于 OpenMLDB，开发者只需要掌握 SQL 编程开发，即能保障机器学习的线上线下数据一致性供给，实现开发即上线的全流程。

## 2. 主要特性

### 2.1 SQL 编程

我们相信，基于 SQL 语言的简洁高效的设计和广泛使用，SQL 将会是特征工程的最适合的编程语言。因此，OpenMLDB 使得开发者仅仅需要使用 SQL，就能完成线上线下的特征计算和存取的全部任务。此外，我们也对标准 SQL 语法做了若干扩展，使得可以针对特征计算场景做到更加强大高效。

### 2.2 线上线下一致性

配合 SQL 编程接口，我们同样设计了底层统一的计算执行引擎。因此，线上线下一致性在基于 OpenMLDB 的编程流程中，得到了天然的保证而无需付出额外开发代价。

### 2.3 高性能

为了保证线下和线上特征计算存取的高性能，OpenMLDB 提出了具有针对性的系统优化技术。基于这些优化，离线特征计算的性能显著好于现有的开源大数据处理框架。而对于性能延迟非常敏感的线上服务，OpenMLDB 可以在高吞吐压力下提供几十毫秒量级的延迟，满足线上预估服务的性能要求。

你可以阅读我们的学术论文和技术博客（章节 7. 学术论文和技术博客）来理解更多的关于 OpenMLDB 的技术细节。

### 2.4 命令行客户端

OpenMLDB 提供了一个强大的整合的命令行客户端。基于命令行，用户可以完成 SQL 开发，任务管理，线上线下部署，数据库管理等任务。对于熟悉传统数据库命令工具的开发者来说，使用 OpenMLDB 的命令行客户端将会非常易用。

*注意，当前版本 0.3.0 的命令行客户端对于集群模式仅做部分功能支持。将会在下一个版本 0.4.0 中做完全支持。*

## 3. 编译和安装

:point_right: [点击这里](docs/cn/compile.md)

## 4. Demo & QuickStart

从 0.3.0 版本开始，OpenMLDB 引入了两种部署工作模式：集群模式和单机模式。集群模式为基于大数据的实际业务场景提供了高性能的集群模式，具备高可扩展和高可用的特点。单机模式更适合于小数据场景或者测试试用目的，可以更加方便的部署、开发和使用。

我们演示基于这两种模式的 demo 和快速上手指南：

- :point_right: [Demo 代码](demo)
- :point_right: [集群模式快速上手指南](docs/cn/cluster.md)
- :point_right: [单机模式快速上手指南](docs/cn/standalone.md)
- :point_right: [性能敏感模式说明](docs/cn/performance_sensitive_mode.md)

## 5. 开发计划

OpenMLD 社区持续进行开发迭代，在此列出我们已经初步规划好的在未来版本的主要支持特性，如果想详细了解我们的计划，或者提供任何的建议，请加入我们的社区来参与互动。

| 版本号 | 预期发布日期 | 主要特性                                                     |
| ------ | ------------ | ------------------------------------------------------------ |
| 0.4.0  | End of 2021  | - CLI 完全支持单机和集群模式的所有功能                       |
| 0.5.0  | 2022 Q1      | - 在线服务监控模块<br />- 长时间窗口支持 <br />- 支持第三方在线数据流引入，包括 Kafka 和 Pulsar |

## 6. 社区

- **Email**: [contact@openmldb.ai](mailto:contact@openmldb.ai)
- **[Slack Workspace](https://join.slack.com/t/openmldb/shared_invite/zt-ozu3llie-K~hn9Ss1GZcFW2~K_L5sMg)**: 你可以在 Slack 上找到我们，通过在线聊天的方式，获取关于 OpenMLDB 的使用和开发支持。

- **GitHub Issues 和 Discussions**: 如果你是一个严肃的开发者，我们非常欢迎加入我们 GitHub 上的开发者社区，近距离参与我们的开发迭代。GitHub Issues 主要用来搜集 bugs 以及反馈新特性需求；GitHub Discussions 主要用来给开发团队发布并且讨论 RFCs。
- [**技术博客**](https://www.zhihu.com/column/c_1417199590352916480)
- **微信交流群：**
  <img src="images/wechat.png" alt="img" width=100 />  

## 7. 学术论文和技术博客

* Cheng Chen, Jun Yang, Mian Lu, Taize Wang, Zhao Zheng, Yuqiang Chen, Wenyuan Dai, Bingsheng He, Weng-Fai Wong, Guoan Wu, Yuping Zhao, and Andy Rudoff. *[Optimizing in-memory database engine for AI-powered on-line decision augmentation using persistent memory](http://vldb.org/pvldb/vol14/p799-chen.pdf)*. International Conference on Very Large Data Bases (VLDB) 2021.
* [第四范式OpenMLDB优化创新论文被国际数据库顶会VLDB录用](https://zhuanlan.zhihu.com/p/401513878)
* [OpenMLDB在银行上线事中交易反欺诈模型实践](https://zhuanlan.zhihu.com/p/389599785)
* [OpenMLDB在AIOPS领域关于交易系统异常检测应用实践](https://zhuanlan.zhihu.com/p/393602288)
* [5分钟完成硬件剩余寿命智能预测](https://zhuanlan.zhihu.com/p/399346826)

