
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

**[English version](./README.md) | 中文版**

#### OpenMLDB 是一个开源机器学习数据库，提供企业级 FeatureOps 全栈解决方案。

## 1. 设计理念

在人工智能工程化落地过程中，企业的数据和工程化团队 95% 的时间精力会被数据处理、数据校验等相关工作所消耗。为了解决该痛点，1% 的头部企业会花费上千小时自研构建数据与特征平台，来解决诸如线上线下一致性、数据穿越、高并发低延迟、高可用等工程挑战；其他 99% 的企业则采购高昂的 SaaS 工具和数据治理服务。 

OpenMLDB 致力于闭环解决 AI 工程化落地的数据治理难题，并且已经在上百个企业级人工智能场景中得到落地。OpenMLDB 优先开源了特征数据治理能力，依托 SQL 的开发能力，为企业提供全栈功能的，低门槛特征数据计算和管理平台。

## 2. 企业级 FeatureOps 全栈解决方案

MLOps 为人工智能工程化落地提供全栈技术方案，作为其中的关键一环，FeatureOps 负责特征计算和供给，衔接 DataOps 和 ModelOps。一个完整的可工程化落地的 FeatureOps 需要覆盖特征工程的各个方面，包括特征生成、特征计算、特征上线、特征共享、特征服务、灾备和高可用等。OpenMLDB 提供一套全栈 FeatureOps 企业级解决方案，同时拥有低门槛和极简的使用和管理体验，让特征工程开发回归于本质：专注于高质量的特征抽取脚本开发，不再被工程化落地所羁绊。

<p align="center">
 <img src="images/workflow_cn.png" alt="image-20211103103052252" width=800 />
</p>


上图显示了基于 OpenMLDB 的 FeatureOps 的基本使用流程，从特征开发到上线，只需要三个步骤：

1. 线下流程：基于 SQL 的特征脚本开发
1. SQL 脚本一键部署上线，由线下模式切换为线上模式
3. 线上流程：接入实时数据流，进行实时特征供给上线服务

## 3. 主要特性

**线上线下一致性执行引擎：** 离线和实时特征计算使用统一的计算执行引擎，线上线下一致性得到了天然保证。

**低门槛且功能强大的数据库开发体验：** 低门槛的数据库开发体验，全流程基于 SQL 和 CLI 进行特征抽取脚本开发以及部署上线。

**面向特征计算的定制化性能优化：** 离线特征计算提供[基于 Spark 的高性能批处理优化版本](https://github.com/4paradigm/spark)；线上实时特征计算在高吞吐压力下的复杂查询提供几十毫秒量级的延迟，充分满足高并发、低延迟的性能需求。

**企业级特性：** 为大规模企业级应用而设计，整合诸多企业级特性，包括灾备恢复、高可用、可无缝扩缩容、可平滑升级、可监控、企业级异构内存架构支持等。

## 4. FAQ

1. **主要使用场景是什么？**

   目前主要面向人工智能场景，为机器训练模型和推理提供一站式特征供给解决方案，包含特征计算、特征存储、特征访问服务等功能。此外，OpenMLDB 本身也包含了一个高效且功能完备的时序数据库，使用于金融、IoT等领域。

2. **OpenMLDB 是如何发展起来的？**
   
   OpenMLDB 起源于领先的人工智能平台提供商[第四范式](https://www.4paradigm.com/)的商业化平台。我们将商业产品中作为数据供给的若干核心组件进行了抽象、增强、以及社区友好化，将它们形成了一个系统的开源产品，以帮助更多的企业低成本实现数字化转型。在 OpenMLDB 开源之前，已经作为第四范式的商业化组件之一在上百个场景中得到了部署和上线。
   
3. **OpenMLDB 是否就是一个 feature store？**
   
   OpenMLDB 包含 feature store 的全部功能但是提供更为完整的 FeatureOps 全栈方案。除了提供特征存储功能，还具有基于 SQL 的数据库开发体验、特征计算、特征上线、企业级运维等功能。
   
4. **OpenMLDB 为什么选择 SQL 作为开发语言并且提供数据库的开发体验？**
   
   SQL 具备表达语法简洁且功能强大的特点，选用 SQL 和数据库开发体验一方面降低开发门槛，另一方面更易于跨部门之间的协作和共享。此外，基于 OpenMLDB 的实践经验表明，SQL 在特征计算的表达上功能完备，已经经受了长时间的实践考验。
   
5. **如何取得技术支持**

   欢迎加入我们的社区，为你提供使用支持。

## 5. 编译和安装

:point_right: [点击这里](docs/cn/compile.md)

## 6. Demo & QuickStart

从 0.3.0 版本开始，OpenMLDB 引入了两种部署模式：集群模式和单机模式。集群模式适合于大规模数据的实际生产环境；单机模式适合于小数据场景或者试用目的，更加方便部署和使用。我们演示基于这两种模式的 demo 和快速上手指南：

- :point_right: [Demo 代码](demo)
- :point_right: [集群模式快速上手指南](docs/cn/cluster.md)
- :point_right: [单机模式快速上手指南](docs/cn/standalone.md)

## 7. 开发计划

OpenMLD 社区持续进行开发迭代，在此列出我们已经初步规划好的在未来版本的主要支持特性，如果想详细了解我们的计划，或者提供任何的建议，请加入我们的社区来参与互动。

| 版本号 | 预期发布日期 | 主要特性                                                     |
| ------ | ------------ | ------------------------------------------------------------ |
| 0.4.0  | End of 2021  | - 完全支持基于 CLI 的数据库开发体验（包括单机和集群版）      |
| 0.5.0  | 2022 Q1      | - 在线服务监控模块<br />- 长时间窗口支持 <br />- 支持第三方在线数据流引入，包括 Kafka 和 Pulsar |

此外，OpenMLDB roadmap 上有一些规划中的重要特性支持，欢迎给我们任何反馈：

- Cloud-native 版本
- 基于外存（如 SSD）进行优化的低成本版本
- 整合基于傲腾持久内存的快速恢复技术
- 开源整合自动特征生成功能

## 8. 社区

- **Email**: [contact@openmldb.ai](mailto:contact@openmldb.ai)
- **[Slack Workspace](https://join.slack.com/t/openmldb/shared_invite/zt-ozu3llie-K~hn9Ss1GZcFW2~K_L5sMg)**: 你可以在 Slack 上找到我们，通过在线聊天的方式，获取关于 OpenMLDB 的使用和开发支持。

- **GitHub Issues 和 Discussions**: 如果你是一个严肃的开发者，我们非常欢迎加入我们 GitHub 上的开发者社区，近距离参与我们的开发迭代。GitHub Issues 主要用来搜集 bugs 以及反馈新特性需求；GitHub Discussions 主要用来给开发团队发布并且讨论 RFCs。
- [**技术博客**](https://www.zhihu.com/column/c_1417199590352916480)
- **微信交流群：**
  <img src="images/wechat.png" alt="img" width=100 />  

## 9. 学术论文和技术博客

* Cheng Chen, Jun Yang, Mian Lu, Taize Wang, Zhao Zheng, Yuqiang Chen, Wenyuan Dai, Bingsheng He, Weng-Fai Wong, Guoan Wu, Yuping Zhao, and Andy Rudoff. *[Optimizing in-memory database engine for AI-powered on-line decision augmentation using persistent memory](http://vldb.org/pvldb/vol14/p799-chen.pdf)*. International Conference on Very Large Data Bases (VLDB) 2021.
* [第四范式OpenMLDB优化创新论文被国际数据库顶会VLDB录用](https://zhuanlan.zhihu.com/p/401513878)
* [OpenMLDB在银行上线事中交易反欺诈模型实践](https://zhuanlan.zhihu.com/p/389599785)
* [OpenMLDB在AIOPS领域关于交易系统异常检测应用实践](https://zhuanlan.zhihu.com/p/393602288)
* [5分钟完成硬件剩余寿命智能预测](https://zhuanlan.zhihu.com/p/399346826)

## 10. [用户列表](https://github.com/4paradigm/OpenMLDB/discussions/707)

我们创建了一个用于搜集用户使用反馈意见的[用户列表](https://github.com/4paradigm/OpenMLDB/discussions/707)。我们非常感激我们的社区用户可以留下基于 OpenMLDB 的使用案例、意见、或者任何反馈。我们非常期待听到你的声音！
