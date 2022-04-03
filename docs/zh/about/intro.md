![](images/openmldb_logo.png)

# OpenMLDB 简介

### OpenMLDB 是一个开源机器学习数据库，提供企业级 FeatureOps 全栈解决方案。

## 1. 设计理念

在人工智能工程化落地过程中，企业的数据和工程化团队 95% 的时间精力会被数据处理、数据校验等相关工作所消耗。为了解决该痛点，1% 的头部企业会花费上千小时自研构建数据与特征平台，来解决诸如线上线下一致性、数据穿越、高并发低延迟、高可用等工程挑战；其他 99% 的企业则采购高昂的 SaaS 工具和数据治理服务。 

OpenMLDB 致力于闭环解决 AI 工程化落地的数据治理难题，并且已经在上百个企业级人工智能场景中得到落地。OpenMLDB 优先开源了特征数据治理能力，依托 SQL 的开发能力，为企业提供全栈功能的，低门槛特征数据计算和管理平台。

## 2. 企业级 FeatureOps 全栈解决方案

MLOps 为人工智能工程化落地提供全栈技术方案，作为其中的关键一环，FeatureOps 负责特征计算和供给，衔接 DataOps 和 ModelOps。一个完整的可高效工程化落地的 FeatureOps 解决方案需要覆盖特征工程的各个方面，包括功能需求（如特征存储、特征计算、特征上线、特征共享、特征服务等）和产品级需求（如低延迟、高并发、灾备、高可用、扩缩容、平滑升级、可监控等）。OpenMLDB 提供一套企业级全栈 FeatureOps 解决方案，以及低门槛的基于 SQL 的开发和管理体验，让特征工程开发回归于本质：专注于高质量的特征计算脚本开发，不再被工程化效率落地所羁绊。

![](images/workflow_cn.png)

上图显示了基于 OpenMLDB 的 FeatureOps 的基本使用流程，从特征开发到上线，只需要三个步骤：

1. 使用 SQL 进行线下特征计算脚本开发
2. SQL 特征计算脚本一键部署上线，由线下模式切换为线上模式
3. 接入实时数据流，进行线上实时特征计算和供给服务

## 3. 核心特性

**线上线下一致性执行引擎：** 离线和实时特征计算使用统一的计算执行引擎，线上线下一致性得到了天然保证。

**基于 SQL 的数据库开发体验：** 低门槛且功能强大的数据库开发体验，全流程基于 SQL 进行特征计算脚本开发以及部署上线。

**面向特征计算的定制化性能优化：** 离线特征计算使用[面向特征计算优化的 OpenMLDB Spark 发行版](../tutorial/openmldbspark_distribution.md)；线上实时特征计算在高吞吐压力下的复杂查询提供几十毫秒量级的延迟，充分满足高并发、低延迟的性能需求。

**企业级特性：** 为大规模企业级应用而设计，整合诸多企业级特性，包括灾备恢复、高可用、可无缝扩缩容、可平滑升级、可监控、企业级异构内存架构支持等。

## 4. FAQ

1. **主要使用场景是什么？**

   目前主要面向人工智能场景，为机器训练模型和推理提供一站式特征供给解决方案，包含特征计算、特征存储、特征访问等功能。此外，OpenMLDB 本身也包含了一个高效且功能完备的时序数据库，使用于金融、IoT、数据标注等领域。

2. **OpenMLDB 是如何发展起来的？**

   OpenMLDB 起源于领先的人工智能平台提供商[第四范式](https://www.4paradigm.com/)的商业化软件。其核心开发团队在 2021 年将商业产品中作为特征工程的核心组件进行了抽象、增强、以及社区友好化，将它们形成了一个系统的开源产品，以帮助更多的企业低成本实现人工智能转型。在开源之前，OpenMLDB 已经作为第四范式的商业化组件之一在上百个场景中得到了部署和上线。

3. **OpenMLDB 是否就是一个 feature store？**

   OpenMLDB 包含 feature store 的全部功能，并且提供更为完整的 FeatureOps 全栈方案。除了提供特征存储功能，还具有基于 SQL 的数据库开发体验、[面向特征计算优化的 OpenMLDB Spark 发行版](../tutorial/openmldbspark_distribution.md)，针对实时特征计算优化的索引结构，特征上线服务、企业级运维和管理等功能。此外，OpenMLDB 也被用作一个高性能的时序特征数据库。

4. **OpenMLDB 为什么选择 SQL 作为开发语言？**

   SQL 具备表达语法简洁且功能强大的特点，选用 SQL 和数据库开发体验一方面降低开发门槛，另一方面更易于跨部门之间的协作和共享。此外，基于 OpenMLDB 的实践经验表明，SQL 在特征计算的表达上功能完备，已经经受了长时间的实践考验。

## 5. 社区

- **GitHub:** https://github.com/4paradigm/OpenMLDB
- **网站：**[https://openmldb.ai/](https://openmldb.ai) (即将上线)
- **Email**: [contact@openmldb.ai](mailto:contact@openmldb.ai)
- **[Slack](https://join.slack.com/t/openmldb/shared_invite/zt-ozu3llie-K~hn9Ss1GZcFW2~K_L5sMg)**
- **[GitHub Issues](https://github.com/4paradigm/OpenMLDB/issues) 和 [GitHub Discussions](https://github.com/4paradigm/OpenMLDB/discussions)**: 如果你是一个严肃的开发者，我们非常欢迎加入我们 GitHub 上的开发者社区，近距离参与我们的开发迭代。GitHub Issues 主要用来搜集 bugs 以及反馈新特性需求；GitHub Discussions 主要用来给开发团队发布并且讨论 RFCs。
- [**技术博客**](https://www.zhihu.com/column/c_1417199590352916480)
- **微信交流群：**

![](images/wechat.png)


## 6. 阅读更多

- 了解 OpenMLDB 的发展历程 - [OpenMLDB 发展历程](milestones.md)
- 快速开始使用 OpenMLDB - [快速上手](../quickstart/content.md)
- 了解 OpenMLDB 的实际使用案例 - [使用案例](../use_case/content.md)
