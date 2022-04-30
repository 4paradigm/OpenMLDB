
<div align=center><img src="./images/openmldb_logo.png" width="400" /></div>

[![build status](https://github.com/4paradigm/openmldb/actions/workflows/cicd.yaml/badge.svg?branch=openmldb)](https://github.com/4paradigm/openmldb/actions/workflows/cicd.yaml)
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

**English version | [中文版](README_cn.md)**

### OpenMLDB is an open-source machine learning database that provides a production-ready real-time feature engineering system with online-offline consistency.

## 1. Our Philosophy

For the artificial intelligence (AI) engineering, 95% of the time and effort is consumed by data related workloads. In order to tackle this problem, tech giants spend thousands of hours on building in-house data platforms to address AI engineering challenges such as online-offline consistency, feature backfilling, and performance. The other small and medium-sized enterprises have to purchase expensive SaaS tools and data governance services. 

OpenMLDB is an open-source machine learning database that is committed to solving the data governance challenge. OpenMLDB has been deployed in hundreds of real-world enterprise applications. It gives priority to open-source the capability of feature engineering using SQL, which offers a production-ready feature engineering platform with online-offline consistency.

## 2. A Production-Ready Real-Time Feature Engineering System

Efficient real-time feature engineering is essential for many machine learning applications, such as real-time personalized recommendation and risk analytics. However, a feature engineering script developed by data scientists (Python scripts in most cases) cannot be directly deployed into production for online inference because it usually cannot meet the engineering requirements, such as low latency, high throughput and high availability. Therefore, a engineering team needs to be involved to refactor and optimize the source code using database or C++ to ensure its efficiency and robustness. As there are two teams and two toolchains involved for the entire life cycle, the verification for online-offline processing consistency is essential, which usually costs a lot of time and human power. 

OpenMLDB is particularly designed to achieve the goal of **Development as Deployment** for feature engineering, to significantly reduce the cost from the offline development to online production deployment. There are three steps only for the entire life cycle of feature engineering:

- Step 1: Offline development of feature engineering script based on SQL
- Step 2: SQL online deployment using just one command
- Step 3: Online data source configuration for real-time data

With those three steps done, the system is ready to serve real-time feature engineering requests with highly optimized low latency and high throughput for production.

<p align="center">
 <img src="docs/en/about/images/workflow.png" alt="image-20211103103052253" width=800 />
</p>

The figure above shows the OpenMLDB's architecture designed to achieve the goal of Development as Deployment. There are four key components:

- SQL language as the only programming language for both offline and online
- Online real-time SQL engine optimized for low latency and high throughput
- Offline batch SQL engine optimized for big data and efficient batch data processing
- The unified execution plan generator to bridge the online and offline SQL engines to guarantee the online-offline processing consistency

## 3. Highlights

**Online-Offline Processing Consistency:** Based on the unified execution plan generator, the online-offline feature processing consistency is inherently guaranteed.

**Customized Optimization for Feature Engineering**: The online real-time SQL engine is built from scratch and particularly optimized for time series data. It can achieve the response time of a few milliseconds, which significantly outperforms other commercial in-memory database systems (see Figure 9 & 10 of [the VLDB 2021 paper](http://vldb.org/pvldb/vol14/p799-chen.pdf)). Offline feature extraction is performed based on [a tailored Spark version](https://github.com/4paradigm/spark) that is particularly optimized for batch-based feature processing.

**SQL-Centric Development and Management**: Feature engineering script development, deployment, and maintenance are all based on SQL with great ease of use.

**Production-Ready**: OpenMLDB has been implementing important production features for enterprise-grade applications, including fault recovery, high availability, seamless scale-out, smooth upgrade, monitoring, heterogeneous memory support, and so on.

## 4. FAQ

1. **What are use cases of OpenMLDB?**
   
   At present, it is mainly positioned as a real-time feature processing system for machine learning applications. It provides the capability of Development as Deployment to significantly reduce the cost for machine learning applications. On the other hand, OpenMLDB contains an efficient and fully functional time-series database, which is used in finance, IoT and other fields.
   
2. **How does OpenMLDB evolve?**
   
   OpenMLDB originated from the commercial product of [4Paradigm](https://www.4paradigm.com/) (a leading artificial intelligence service provider). In 2021, the core team has abstracted, enhanced and developed community-friendly features based on the commercial product; and then makes it publicly available as an open-source project to benefit more enterprises to achieve successful digital transformations at low cost. Before OpenMLDB was open-source, it had been successfully deployed in hundreds of real-world applications together with 4Paradigm's other commercial products.
   
3. **Is OpenMLDB a feature store?**
   
   OpenMLDB is considered as a superset of feature store, which is defined as offline and online features providers in general. Furthermore, OpenMLDB is able to provide the capability of efficient real-time feature processing with the online-offline processing consistency. Nowadays, most feature stores in the market serve online features by syncing features pre-computed at offline. But they are incapable of real-time online feature processing in a few milliseconds. By comparison, OpenMLDB is taking advantage of its online SQL engine, to efficiently support real-time feature processing.
   
4. **Why does OpenMLDB choose SQL as the programming language for users?**
   
   SQL has the elegant syntax but yet powerful expression ability. SQL based programming experience flattens the learning curve of using OpenMLDB, and further makes it easier for collaboration and sharing. In addition, based on the experience of developing and deploying hundreds of real-world applications using OpenMLDB, it shows that SQL has complete functions in the expression of feature extraction and has withstood the test of practice for a long time.

## 5. Build & Install

:point_right: [Read more](https://openmldb.ai/docs/en/main/deploy/index.html)

## 6. QuickStart

**Cluster and Standalone Versions**

OpenMLDB has two versions with different deployment options, which are *cluster version* and *standalone version*. The cluster version is suitable for large-scale applications and ready for production. On the other hand, the lightweight standalone version running on a single node is ideal for evaluation and demonstration. The cluster and standalone versions have the same functionalities but with different limitations for particular functions. Please refer to [this document](https://openmldb.ai/docs/en/main/tutorial/standalone_vs_cluster.html)  for details. 

**Getting Started with OpenMLDB**

:point_right: [OpenMLDB QuickStart](https://openmldb.ai/docs/en/main/quickstart/openmldb_quickstart.html)

## 7. Use Cases

We are building a list of real-world use cases based on OpenMLDB to demonstrate how it can fit into your business. 

| Application                                                  | Tools                                       | Brief Introduction                                           |
| ------------------------------------------------------------ | ------------------------------------------- | ------------------------------------------------------------ |
| [New York City Taxi Trip Duration](https://openmldb.ai/docs/zh/main/use_case/taxi_tour_duration_prediction.html) | OpenMLDB, LightGBM                          | This is a challenge from Kaggle to predict the total ride duration of taxi trips in New York City. You can read [more detail here](https://www.kaggle.com/c/nyc-taxi-trip-duration/). It demonstrates using the open-source tools OpenMLDB + LightGBM to build an end-to-end machine learning applications easily. |
| [Using the Pulsar connector to import real-time data streams](https://openmldb.ai/docs/en/main/use_case/pulsar_openmldb_connector_demo.html) | OpenMLDB, Pulsar, Pulsar OpenMLDB Connector | Apache Pulsar is a cloud-native streaming platform. Based on the [Pulsar OpenMLDB connector](https://pulsar.apache.org/docs/en/next/io-connectors/#jdbc-openmldb) , we are able to seamlessly import real-time data streams from Pulsar to OpenMLDB as the online data sources. |

## 8. Documentation

- Chinese documentations: [https://openmldb.ai/docs/zh](https://openmldb.ai/docs/zh)
- English documentations: [https://openmldb.ai/docs/en/](https://openmldb.ai/docs/en/)

## 9. Roadmap

Please refer to our [public Roadmap page](https://github.com/4paradigm/OpenMLDB/projects/10).

Furthermore, there are a few important features on the development roadmap but have not been scheduled yet. We appreciate any feedbacks on those features.

- A cloud-native OpenMLDB
- Automatic feature extraction
- Optimization based on heterogeneous storage and computing resources
- A lightweight OpenMLDB for edge computing

## 10. Contributors

We really appreciate the contribution from our community.

- If you are interested to contribute, please read our [Contribution Guideline](CONTRIBUTING.md) for more details. 
- If you are a new contributor, you may get start with [the list of issues labeled with `good first issue`](https://github.com/4paradigm/OpenMLDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22).
- If you have experience of OpenMLDB development, or want to tackle a challenge that may take 1-2 weeks, you may find [the list of issues labeled with `call-for-contributions`](https://github.com/4paradigm/OpenMLDB/issues?q=is%3Aopen+is%3Aissue+label%3Acall-for-contributions).

Let's clap hands for our community contributors :clap:

<a href="https://github.com/4paradigm/openmldb/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=4paradigm/openmldb" width=600/>
</a>

## 11. Community

- **Website**: [https://openmldb.ai/en](https://openmldb.ai/en)

- **Email**: [contact@openmldb.ai](mailto:contact@openmldb.ai)

- **[Slack](https://join.slack.com/t/openmldb/shared_invite/zt-ozu3llie-K~hn9Ss1GZcFW2~K_L5sMg)** 

- **[GitHub Issues](https://github.com/4paradigm/OpenMLDB/issues)** and **[GitHub Discussions](https://github.com/4paradigm/OpenMLDB/discussions)**: The GitHub Issues is used to report bugs and collect new feature requirements. The GitHub Discussions is open to any discussions related to OpenMLDB.

- **[Blogs (English)](https://openmldb.medium.com/)**

- [**Blogs (Chinese)**](https://www.zhihu.com/column/c_1417199590352916480)

- Public drives maintained by the PMC: **[English](https://drive.google.com/drive/folders/1T5myyLVe--I9b77Vg0Y8VCYH29DRujUL) |  [中文](https://go005qabor.feishu.cn/drive/folder/fldcn3W5i52QmWqgJzRlHvxFf2d)**

- [**Mailing list for developers**](https://groups.google.com/g/openmldb-developers)

- **WeChat Groups (Chinese)**:

  <img src="images/wechat.png" alt="img" width=120 />  

## 12. Publications

- Cheng Chen, Jun Yang, Mian Lu, Taize Wang, Zhao Zheng, Yuqiang Chen, Wenyuan Dai, Bingsheng He, Weng-Fai Wong, Guoan Wu, Yuping Zhao, and Andy Rudoff. *[Optimizing in-memory database engine for AI-powered on-line decision augmentation using persistent memory](http://vldb.org/pvldb/vol14/p799-chen.pdf)*. International Conference on Very Large Data Bases (VLDB) 2021.

## 13. [The User List](https://github.com/4paradigm/OpenMLDB/discussions/707)

We are building [a user list](https://github.com/4paradigm/OpenMLDB/discussions/707) to collect feedback from the community. We really appreciate it if you can provide your use cases, comments, or any feedback when using OpenMLDB. We want to hear from you! 
