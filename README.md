
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

### OpenMLDB is an open-source machine learning database that provides a full-stack FeatureOps solution for enterprises.

## 1. Our Philosophy

In the process of artificial intelligence engineering (AI), 95% of the time and effort of an enterprise's data and engineering teams will be consumed by data processing, data verification and other related work. In order to solve this problem, those 1% tech giants will spend thousands of hours on building an in-house data and feature platform to address engineering challenges such as online and offline consistency, data correctness, high throughput, low latency and high availability. The other 99% enterprises purchase expensive SaaS tools and data governance services. 

OpenMLDB is committed to solving the data governance challenge of AI engineering in a closed loop, and has been implemented in hundreds of real-world applications for enterprise scenarios. OpenMLDB gives priority to open-source feature engineering capability, and provides enterprises with a full-stack feature engineering platform (aka FeatureOps) that supports SQL programming APIs with great ease of use.

## 2. A Full-Stack FeatureOps Solution for Enterprises

MLOps provides a set of practices to develop, deploy, and maintain machine learning models in production efficiently and reliably. As a key link, FeatureOps is responsible for feature extraction and serving, bridging DataOps and ModelOps. A closed-loop FeatureOps solution needs to cover all aspects of Feature Engineering, from functionalities (such as feature generation, feature extraction, feature serving, feature sharing, and so on) and production requirements (such as low latency, high throughput, fault recovery, high availability, and so on). OpenMLDB provides a full-stack FeatureOps solution for enterprises, with great ease of use for development and management, so that feature engineering development returns to its essence: focusing on high-quality feature extraction script development and is no longer bound by engineering issues.

<p align="center">
 <img src="images/workflow.png" alt="image-20211103103052253" width=800 />
</p>
The figure above shows the workflow of FeatureOps based on OpenMLDB. From feature offline development to online serving, it only needs three steps:

1. Offline feature extraction development based SQL
2. SQL script deployment with one click, switching the system from the offline to online mode
3. Real-time features extraction and serving by connecting with online data streams

## 3. Highlight Features

**The Unified Online-Offline Execution Engine:** Offline and real-time online feature extraction use a unified execution engine, thus online and offline consistency is inherently guaranteed.

**SQL Based Development and Management Experience**: Feature extraction script development, deployment, and maintenance are all based on SQL and CLI with great ease of use.

**High Performance Feature Extraction**: Offline feature extraction is performed based on [a tailored Spark version](https://github.com/4paradigm/spark) that is particularly optimized for batch-based feature extraction. Online feature extraction provides tens of milliseconds latency under high throughput pressure, which fully meets the performance requirements of low latency and high throughput.

**Enterprise Features**: Designed for large-scale enterprise applications, OpenMLDB integrates important enterprises features, including fault recovery, high availability, seamless scale-out, smooth upgrade, monitoring, heterogeneous memory support, and so on.

## 4. FAQ

1. **What are the use cases of OpenMLDB?**
   
   At present, it mainly provides a full-stack FeatureOps solutions for machine learning applications. Its major functionality consists of feature extraction, feature storage, feature serving, feature sharing, and so on. In addition, OpenMLDB is built based on an efficient and fully functional time-series database, which is used in finance, IoT and other fields.
   
2. **How does OpenMLDB evolve?**
   
   OpenMLDB originated from the commercial product of [4Paradigm](https://www.4paradigm.com/) (a leading artificial intelligence service provider). The core team has abstracted, enhanced and developed community-friendly features based on the commercial product to make it publicly available as an open-source project, in order to benefit more enterprises to achieve successful digital transformations at low cost. Before OpenMLDB was open-source, it had been successfully deployed in hundreds of real-world applications as one of the components in 4Paradigm's commercial products.
   
3. **Is OpenMLDB a feature store?**
   
   OpenMLDB contains all the functions of a feature store, but provides a more complete full-stack FeatureOps solution, which includes feature store, SQL based development experience, a tailored Spark distribution for offline feature extraction, highly optimized indexing for real-time feature extraction, feature serving, and industry features (such as monitoring and high-availability). Furthermore, OpenMLDB is also used as a high performance time-series database besides FeatureOps. 
   
4. **Why does OpenMLDB choose SQL as the programming language for users?**
   
   SQL has the elegant syntax but yet powerful expression ability. SQL based programming experience reduces the learning curve of using OpenMLDB, and further makes it easier for cross department cooperation and sharing. In addition, based on the experience of developing and deploying hundreds of real-world applications using OpenMLDB, it shows that SQL has complete functions in the expression of feature extraction and has withstood the test of practice for a long time.

## 5. Build & Install

:point_right: [Read more](docs/en/compile.md)

## 6. QuickStart

**Cluster and Standalone Deployments**

Since OpenMLDB v0.3.0, we have introduced two deployment modes, which are *cluster deployment* and *standalone deployment*. The cluster deployment is suitable for large-scale applications, which provides the scalability and high-availability. On the other hand, the lightweight standalone deployment running on a single node is ideal for small businesses and demonstration.

The cluster and standalone deployment have the same functionalities but with different limitations for particular functions. Please refer to ==[this document TBD](das)==  for details. 

**Getting Started with OpenMLDB**

- :point_right: [QuickStart for the cluster deployment](docs/en/cluster.md)
- :point_right: [QuickStart for the standalone deployment](docs/en/standalone.md)

## 7. Use Cases

We are making efforts to build a list of real-world use cases based on OpenMLDB to demonstrate how it can fit into your applications. Please stay tuned.

| Application                              | Tools Used         | Brief Introduction                                           |
| ---------------------------------------- | ------------------ | ------------------------------------------------------------ |
| [New York City Taxi Trip Duration](demo) | OpenMLDB, LightGBM | This is a challenge from Kaggle to predict the total ride duration of taxi trips in New York City. You can read [more detail here](https://www.kaggle.com/c/nyc-taxi-trip-duration/). |

## 8. Documentation

You can find our detailed documentation here: [docs.openmldb.ai](https://docs.openmldb.ai/) 

## 9. Roadmap

| Version | Est. release date | Highlight features                                           |
| ------- | ----------------- | ------------------------------------------------------------ |
| 0.5.0   | 2022 Q1           | - Monitoring APIs and tools for online serving <br />- Efficient queries over a fairly long period of time by window functions <br />- Kafka/Pulsar connector support for online data source <br />- Supporting external storage devices for online feature extraction engine |

## 10. Contributors

We really appreciate the effort from our community! Please read our [Contribution Guideline](CONTRIBUTING.md) for more detail. If you are a new contributor, you may be interested in the [list of good-first-issue](https://github.com/4paradigm/OpenMLDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22).

<a href="https://github.com/4paradigm/openmldb/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=4paradigm/openmldb" />
</a>

## 11. Community

You may join our community for feedback and discussion

- **Website**: [https://openmldb.ai/](https://openmldb.ai)

- **Email**: [contact@openmldb.ai](mailto:contact@openmldb.ai)

- **[Slack Workspace](https://join.slack.com/t/openmldb/shared_invite/zt-ozu3llie-K~hn9Ss1GZcFW2~K_L5sMg)**: You may find useful information of release notes, user support, development discussion and even more from our various Slack channels. 

- **GitHub Issues and Discussions**: If you are a serious developer, you are most welcome to join our discussion on GitHub. **GitHub Issues** are used to report bugs and collect new requirements. **GitHub Discussions** are mostly used by our project maintainers to publish and comment RFCs.

- [**Blogs (Chinese)**](https://www.zhihu.com/column/c_1417199590352916480)

- **WeChat Groups (Chinese)**:

  <img src="images/wechat.png" alt="img" width=120 />  

## 12. Publications & Blogs

- Cheng Chen, Jun Yang, Mian Lu, Taize Wang, Zhao Zheng, Yuqiang Chen, Wenyuan Dai, Bingsheng He, Weng-Fai Wong, Guoan Wu, Yuping Zhao, and Andy Rudoff. *[Optimizing in-memory database engine for AI-powered on-line decision augmentation using persistent memory](http://vldb.org/pvldb/vol14/p799-chen.pdf)*. International Conference on Very Large Data Bases (VLDB) 2021.
- [In-Depth Interpretation of the Latest VLDB 2021 Paper: Artificial Intelligence Driven Real-Time Decision System Database and Optimization Based on Persistent Memory](https://medium.com/@fengxindai0/in-depth-interpretation-of-the-latest-vldb-2021-paper-artificial-intelligence-driven-real-time-f2a818bcf2b2)
- [Predictive maintenance — 5 minutes demo of an end to end machine learning project](https://towardsdatascience.com/predictive-maintenance-5minutes-demo-of-an-end-to-end-machine-learning-project-60941f1c9793)
- [Compared to Native Spark 3.0, We Have Achieved Significant Optimization Effects in the AI Application Field](https://towardsdatascience.com/we-have-achieved-significant-optimization-effects-in-the-ai-application-field-compared-to-native-2a055e47250f)
- [MLOp Practice: Using OpenMLDB in the Real-Time Anti-Fraud Model for the Bank’s Online Transaction](https://towardsdatascience.com/practice-of-openmldbs-transaction-real-time-anti-fraud-model-in-the-bank-s-online-event-40ab41fec6d4)

## 13. [User List](https://github.com/4paradigm/OpenMLDB/discussions/707)
We have built [a user list](https://github.com/4paradigm/OpenMLDB/discussions/707) to collect feedback from the community. We really appreciate it if you can provide your use cases, comments, or any feedback when using OpenMLDB. We want to hear from you! 
