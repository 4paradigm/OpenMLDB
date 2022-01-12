
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


## 1. Introduction

OpenMLDB is an open-source database particularly designed to efficiently provide consistent data for machine learning.  A database for machine learning consists of two major tasks: feature extraction and feature access, which are served as data provisioning for offline training and online inference. Without OpenMLDB, there are two separate systems for online and offline data provisioning, which cost significant effort to verify the online-offline consistency. On the contrary, OpenMLDB supports the unified SQL programming and its execution engine for both online and offline data provisioning. As a result, the online-offline consistency is inherently guaranteed. Moreover, the system is carefully designed and optimized to ensure the efficiency. By taking advantages of OpenMLDB, database engineers are now able to write SQL scripts only to efficiently provide consistent data to machine learning, and an offline model can be immediately deployed for online serving with little cost involved.
<p align="center">
 <img src="images/workflow.png" alt="image-20211103103052252" width=800 />
 </p>

The above figure illustrates the OpenMLDB workflow. SQL engineers first write SQL scripts for offline feature extraction, which provides data for offline model training. When the model quality is satisfied, the online feature extraction and access can be enabled immediately for online serving without additional efforts involved. Thanks to the unified SQL programming and execution engine, the online-offline consistency verification is eliminated, which is inherently guaranteed by OpenMLDB. Furthermore, certain optimization techniques (e.g., data skew optimization and in-memory indexing for offline and online feature extraction, respectively) are adopted to ensure that the performance requirement can be met for both offline training and online inference. In summary, OpenMLDB enables SQL as the only programming interface for consistent and efficient data provisioning for both offline model training and online inference serving.

## 2. Highlight Features
### 2.1. SQL Programming APIs 

We believe SQL is the most suitable programming APIs for feature engineering because of its elegant design and popularity. OpenMLDB enables SQL as the programming APIs for developers for both offline and online feature extraction. Besides, we extend the capability of standard SQL and make it more powerful for feature extraction. 

### 2.2 Online-Offline Consistency

Based on the SQL programming APIs, we design an unified execution engine for both online and offline feature extraction. As a result, the online-offline consistency is inherently guaranteed by OpenMLDB with no other cost.

### 2.3. Efficiency

We propose a few techniques to improve the performance for both offline and online feature extraction. As a result, our offline feature extraction can be significantly faster than existing opensource bigdata processing frameworks. Moreover, our online service can provide low latency (tens of milliseconds) to meet the performance requirement of online inference.

You can read our below  section (7. Publications & Blogs) for more technical detail.

### 2.4 Integrated CLI

We provide a powerful integrated CLI for SQL programming, job management, online and offline deployment, and database administration. Developers who are familiar with database's CLIs should be very comfortable with our tool.

*Note that, the CLI of current release 0.3.0 supports the cluster mode partially. It will be fully supported in the next release of 0.4.0* 

## 3. Build & Install

:point_right: [Read more](docs/en/compile.md)

## 4. Demo & QuickStart

Since OpenMLDB v0.3.0, we have introduced two operating modes, which are cluster mode and standalone mode. The cluster mode is suitable for large-scale datasets and real-world applications, which provides the scalability and high-availability. On the other hand, the lightweight standalone mode running on a single node is ideal for small businesses and demonstration. 

We demonstrate the workflow using the cluster and standalone modes:

- :point_right: [Demo code](demo)
- :point_right: [QuickStart for the cluster mode](docs/en/cluster.md)
- :point_right: [QuickStart for the standalone mode](docs/en/standalone.md)

## 5. Roadmap

We list a few highlight features that we have planned in the future releases. Please join our community to understand more about our planning and discuss your ideas.

| Version | Est. release date | Highlight features                                           |
| ------- | ----------------- | ------------------------------------------------------------ |
| 0.4.0   | End of 2021       | - Full support of standalone and cluster modes in the integrated CLI |
| 0.5.0   | 2022 Q1           | - Monitoring APIs and tools for online serving <br />- Efficient queries over a fairly long period of time by window functions <br />- Kafka/Pulsar connector support for online data source |

## 6. Community

You may join our community for feedback and discussion

- **Email**: [contact@openmldb.ai](mailto:contact@openmldb.ai)

- **[Slack Workspace](https://join.slack.com/t/openmldb/shared_invite/zt-ozu3llie-K~hn9Ss1GZcFW2~K_L5sMg)**: You may find useful information of release notes, user support, development discussion and even more from our various Slack channels. 

- **GitHub Issues and Discussions**: If you are a serious developer, you are most welcome to join our discussion on GitHub. **GitHub Issues** are used to report bugs and collect new requirements. **GitHub Discussions** are mostly used by our project maintainers to publish and comment RFCs.

- [**Blogs (Chinese)**](https://www.zhihu.com/column/c_1417199590352916480)

- **WeChat Groups (Chinese)**:

  <img src="images/wechat.png" alt="img" width=100 />  

## 7. Publications & Blogs

- Cheng Chen, Jun Yang, Mian Lu, Taize Wang, Zhao Zheng, Yuqiang Chen, Wenyuan Dai, Bingsheng He, Weng-Fai Wong, Guoan Wu, Yuping Zhao, and Andy Rudoff. *[Optimizing in-memory database engine for AI-powered on-line decision augmentation using persistent memory](http://vldb.org/pvldb/vol14/p799-chen.pdf)*. International Conference on Very Large Data Bases (VLDB) 2021.
- [In-Depth Interpretation of the Latest VLDB 2021 Paper: Artificial Intelligence Driven Real-Time Decision System Database and Optimization Based on Persistent Memory](https://medium.com/@fengxindai0/in-depth-interpretation-of-the-latest-vldb-2021-paper-artificial-intelligence-driven-real-time-f2a818bcf2b2)
- [Predictive maintenance — 5 minutes demo of an end to end machine learning project](https://towardsdatascience.com/predictive-maintenance-5minutes-demo-of-an-end-to-end-machine-learning-project-60941f1c9793)
- [Compared to Native Spark 3.0, We Have Achieved Significant Optimization Effects in the AI Application Field](https://towardsdatascience.com/we-have-achieved-significant-optimization-effects-in-the-ai-application-field-compared-to-native-2a055e47250f)
- [MLOp Practice: Using OpenMLDB in the Real-Time Anti-Fraud Model for the Bank’s Online Transaction](https://towardsdatascience.com/practice-of-openmldbs-transaction-real-time-anti-fraud-model-in-the-bank-s-online-event-40ab41fec6d4)

## 8. [User List](https://github.com/4paradigm/OpenMLDB/discussions/707)
We have built [a user list](https://github.com/4paradigm/OpenMLDB/discussions/707) to collect feedback from the community. We really appreciate it if you can provide your use cases, comments, or any feedback when using OpenMLDB. We want to hear from you! 
