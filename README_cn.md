[English version](README.md)


![](images/fedb_black.png)

- [**Slack Channel**](https://hybridsql-ws.slack.com/archives/C01R7L7AL3W)
- [**Discussions**](https://github.com/4paradigm/fedb/discussions)

## 介绍

FEDB是一个面向在线推理和决策应用的NewSQL数据库。

- __高性能__

基于内存的存储引擎降低数据访问延迟，对SQL进行极致地编译优化提升执行效率，让开发高性能的实时推理和决策应用变的非常简单。

- __SQL兼容__

兼容大部分ANSI SQL语法，有python和java client。其中java client支持大部分JDBC接口。

- __在线离线一致性__

使用FEDB开发的机器学习应用可以一键上线，并且保证在线离线一致性，大大降低了机器学习场景的落地成本。

- __支持分布式，易扩展__

支持故障自动切换，支持横向扩展

注:目前还处于unstable状态并且有许多功能待补齐,不能运用于生产环境。

## 快速开始

### 构建

```
git clone --recurse-submodules https://github.com/4paradigm/fedb.git
cd fedb
docker run -v `pwd`:/fedb -it ghcr.io/4paradigm/centos6_gcc7_hybridsql:latest
cd /fedb
sh tools/install_hybridse.sh
mkdir -p build && cmake ../ && make -j5 fedb
```

### 典型应用场景

* [出租车线路耗时预测](https://github.com/4paradigm/DemoApps/tree/main/predict-taxi-trip-duration) 
* 在线交易系统健康检测和预警
* 在线交易反欺诈

## 架构图

![架构图](images/fedb_arch.png)  

## 未来规划

### ANSI SQL兼容

FEDB目前已经兼容主流DDL、DML语法，并逐步增强ANSI SQL语法的兼容性。

* [2021H1] 完善Window的标准语法，支持Where, Group By, Join等操作
* [2021H1&H2]针对AI场景扩展特有的语法特性和UDAF函数

### 功能/性能提升

为了满足实时推理与决策场景的高性能需求，FEDB选择内存作为存储引擎介质，而目前业界使用内存存储引擎都存在内存碎片和重启恢复效率问题，FEDB计划在内存分配算法进行优化降低碎片问题以及引入[PMEM](https://www.intel.com/content/www/us/en/architecture-and-technology/optane-dc-persistent-memory.html)(Intel Optane DC Persistent Memory Module)存储介质提升数据恢复效率，具体计划如下：

* [2021H1]支持新内存分配策略，降低内存碎片问题
* [2021H2]实验支持PMEM存储引擎

### 生态构建
FEDB有java/python client，java client支持jdbc接口的大部分功能。未来会对接到大数据生态，让Flink/Kafka/Spark与FEDB更方便集成。

* [2021H1&H2]支持Flink/Kafka/Spark connector


## 反馈和参与
* bug、疑惑、修改欢迎提在[Github Issues](https://github.com/4paradigm/fedb/issues/new)
* 想了解更多或者有想法可以参与到[slack](https://hybridsql-ws.slack.com/archives/C01R7L7AL3W)交流

## 许可证
Apache License 2.0
