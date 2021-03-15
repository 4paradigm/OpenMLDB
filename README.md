# FEDB

## 介绍

FEDB是一个面向实时推理和决策应用开发的NewSQL数据库

注:目前还处于unstable状态并且有许多功能待补齐,不能运用于生产环境

![架构图](images/rtidb_arch.png)
## FEDB有什么特点
* 高性能  
c++实现，底层基于内存的存储引擎，执行引擎利用LLVM高度优化。在实时推理和决策场景下相比其他数据库有很大的优势，参考[FEDB论文](https://vldb.org/pvldb/vol14/p799-chen.pdf)
* SQL兼容/迁移成本低  
兼容大部分ANSI SQL语法，有python和java client。其中java client支持大部分JDBC接口
* 在线离线一致性  
结合[NativeSpark](https://github.com/4paradigm/NativeSpark), 使用FEDB开发的机器学习应用可以一键上线，并且保证在线离线一致性，大大降低了机器学习场景的落地成本
* 支持分布式，易扩展
## 快速开始
### 编译
1. 启动镜像 docker run -v \`pwd\`:/rtidb -it FEDB-docker bash
2. cd /rtidb && sh tools/install_fesql.sh
3. mkdir -p build && cmake ../ && make -j5 rtidb
### 示例
一个快速搭建机器学习实时推理应用的例子，例子介绍[参考](https://github.com/4paradigm/SparkSQLWithFeDB)
1. 拉取并启动镜像
    ```
    docker pull fedb/fedb:2.1.0.1 && docker run -d fedb/fedb:2.1.0.1
    ```
2. 进入容器
    ```
    docker ps | grep fedb | awk '{print $1}' 
    55275653a728
    docker exec -it 55275653a728 /bin/bash
    ```
3. 运行demo
    ```
    git clone https://github.com/4paradigm/SparkSQLWithFeDB.git
    cd SparkSQLWithFeDB
    #下载llvm加速版本spark
    sh get_deps.sh
    #训练模型, 看到如下信息说明训练成功
    sh train.sh
    # 创建数据库和表并导入数据到数据库
    python3 import.py
    # 启动推理服务
    python3 predict_server.py >log 2>&1 &
    # 发送推理请求 ,会看到如下输出
    python3 predict.py
    ----------------ins---------------
    [[ 2.       40.774097 40.774097 40.774097 40.774097 40.774097 40.774097
    40.774097 40.774097  1.        1.      ]]
    ---------------predict trip_duration -------------
    864.4013066701439 s
    ```

## 未来规划
### ANSI SQL兼容
FEDB目前已经兼容主流DDL、DML语法，并逐步增强ANSI SQL语法的兼容性
* [2021H1] 完善Window的标准语法，支持Where, Group By, Join等操作
* [2021H1&H2]针对AI场景扩展特有的语法特性和UDAF函数
### 功能/性能提升
为了满足实时推理与决策场景的高性能需求，fedb选择内存作为存储引擎介质，而目前业界使用内存存储引擎都存在内存碎片和重启恢复效率问题，fedb计划在内存分配算法进行优化降低碎片问题以及引入[PMEM](https://www.intel.com/content/www/us/en/architecture-and-technology/optane-dc-persistent-memory.html)(Intel Optane DC Persistent Memory Module)存储介质提升数据恢复效率，具体计划如下
* [2021H1]支持新内存分配策略，降低内存碎片问题
* [2021H2]实验支持PMEM存储引擎
### 生态构建
FEDB有java/python client，java client支持jdbc接口的大部分功能。未来会对接到大数据生态，让flink/kafka/spark与FEDB更方便集成
* [2021H1&H2]支持flink/kafka/spark connector

## 贡献代码
请参考这里
## 反馈和参与
* bug、疑惑、修改欢迎提在[Github Issues](https://github.com/4paradigm/fedb/issues/new)
* 想了解更多或者有想法可以参与到[slack](https://hybridsql-ws.slack.com/archives/C01R7L7AL3W)交流，也可以通过[邮件](mailto:g_fedb_dev@4paradigm.com)

## 许可证
Apache License 2.0