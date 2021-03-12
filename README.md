# FEDB

## 介绍
FEDB是一个面向实时决策的NewSQL数据库。目前还处于unstalbe状态并且有许多功能待补齐

## 整体架构 
![架构图](images/rtidb_arch.png)
## 背景
FEDB有什么特点?  
* 高性能  
c++实现，底层基于内存的存储引擎，执行引擎利用LLVM高度优化。在AI场景下相比其他数据库有很大的优势
* SQL兼容/迁移成本低  
兼容大部分ANSI SQL语法，有python和java client。其中java client支持大部分JDBC接口
* 在线离线一致性  
结合[NativeSpark](https://github.com/4paradigm/NativeSpark), 使用FEDB开发的机器学习应用可以一键上线，并且保证在线离线一致性，大大降低了机器学习场景的落地成本
* 支持分布式
* 易扩展
## 快速开始
### 编译
1. 启动镜像 docker run -v \`pwd\`:/rtidb -it FEDB-docker bash
2. cd /rtidb && sh tools/install_fesql.sh
3. mkdir -p build && cmake ../ && make -j5 rtidb
### 示例
在机器学习场景中，比较典型的应用是在线推理和决策。下面是一个快速搭建在线推理应用的例子
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
    Starting training...
    [1] valid_0's l2: 1.17079e+07   valid_0's l1: 620.283
    Training until validation scores don't improve for 5 rounds
    [2] valid_0's l2: 1.17068e+07   valid_0's l1: 622.091
    [3] valid_0's l2: 1.17066e+07   valid_0's l1: 622.494
    [4] valid_0's l2: 1.17072e+07   valid_0's l1: 622.287
    [5] valid_0's l2: 1.17077e+07   valid_0's l1: 622.263
    [6] valid_0's l2: 1.17098e+07   valid_0's l1: 625.248
    Early stopping, best iteration is:
    [1] valid_0's l2: 1.17079e+07   valid_0's l1: 620.283
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
[点击这里](https://github.com/4paradigm/SparkSQLWithFeDB)查看更多内容
## 未来规划
### ANSI SQL兼容
FEDB目前已经兼容主流DDL、DML语法，并逐步增强ANSI SQL语法的兼容性
* [2021H1] 完善Window的标准语法，支持Where, Group By, Join等操作
* [2021H1&H2]针对AI场景扩展特有的语法特性和UDAF函数
### 功能/性能提升
为了提升FEDB的性能，内部使用了内存存储引擎，长时间运行会有内存碎片的问题，未来会解决这一问题。目前内存引擎有数据恢复慢的弊端，引入[PMEM](https://www.intel.com/content/www/us/en/architecture-and-technology/optane-dc-persistent-memory.html)(Intel Optane DC Persistent Memory Module)会大大降低数据恢复的时间
* [2021H1]解决内存碎片问题
* [2021H2]支持PMEM存储引擎
### 生态构建
FEDB有java/python client，java client支持jdbc接口的大部分功能。未来FEDB会对接到大数据生态，让flink/kafka/spark等组件产生的数据很便捷地导入到FEDB
* [2021H1&H2]开发flink/kafka/spark connector

## 贡献代码
请参考这里
## 反馈和参与
* bug、疑惑、修改建议都欢迎提在Github Issues中
* 想了解更多或者有想法[点击这里](https://hybridsql-ws.slack.com/archives/C01R7L7AL3W)
## 许可证
Apache License 2.0