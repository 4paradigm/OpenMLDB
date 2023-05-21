
# 代码结构

## Hybridse SQL 引擎
```
hybridse/
├── examples          // demo db和hybrisde集成测试
├── include           // 代码的include目录，里边结构和src基本一致
├── src
│   ├── base          // 基础库目录
│   ├── benchmark     // benchmark相关
│   ├── case          // 测试case相关
│   ├── cmd           // 封装的demo等
│   ├── codec         // 编解码相关
│   ├── codegen       // llvm代码生成相关
│   ├── llvm_ext      // llvm符号解析相关
│   ├── node          // 逻辑计划、物理计划中的节点定义, 表达式、类型节点定义
│   ├── passes        // sql优化器
│   ├── plan          // 生成逻辑计划
│   ├── planv2        // zetasql语法树转化成节点
│   ├── proto         // protobuf定义
│   ├── sdk           // sdk相关
│   ├── testing       // 测试相关
│   ├── udf           // udf和udaf的注册生成等
│   └── vm            // sql物理计划和执行计划的生成以及sql编译和执行人口
└── tools     // benchmark相关
```

## 在线存储引擎和对外服务接口
```
src/
├── apiserver      // apiserver相关
├── base           // 基础库目录
├── catalog        // catalog相关
├── client         // ns/tablet/taskmanager client的接口定义和实现
├── cmd            // CLI以及openmldb二进制生成相关
├── codec          // 编解码相关
├── datacollector  // 在离线同步工具
├── log            // binlog和snapshot格式以及读写
├── nameserver     // nameserver相关
├── proto          // protobuf相关定义
├── replica        // 主从同步
├── rpc            // 封装brpc请求
├── schema         // shema和索引解析生成
├── sdk            // sdk相关
├── storage        // 存储引擎
├── tablet         // tablet中接口的实现
├── test           // 测试相关
├── tools          // 封装一些小工具
└── zk             // zookeeper client的一些封装
```

## Java 模块
```
java/
├── hybridse-native          // sql引擎swig自动生成的代码
├── hybridse-proto           // sql引擎相关proto
├── hybridse-sdk             // sql引擎封装的sdk
├── openmldb-batch           // 离线的planner，把sql的逻辑翻译成spark的计划
├── openmldb-batchjob        // 执行离线任务相关
├── openmldb-common          // java sdk中的一些公用代码，基础库
├── openmldb-import          // 数据导入工具
├── openmldb-jdbc            // java sdk
├── openmldb-jmh             // 用作性能和稳定性测试相关
├── openmldb-native          // swig自动生成的代码
├── openmldb-spark-connector // spark的connector实现，用来读写OpenMLDB
├── openmldb-synctool        // 在离线同步工具
└── openmldb-taskmanager     // 离线任务管理模块
```

## Python SDK
```
python
├── openmldb
│   ├── dbapi                // dbapi接口封装
│   ├── native               // swig自动生成的代码
│   ├── sdk                  // 调用底层c++接口代码
│   ├── sqlalchemy_openmldb  // sqlalchemy接口封装
│   ├── sql_magic            // notebook magic
│   └── test                 // 测试相关
```

## 离线执行引擎
https://github.com/4paradigm/spark