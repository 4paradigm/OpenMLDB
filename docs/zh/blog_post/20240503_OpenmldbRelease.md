# OpenMLDB v0.9.0 发布：SQL 能力大升级覆盖特征上线全流程

## 发布日期

25 April 2024

## Release note

[https://github.com/4paradigm/OpenMLDB/releases/tag/v0.9.0](https://github.com/4paradigm/OpenMLDB/releases/tag/v0.9.0)


## 亮点特性
- 增加最新版 SQLAlchemy 2 的支持，无缝集成 Pandas 和 Numpy 等常用 Python 框架。
- 支持更多数据后端，融合 TiDB 的分布式文件存储能力以及 OpenMLDB 内存高性能特征计算能力。
- 完善 ANSI SQL 支持，修复 first_value 语义，支持 MAP 类型和特征签名，离线模式支持 INSERT 语句。
- 支持 MySQL 协议，可用 NaviCat、Sequal Ace 及各种编程语言的 MySQL SDK 访问 OpenMLDB 集群。
- 支持 SQL 语法拓展，通过 SELECT CONFIG 或 CALL 语句直接进行在线特征计算。


社区朋友们大家好！OpenMLDB 正常发布了一个新的版本 v0.9.0，包含了 SQL 语法拓展、MySQL 协议兼容、TiDB 存储支持、在线执行特征计算、特征签名等功能，其中最值得关注和分享的就是对 MySQL 协议和 ANSI SQL 兼容的特性，以及本地拓展的 SQL 语法能力。
首先 MySQL 协议兼容让 OpenMLDB 的用户，可以使用任意的 MySQL 客户端来访问 OpenMLDB 集群，不仅限于 NaviCat、Sequal Ace 等 GUI 应用，还可以使用 Java JDBC MySQL Driver、Python SQLAlchemy、Go MySQL Driver 等各种编程语言的 SDK。更多介绍可以参考 《超高性能数据库 OpenM(ysq)LDB：无缝兼容 MySQL 协议 和多语言 MySQL 客户端》 。
其次新版本极大拓展了 SQL 的能力，尤其是在标准 SQL 语法上实现了 OpenMLDB 特有的请求模式和存储过程的执行。相比于传统的 SQL 数据库，OpenMLDB 覆盖机器学习的全流程，包含离线模式和在线模式，在线模式下支持用户传入单行样本数据，通过 SQL 特征抽取返回特征结果。过去我们需要先通过 Deploy 命令部署 SQL 成存储过程，然后通过 SDK 或 HTTP 接口进行在线特征计算。新版本加入了SELECT CONFIG 和 CALL 语句，用户在 SQL 中直接指定请求模式和请求样本就可以计算得到特征结果，示例如下。

```
-- 执行请求行为 (10, "foo", timestamp(4000)) 的在线请求模式 query
SELECT id, count (val) over (partition by id order by ts rows between 10 preceding and current row)
FROM t1
CONFIG (execute_mode = 'online', values = (10, "foo", timestamp (4000)))
```

也可以通过 ANSI SQL 的 CALL语句，以样本行作为参数传入进行存储过程的调用，示例如下。

```
-- 执行请求行为 (10, "foo", timestamp(4000)) 的在线请求模式 query
DEPLOY window_features SELECT id, count (val) over (partition by id order by ts rows between 10 preceding and current row)
FROM t1;


CALL window_features(10, "foo", timestamp(4000))
```

详细的 release note 参照： [https://github.com/4paradigm/OpenMLDB/releases/tag/v0.9.0](https://github.com/4paradigm/OpenMLDB/releases/tag/v0.9.0)
欢迎大家下载试用，提供意见。