# 简介

OpenM(ysq)LDB 是 OpenMLDB 数据库的子模块，项目名蕴含了 OpenMLDB 中包含 MySQL 的意思，用户可以通过使用 MySQL 命令行客户端以及多种编程语言的 MySQL Connector，实现 OpenMLDB 数据库特有的在线离线特征计算功能。目前支持 `MySQL Community CLI 8.0.29`, `Sequel Ace 4.0.13` 和 `Navicat Premium 16.3.4` 客户端，更多的客户端陆续适配中。

OpenMLDB 是基于 C++ 和 LLVM 实现的分布式高性能内存时序数据库，在架构设计和实现逻辑上与专注于单机的关系型数据库 MySQL 有很大区别，OpenMLDB 已经广泛应用于金融营销等硬实时在线特征计算场景中。由于二者都是提供标准的 ANSI SQL 接口，OpenMLDB 通过兼容 MySQL 协议，让客户直接使用熟悉的 MySQL 客户端，甚至在Java、Python 等 SDK 编程中也无需安装额外的客户端，可直接访问 OpenMLDB 数据以及执行特殊的 OpenMLDB SQL 特征抽取语法。
