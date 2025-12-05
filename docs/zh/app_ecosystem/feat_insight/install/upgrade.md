# 版本升级

## 介绍

FeatInsight 对外提供 HTTP 接口，底层依赖 OpenMLDB 数据库存储元数据，因此可以通过多实例和 Rolling update 等方法进行版本升级。

## 单实例升级步骤

1. 下载新版本的安装包或 Docker 镜像。
2. 停止当前正在运行的 FeatInsight 实例。
3. 基于新版本 FeatInsight 包启动新实例。
