# Docker

## 介绍

使用构建好的 Docker 镜像, 可以快速启动 OpenMLDB 特征服务.

## 配置

参考[特征平台配置文件](./config_file.md)，创建 `application.yml` 配置文件。

```
server:
  port: 8888
 
openmldb:
  zk_cluster: 127.0.0.1:2181
  zk_path: /openmldb
  apiserver: 127.0.0.1:9080
```

## Linux

参考 [OpenMLDB 部署文档](../../../deploy/index.rst) 提前部署 OpenMLDB 集群。

启动 OpenMLDB 特征平台 容器.

```
docker run -d -p 8888:8888 --net=host -v `pwd`/application.yml:/app/application.yml registry.cn-shenzhen.aliyuncs.com/tobe43/openmldb-feature-platform
```

## MacOS

由于 MacOS 通过虚拟机启动 Docker 容器，使用 `--net=host` 参数无法正常工作，需要提前修改配置文件指向正确的 OpenMLDB 服务。

```
docker run -d -p 8888:8888  -v `pwd`/application.yml:/app/application.yml registry.cn-shenzhen.aliyuncs.com/tobe43/openmldb-feature-platform
```
