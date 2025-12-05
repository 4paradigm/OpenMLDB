# Docker

## 介绍

使用官方构建好的 Docker 镜像, 可以快速部署 FeatInsight 特征服务.

## 内置 OpenMLDB 镜像

使用内置 OpenMLDB 的镜像，可以一键启动 OpenMLDB 集群和 FeatInsight 特征服务，无需额外部署即可使用特征服务。

```
docker run -d -p 8888:8888 registry.cn-shenzhen.aliyuncs.com/tobe43/portable-openmldb
```

启动 OpenMLDB 和 FeatInsight 需要约一分钟，可通过 `docker logs` 查看日志，启动成功后在本地浏览器打开 `http://127.0.0.1:8888` 即可访问 FeatInsight 服务。


## 不包含 OpenMLDB 镜像

使用不包含 OpenMLDB 的镜像，需要提前部署 OpenMLDB 集群，然后启动 FeatInsight 特征服务容器，部署步骤较繁琐但灵活性高。

首先参考 [OpenMLDB 部署文档](../../../deploy/index.rst) 提前部署 OpenMLDB 集群。

然后参考 [FeatInsight 配置文件](./config_file.md)，创建 `application.yml` 配置文件。

```
server:
  port: 8888
 
openmldb:
  zk_cluster: 127.0.0.1:2181
  zk_path: /openmldb
  apiserver: 127.0.0.1:9080
```

对于 Linux 操作系统可以使用下面命令启动 FeatInsight 容器.

```
docker run -d -p 8888:8888 --net=host -v `pwd`/application.yml:/app/application.yml registry.cn-shenzhen.aliyuncs.com/tobe43/featinsight
```

由于 MacOS 通过虚拟机启动 Docker 容器，使用 `--net=host` 参数无法正常工作，需要提前修改配置文件指向正确的 OpenMLDB 服务。

```
docker run -d -p 8888:8888  -v `pwd`/application.yml:/app/application.yml registry.cn-shenzhen.aliyuncs.com/tobe43/featinsight
```
