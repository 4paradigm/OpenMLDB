# Docker

## Introduction

User official Docker image for quick deployment of FeatInsight feature services.

## All-in-One Image

With All-in-One image which contains a automatic OpenMLDB deployment, you can start both the OpenMLDB cluster and FeatInsight at the same time. No additional actions are required.

```
docker run -d -p 8888:8888 registry.cn-shenzhen.aliyuncs.com/tobe43/portable-openmldb
```

It takes around one minute to start. You can check the logs through `docker logs`.

After successful start-up, you can access FeatInsight service with any web browser at `http://127.0.0.1:8888`.

## Docker Image without OpenMLDB

With this image, you need to deploy a OpenMLDB cluster in advance, and then start this FeatInsight docker container. There are more steps but it offers higher flexibility.

Please refer to [OpenMLDB Deployment](../../../deploy/index.rst) to deploy a OpenMLDB cluster.

Then, refer to [FeatInsight Configuration File](./config_file.md) to create an `application.yml` configuration file.

```
server:
  port: 8888
 
openmldb:
  zk_cluster: 127.0.0.1:2181
  zk_path: /openmldb
  apiserver: 127.0.0.1:9080
```

For Linux OS, use the following command to start the container.

```
docker run -d -p 8888:8888 --net=host -v `pwd`/application.yml:/app/application.yml registry.cn-shenzhen.aliyuncs.com/tobe43/featinsight
```

For MacOS, since virtual machine is used to start Docker container,  `--net=host` is not working properly, please configure `application.yml` to point to OpenMLDB service addresses correctly.

```
docker run -d -p 8888:8888  -v `pwd`/application.yml:/app/application.yml registry.cn-shenzhen.aliyuncs.com/tobe43/featinsight
```
