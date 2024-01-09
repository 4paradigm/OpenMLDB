# OpenMLDB 特征平台

## 介绍

OpenMLDB 特征平台是一个先进的特征存储服务，利用 [OpenMLDB](https://github.com/4paradigm/OpenMLDB) 实现高效的特征管理和编排。

## 核心概念

* 特征：通过对原始数据进行特征抽取得到的可直接用于模型训练和推理的数据
* 特征视图：通过单个 SQL 计算语句定义的一组特征
* 数据表：在 OpenMLDB 中数据表包括能实时查询的在线存储以及支持分布式的离线存储
* 在线场景：通过上线特征服务，使用在线数据提供硬实时的在线特征抽取接口
* 离线场景：使用分布式计算，对离线数据进行特征计算并导出机器学习所需的样本文件
* 在线离线一致性：通过相同的 SQL 定义可保证在线场景和离线场景计算的特征结果一致

## 安装

### Java

下载 Jar 文件。

```
wget https://openmldb.ai/download/feature-platform/openmldb-feature-platform-0.8-SNAPSHOT.jar
```

准备配置文件并命名为 `application.yml`。

```
server:
  port: 8888
 
openmldb:
  zk_cluster: 127.0.0.1:2181
  zk_path: /openmldb
  apiserver: 127.0.0.1:9080
```

启动特征平台服务。

```
java -jar ./openmldb-feature-platform-0.8-SNAPSHOT.jar
```

### Docker

准备配置文件 `application.yml` 并启动 Docker 容器.

```
docker run -d -p 8888:8888 -v `pwd`/application.yml:/app/application.yml registry.cn-shenzhen.aliyuncs.com/tobe43/openmldb-feature-platform
```

### 源码编译

下载项目源码并从头编译。

```
git clone https://github.com/4paradigm/feature-platform

cd ./feature-platform/frontend/
npm run build

cd ../
mvn clean package
```

使用本地配置文件启动服务。

```
./start_server.sh
```

## 使用教程

使用任意网页浏览器访问特征平台服务地址 http://127.0.0.1:8888/ 。

1. 导入数据：使用 SQL 命令或前端表单进行创建数据库、创建数据表、导入在线数据和导入离线数据等操作。
2. 创建特征：使用 SQL 语句来定义特征视图，特征平台将使用 SQL 编译器进行特征分析并创建对应的特征。
3. 离线场景：选择想要导入的特征，可以同时选择不同特征视图的特征，并使用分布式计算把样本文件导入到本地或分布式存储。
3. 在线场景：选择想要上线的特征，一键发布成在线特征抽取服务，然后可使用 HTTP 客户端进行请求和返回在线特征抽取结果。
4. SQL 调试：执行任意的在线或者离线计算 SQL 语句，并且在网页前端查看执行的结果和日志。