# QuickStart

## 安装部署
注意特征平台为独立于OpenMLDB的工具，用户需要首先进行OpenMLDB的安装部署，再启动特征平台。OpenMLDB的部署请参照[文档]（https://openmldb.ai/docs/zh/main/quickstart/openmldb_quickstart.html#id3）。

1. 准备配置文件并命名为 `application.yml`。其中，OpenMLDB的信息需要与已真实部署一致。

```
server:
  port: 8888
 
openmldb:
  zk_cluster: 127.0.0.1:2181
  zk_path: /openmldb
  apiserver: 127.0.0.1:9080
```

2. 拉取镜像并启动Docker容器.

```
docker run -d -p 8888:8888 -v `pwd`/application.yml:/app/application.yml registry.cn-shenzhen.aliyuncs.com/tobe43/openmldb-feature-platform
```

## 特征平台使用
这里我们使用一个简单的例子来介绍特征平台的使用方法。使用任意网页浏览器访问特征平台服务地址 http://127.0.0.1:8888/ 。

### 导入数据
1. 创建数据库： 创建新的数据库`demo_db`

![create_db](../images/create_db.png)

2. 创建数据表：选择`demo_db`,创建新的数据表`t1`，并且添加数据列`name`和`age`，选择对应数据类型。

![create_table](../images/create_table.png)

### 创建特征

### 生成离线样本

### 创建在线服务