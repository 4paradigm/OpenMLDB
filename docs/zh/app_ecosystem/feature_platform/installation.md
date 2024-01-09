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