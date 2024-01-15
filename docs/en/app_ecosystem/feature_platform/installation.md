## Installation

### Java

Download the jar file.

```
wget https://openmldb.ai/download/feature-platform/openmldb-feature-platform-0.8-SNAPSHOT.jar
```

Prepare the config file which may be named as `application.yml`.

```
server:
  port: 8888
 
openmldb:
  zk_cluster: 127.0.0.1:2181
  zk_path: /openmldb
  apiserver: 127.0.0.1:9080
```

Start the feature platform server.

```
java -jar ./openmldb-feature-platform-0.8-SNAPSHOT.jar
```

### Docker

Prepare the config file `application.yml` and start the docker container.

```
docker run -d -p 8888:8888 -v `pwd`/application.yml:/app/application.yml registry.cn-shenzhen.aliyuncs.com/tobe43/openmldb-feature-platform
```

### Compiling from Source

Clone the source code and build from scratch.

```
git clone https://github.com/4paradigm/feature-platform

cd ./feature-platform/frontend/
npm run build

cd ../
mvn clean package
```

Start the server with local config file.

```
./start_server.sh
```