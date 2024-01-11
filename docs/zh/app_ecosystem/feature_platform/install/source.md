# 源码编译

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