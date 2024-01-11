# Docker

准备配置文件 `application.yml` 并启动 Docker 容器.

```
docker run -d -p 8888:8888 -v `pwd`/application.yml:/app/application.yml registry.cn-shenzhen.aliyuncs.com/tobe43/openmldb-feature-platform
```
