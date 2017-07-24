# rtidb部署文档

## 目录结构

* bin 存放rtidb二进制文件以及相关启动脚本
* binlogs 存储table的binlogs文件
* logs 存储rtidb运行日志文件
* conf 存放rtidb配置文件

## bin/start.sh

rtidb启动脚本，运行目录需要在bin下面，启动命令为
```
cd bin && sh start.sh
```

## bin/stop.sh

rtidb停止脚本，运行目录在bin下面，停止命令为
```
cd bin && sh stop.sh
```

## rtidb 配置文件

### 配置 rtidb启动端口号以及监听ip地址

这里只允许ip地址，不允许域名

```
--endpoint=0.0.0.0:9527
```

### 配置日志

```
# 配置日志存储路径，如果是自定义路径，请写绝对路径
--log_dir=./logs

# 配置日志级别, info 或者 warning
--log_level=info
```

