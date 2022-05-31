# IP 配置

## 物理环境 IP
跨主机访问OpenMLDB服务，需要将OpenMLDB配置中的endpoint配置`127.0.0.1`改为`0.0.0.0`或公网IP，再启动OpenMLDB服务。请保证端口没有被防火墙阻挡。
```{attention}
单机版中，不只是需要改endpoint，nameserver的配置中还有tablet ip `--tablet=`，此处也需要修改。
```

## Docker IP

希望从容器的外部（无论是同一主机还是跨主机）访问容器内，请先
更改endpoint`127.0.0.1`为 `0.0.0.0`（单机版中`tablet`配置项也需要更改），以避免不必要的麻烦。

### 容器外部访问（同一主机）
在同一主机中，想要从**容器的外部**（物理机或者是其他容器）访问**容器内**启动的OpenMLDB服务端，可以直接使用bridge的方式连接，也可以暴露端口，还可以直接使用host网络模式。

```{caution}
Docker Desktop for Mac无法支持从物理机访问容器（以下任何模式都不能），参考[i-cannot-ping-my-containers](https://docs.docker.com/desktop/mac/networking/#i-cannot-ping-my-containers)。

但macOS中，可以从容器内访问其他容器。
```

#### bridge连接
bridge连接不需要更改docker run命令，只需要查询一下bridge ip。
```
docker network inspect bridge
```
查看“Containers”字段，可以看到每个容器绑定的ip，客户端使用该ip就可以进行访问。

例如，启动容器并运行OpenMLDB单机版后，inspect结果为`172.17.0.2`，那么CLI连接可以使用：
```
../openmldb/bin/openmldb --host 172.17.0.2 --port 6527
```

#### 暴露端口
在启动容器时通过 `-p` 暴露端口，客户端可以使用本机ip地址或回环地址进行访问。

单机版需要暴露三个组件（nameserver，tabletserver，apiserver）的端口：
```
docker run -p 6527:6527 -p 9921:9921 -p 8080:8080 -it 4pdosc/openmldb:0.5.1 bash
```

集群版需要暴露zk端口与所有组件的端口：
```
docker run -p 2181:2181 -p 7527:7527 -p 10921:10921 -p 10922:10922 -p 8080:8080 -p 9902:9902 -it 4pdosc/openmldb:0.5.1 bash
```

```{tip}
`-p` 将“物理机端口”和“容器内端口”进行绑定，可能出现“容器端口号”在物理机上已被使用的情况。

如果OpenMLDB服务仅在单个容器内，只需要改变一下暴露的物理机端口号，客户端相应地改变访问端口。各个服务进程的配置项不需要更改。

如果OpenMLDB服务进程是分布式的，在多个容器内，出现“端口号被占用”，我们不推荐“切换暴露端口号”的方式，请改变配置的端口号，暴露时使用同样的端口号。
```

#### host network
或者更方便地，使用 host networking，不进行端口隔离，例如：
```
docker run --network host -it 4pdosc/openmldb:0.5.1 bash
```
但这种情况下，很容易出现端口已被主机中其他进程占用。如果出现占用，请仔细更改端口号。

### 跨主机访问本机容器
除了bridge模式无法做到跨主机访问，暴露端口和host network的方法均可以实现**跨主机**访问本机容器。
