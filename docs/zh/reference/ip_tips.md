# IP 配置

## 概述

OpenMLDB docker镜像或发布包内的ip配置默认都是127.0.0.1，如果是外部访问，需要更改ip配置。如果使用容器，可能还需要更改容器启动方式。

首先，让我们定义下，什么是外部？

- 物理机：一台主机访问另一台主机，就是外部。同一主机上，使用127.0.0.1也可正常通讯；外部则必须使用“被访问主机“的公网IP。
- 容器：同一主机的容器外，都是外部，包括同一主机的另一个容器、同一主机的物理环境，另外的主机。

其次，让我们明确下，OpenMLDB有哪几种分布形式？
- onebox，所有OpenMLDB server都在一个环境下，同一物理机或一个容器内。例如，我们的[快速上手](../quickstart/openmldb_quickstart.md)，就是将所有进程都放在一个容器内。
- 分布式，正式生产环境中常用分布式，server在不同物理机上，它们自然是需要绑定公网IP。

**分布式部署，除了公网IP，还需要注意网络白名单，请参考[安装部署-网络白名单](../deploy/install_deploy.md#网络白名单)。**

由于容器的网络限制，onebox型的OpenMLDB常出现，IP配置错误、客户端无法连接集群等问题。下面我们将介绍**onebox型OpenMLDB**如何修改配置实现**外部访问**。
```{attention}
单机版中，不只是需要改endpoint，nameserver的配置中的tablet IP `--tablet=`也需要修改。
```

## Onebox型OpenMLDB外部访问

OpenMLDB有多种访问方式，包括HTTP，多种SDK，以及命令行CLI。

### Http

如果你只需要用restful http接口，那么，只需要考虑 APIServer 的 IP 是否可访问。（onebox型OpenMLDB的 APIServer 与其他server在同一环境下，它可以自由访问其他server）。

可以通过
```
curl http://<IP:port>/dbs/foo -X POST -d'{"mode":"online", "sql":"show components"}'
```
可以确认 APIServer 是否正常工作。这里的 nameserver、tablet server 等 IP 即使是127.0.0.1，也不会有问题，因为 APIServer 可以通过127.0.0.1访问到这些server。

#### 物理机onebox APIServer

跨主机访问物理机上的onebox，只需要让 APIServer 的 endpoint（绑定 IP）改为公网 IP。

#### 容器 onebox APIServer

如果是本机访问容器onebox中的 APIServer，可以**任选一种**下面的方式：
 - 可以通过bridge的方式，只需让 APIServer 的endpoint改为`0.0.0.0`（也就是绑定容器ip），然后http使用容器 IP 即可。
 ```{note}
 bridge IP通过`docker network inspect bridge`来查看，通过容器 ID 或 Name 找到 IP。

 Docker Desktop for Mac无法支持从物理机访问容器（以下任何模式都不能），参考[i-cannot-ping-my-containers](https://docs.docker.com/desktop/mac/networking/#i-cannot-ping-my-containers)。

但macOS中，可以从容器内访问其他容器。
 ```
 - 暴露端口，也需要修改apiserver的endpoint改为`0.0.0.0`。这样可以使用127.0.0.1或是公网ip访问到 APIServer。
    单机版：
    ```
    docker run -p 8080:8080 -it 4pdosc/openmldb:0.8.0 bash
    ```
    集群版：
    ```
    docker run -p 9080:9080 -it 4pdosc/openmldb:0.8.0 bash
    ```
 - 使用host网络，可以不用修改endpoint配置。缺点是容易引起端口冲突。
    ```
    docker run --network host -it 4pdosc/openmldb:0.8.0 bash
    ```

如果是跨主机访问容器 onebox 中的 APIServer，可以**任选一种**下面的方式：
 - 暴露端口，并修改 APIServer 的 endpoint 改为`0.0.0.0`。docker启动详情见上。
 - 使用host网络，并修改 APIServer 的 endpoint 改为`0.0.0.0`或是公网IP。docker启动详情见上。

只需要让 APIServer 的endpoint（绑定ip）改为公网 IP，使它可访问。APIServer 与集群内 server 的交互都在同一台 server 上，或同一容器内，并不需要更改。

### CLI/SDK

如果你需要在外部使用CLI/SDK，情况比只连接 APIServer 要复杂，需要保证CLI/SDK能访问到 ZooKeeper，nameserver,tablet server 和 TaskManager。
```{seealso}
由于server间内部通信是使用`endpoint`绑定的ip通信，而CLI/SDK也是直接获取同样的ip，直连nameserver，tablet server和 TaskManager，因此，serve之间用localhost等IP可以互相通信，CLI/SDK却有可能因为跨主机或容器，得到localhost:port这样的server地址，无法正常连接到这些server。
```

你可以通过这样一个简单的SQL脚本来测试确认连接是否正常。
```
show components;
create database db;
use db;
create table t1(c1 int);
set @@execute_mode='online';
insert into t1 values(1);
select * from t1;
```
其中`show components`可以看到CLI获得的 nameserver/tablet/TaskManager 的ip是什么样的。`insert`语句可以测试是否能连接并将数据写入tablet server。

下面，我们分情况讨论如何配置。

#### CLI/SDK->物理机onebox

跨主机访问物理机上的onebox，只需将所有 endpoint 改为公网 IP。

可使用以下命令快速修改。

单机版：
```
sed -i s/127.0.0.1/<IP>/g openmldb/conf/standalone*
```
集群版：

简单地可以更改所有conf文件，
```
sed -i s/127.0.0.1/<IP>/g openmldb/conf/*
sed -i s/0.0.0.0/<IP>/g openmldb/conf/taskmanager.properties
```
或者，精确的只修改集群版的配置文件。
```
cd /work/openmldb/conf/ && ls | grep -v _ | xargs sed -i s/127.0.0.1/<IP>/g && cd -
cd /work/openmldb/conf/ && ls | grep -v _ | xargs sed -i s/0.0.0.0/<IP>/g && cd -
```
```{note}
集群版的ip替换，会将`zk_cluster`的IP也修改为公网地址。通常来讲，是可以的，因为 ZooKeeper 启动默认是bind `0.0.0.0`，本地 server 使用公网IP/0.0.0.0/localhost都能访问到。
```

#### CLI/SDK->容器onebox

如果是本机的容器外CLI访问容器onebox，可以**任选一种**下面的方式：

- bridge连接，bridge IP查看参考[容器onebox-apiserver](#容器-onebox-apiserver)，将所有endpoint配置改为bridge ip。不可以是`0.0.0.0`，容器外CLI/SDK无法通过`0.0.0.0`找到容器内的server。

- 暴露端口，并将conf所有endpoint改为bridge IP或`0.0.0.0`。本机也可以顺利通信。

单机版需要暴露三个组件（nameserver，tabletserver，APIServer）的端口：
```
docker run -p 6527:6527 -p 9921:9921 -p 8080:8080 -it 4pdosc/openmldb:0.8.0 bash
```

集群版需要暴露zk端口与所有组件的端口：
```
docker run -p 2181:2181 -p 7527:7527 -p 10921:10921 -p 10922:10922 -p 8080:8080 -p 9902:9902 -it 4pdosc/openmldb:0.8.0 bash
```

- 使用host网络，可以不用修改 endpoint 配置。如果有端口冲突，请修改 server 的端口配置。
```
docker run --network host -it 4pdosc/openmldb:0.8.0 bash
```

如果是跨主机使用 CLI/SDK 访问问容器onebox，只能通过`--network host`，并更改所有endpoint为公网IP，才能顺利访问。

```{tip}
`-p` 将“物理机端口”和“容器内端口”进行绑定，可能出现“容器端口号”在物理机上已被使用的情况。我们不推荐“切换暴露端口号”的方式，请改变 conf 中 endpoint 的端口号，暴露时使用同样的端口号。

暴露端口的模式，会无法绑定物理机ip（容器中仅有 docker bridge ip和127.0.0.1），所以，想要绑定公网IP，必须使用host网络。
```
