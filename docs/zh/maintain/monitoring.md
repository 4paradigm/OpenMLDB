# 监控

## 概述

OpenMLDB 的监控方案概述如下：

- 使用 [Prometheus](https://prometheus.io) 收集监控指标，[Grafana](https://grafana.com/oss/grafana/) 可视化指标
- OpenMLDB exporter 暴露数据库级别的监控指标
- 每个组件作为独立的 server 暴露组件级别的监控指标
- 使用 [node_exporter](https://github.com/prometheus/node_exporter) 暴露机器和操作系统相关指标

## 安装运行 OpenMLDB exporter

### 简介

[![PyPI](https://img.shields.io/pypi/v/openmldb-exporter?label=openmldb-exporter)](https://pypi.org/project/openmldb-exporter/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/openmldb-exporter?style=flat-square)

OpenMLDB exporter 是以 Python 实现的 Prometheus exporter，核心是通过数据库 SDK 连接 OpenMLDB 实例并通过 SQL 语句查询暴露监控指标。Exporter 会发布到 PyPI，可以通过 pip 安装最新发布的 `openmldb-exporter`，开发使用说明详见代码目录 [README](https://github.com/4paradigm/openmldb-exporter)。

### 环境要求

- Python >= 3.8
- OpenMLDB >= 0.5.0

### 准备

1. 获取 OpenMLDB

   你可以从 [OpenMLDB release](https://github.com/4paradigm/OpenMLDB/releases) 页面下载预编译的安装包.

2. 启动 OpenMLDB

   参见 [install_deploy](../deploy/install_deploy.md) 如何搭建 OpenMLDB。组件启动时需要保证有 flag `--enable_status_service=true`, OpenMLDB启动脚本（无论是sbin或bin）都已配置为true，如果你使用个人方式启动，需要保证启动 flag 文件 (`conf/(tablet|nameserver).flags`) 中有 `--enable_status_service=true`。

3. 注意：合理选择 OpenMLDB 各组件和 OpenMLDB exporter, 以及 Prometheus, Grafana 的绑定 IP 地址，确保 Grafana 可以访问到 Prometheus, 并且 Prometheus，OpenMLDB exporter 和 OpenMLDB 各个组件之间可以相互访问。

### 部署 OpenMLDB exporter

使用 Docker 或者 Pip 安装运行 openmdlb-exporter

<details open=true><summary>Docker</summary>

```sh
docker run ghcr.io/4paradigm/openmldb-exporter \
    --config.zk_root=<openmldb_zk_addr> \
    --config.zk_path=<openmldb_zk_path>
```

</details>

<details open=true><summary>Pip</summary>

```sh
pip install openmldb-exporter

# start
openmldb-exporter \
    --config.zk_root=<openmldb_zk_addr> \
    --config.zk_path=<openmldb_zk_path>
```
</details></br>

注意将 `<openmdlb_zk_addr>` and `<openmldb_zk_path>` 替换成正确的值. 成功后就可以用 curl 查询状态:

```sh
curl http://<IP>:8000/metrics
```
`<IP>` 是容器的 IP, 如果从 pip 安装运行则是本机 IP.

<details><summary>样例输出</summary>

```sh
# HELP openmldb_connected_seconds_total duration for a component conncted time in seconds                              
# TYPE openmldb_connected_seconds_total counter                                                                        
openmldb_connected_seconds_total{endpoint="172.17.0.15:9520",role="tablet"} 208834.70900011063                         
openmldb_connected_seconds_total{endpoint="172.17.0.15:9521",role="tablet"} 208834.70700001717                         
openmldb_connected_seconds_total{endpoint="172.17.0.15:9522",role="tablet"} 208834.71399998665                         
openmldb_connected_seconds_total{endpoint="172.17.0.15:9622",role="nameserver"} 208833.70000004768                     
openmldb_connected_seconds_total{endpoint="172.17.0.15:9623",role="nameserver"} 208831.70900011063                     
openmldb_connected_seconds_total{endpoint="172.17.0.15:9624",role="nameserver"} 208829.7230000496                      
# HELP openmldb_connected_seconds_created duration for a component conncted time in seconds                            
# TYPE openmldb_connected_seconds_created gauge                                                                        
openmldb_connected_seconds_created{endpoint="172.17.0.15:9520",role="tablet"} 1.6501813860467942e+09                   
openmldb_connected_seconds_created{endpoint="172.17.0.15:9521",role="tablet"} 1.6501813860495396e+09                   
openmldb_connected_seconds_created{endpoint="172.17.0.15:9522",role="tablet"} 1.650181386050323e+09                    
openmldb_connected_seconds_created{endpoint="172.17.0.15:9622",role="nameserver"} 1.6501813860512116e+09               
openmldb_connected_seconds_created{endpoint="172.17.0.15:9623",role="nameserver"} 1.650181386051238e+09                
openmldb_connected_seconds_created{endpoint="172.17.0.15:9624",role="nameserver"} 1.6501813860512598e+09               
```

</details>

### OpenMLDB exporter 配置

查看帮助信息:
```sh
openmldb-exporter -h
```
`--config.zk_root` 和 `--config.zk_path` 是必须的.

<details><summary>所有选项</summary>

```
usage: openmldb-exporter [-h] [--log.level LOG.LEVEL] [--web.listen-address WEB.LISTEN_ADDRESS]
                        [--web.telemetry-path WEB.TELEMETRY_PATH] [--config.zk_root CONFIG.ZK_ROOT]
                        [--config.zk_path CONFIG.ZK_PATH] [--config.interval CONFIG.INTERVAL]

OpenMLDB exporter

optional arguments:
 -h, --help            show this help message and exit
 --log.level LOG.LEVEL
                       config log level, default WARN
 --web.listen-address WEB.LISTEN_ADDRESS
                       process listen port, default 8000
 --web.telemetry-path WEB.TELEMETRY_PATH
                       Path under which to expose metrics, default metrics
 --config.zk_root CONFIG.ZK_ROOT
                       endpoint to zookeeper, default 127.0.0.1:6181
 --config.zk_path CONFIG.ZK_PATH
                       root path in zookeeper for OpenMLDB, default /
 --config.interval CONFIG.INTERVAL
                       interval in seconds to pull metrics periodically, default 30.0
```

</details>


## 部署 node exporter

[node_exporter](https://github.com/prometheus/node_exporter) 是 Prometheus 官方实现的暴露系统指标的组件。 安装使用详见它的 README。


## 部署 Prometheus 和 Grafana

如何安装部署 Prometheus, Grafana 详见官方文档 [promtheus get started](https://prometheus.io/docs/prometheus/latest/getting_started/) 和 [Grafana get started](https://grafana.com/docs/grafana/latest/getting-started/getting-started-prometheus/) 。我们建议使用 Docker 容器快速部署, 并且 Grafana >= 8.3, Prometheus >= 1.0.0 。

OpenMLDB 提供了 Prometheus 和 Grafana 配置文件以作参考，详见 [OpenMLDB mixin](https://github.com/4paradigm/openmldb-exporter/tree/main/openmldb_mixin):

- prometheus_example.yml: Prometheus 配置示例, 注意修改 `node`, `openmldb_components` 和 `openmldb_exporter` job 中的 target 地址
- openmldb_dashboard.json: OpenMLDB metrics 的 Grafana dashboard 配置, 分为两步:
   1. 在 Grafana data source 页面下，添加启动的 Prometheus server 地址作为数据源
   2. 在 dashboard 浏览页面下，点击导入一个 dashboard, 输入 dashboard ID `17843`, 或者直接上传该 json 配置文件
      - 导入详细说明见 [Grafana import dashboard](https://grafana.com/docs/grafana/latest/dashboards/manage-dashboards/#import-a-dashboard)
      - Grafana dashboard 配置见 https://grafana.com/grafana/dashboards/17843

## 理解现有的监控指标

以 OpenMLDB 集群系统为例，监控指标根据 Prometheus pull job 不同，分为两类：

### 1. DB-Level 指标

通过 OpenMLDB exporter 暴露，在 `prometheus_example.yml` 配置中对应 `job_name=openmldb_exporter`的一项：

   ```yaml
     - job_name: openmldb_exporter
       # pull OpenMLDB DB-Level specific metric
       # change the 'targets' value to your deployed OpenMLDB exporter endpoint
       static_configs:
         - targets:
           - 172.17.0.15:8000
   ```

   暴露的指标类别主要为:

   - component status: 集群组件状态
   - table status: 数据库表相关信息，如 `rows_count`, `memory_bytes`

   你可以通过

   ```bash
   curl http://172.17.0.15:8000/metrics
   ```

   查看完整 DB-Level 指标和帮助信息。

通过Component-Level 指标通过Grafana聚合的DB-Level 指标（未单独声明时，time单位为us）：

- deploy query response time: deployment query 在OpenMLDB内部的运行时间，按DB.DEPLOYMENT汇总
  **需要全局变量 `deploy_stats` 开启后才会开始统计, 在 OpenMLDB CLI 中输入 SQL:**

   ```sql
   SET GLOBAL deploy_stats = 'on';
   ```
   然后，还需要执行deplpoyment，才会出现相应的指标。
   如果SET变量为off，会清空server中的所有deployment指标并停止统计（已被Prometheus抓取的数据不影响）。
   - count：count类统计值从deploy_stats on时开始统计，不区分请求的成功和失败。
   - latency, qps：这类指标只统计`[current_time - interval, current_time]`时间窗口内的数据，interval由Tablet Server配置项`bvar_dump_interval`配置，默认为75秒。

- api server http time: 各API接口的处理耗时（不包含route），只监测接口耗时，不做细粒度区分，目前也不通过Grafana展示，可以通过Prometheus手动查询。目前监测`deployment`、`sp`和`query`三种方法。
   - api server route time: APIServer进行http route的耗时，通常为us级别，一般忽略不计

以上聚合指标的获取方式见下文。在组件指标中，deploy query response time关键字为`deployment`，api server http time关键字为`http_method`。如果指标展示不正常，可以查询组件指标定位问题。

### 2. Component-Level 指标

OpenMLDB 的相关组件（即 nameserver, tablet, etc）, 本身作为 BRPC server，暴露了 [Prometheus 相关指标](https://github.com/apache/brpc/blob/master/docs/en/bvar.md#export-to-prometheus)， 只需要配置 Prometheus server 从对应地址拉取指标即可。对应 `prometheus_example.yml`中 `job_name=openmldb_components` 项：

   ```yaml
     - job_name: openmldb_components
       # job to pull component metrics from OpenMLDB like tablet/nameserver
       # tweak the 'targets' list in 'static_configs' on your need
       # every nameserver/tablet component endpoint should be added into targets
       metrics_path: /brpc_metrics
       static_configs:
         - targets:
           - 172.17.0.15:9622
   ```

   暴露的指标主要是

   - BRPC server 进程相关信息
   - 对应 BRPC server 定义的 RPC method 相关指标，例如该 RPC 的请求 `count`, `error_count`, `qps` 和 `response_time`
   - Deployment 相关指标，分deployment统计，但只统计该tablet上的deployment请求。它们将通过Grafana聚合，形成最终的的集群级别Deployment指标。

   通过

   ```bash
   curl http://${COMPONENT_IP}:${COMPONENT_PORT}/brpc_metrics
   ```

   查看指标和帮助信息。注意不同的组件暴露的指标会有所不同。
