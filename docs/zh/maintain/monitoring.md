# 监控

## 概述

OpenMLDB 的监控方案概述如下：

- 使用 [prometheus](https://prometheus.io) 收集监控指标，[grafana](https://grafana.com/oss/grafana/) 可视化指标
- OpenMLDB exporter 暴露数据库级别的监控指标
- 每个组件作为独立的 server 暴露组件级别的监控指标
- 使用 [node_exporter](https://github.com/prometheus/node_exporter) 暴露机器和操作系统相关指标

## 安装运行 OpenMLDB exporter

### 简介

[![PyPI](https://img.shields.io/pypi/v/openmldb-exporter?label=openmldb-exporter)](https://pypi.org/project/openmldb-exporter/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/openmldb-exporter?style=flat-square)

OpenMLDB exporter 是以 Python 实现的 prometheus exporter，核心是通过数据库 SDK 连接 OpenMLDB 实例并通过 SQL 语句查询暴露监控指标。Exporter 会发布到 PyPI，可以通过 pip 安装最新发布的 `openmldb-exporter`，开发使用说明详见代码目录 [README](https://github.com/4paradigm/OpenMLDB/tree/main/monitoring)。

### 环境要求

- Python >= 3.8
- OpenMLDB >= 0.5.0

### 准备

1. 获取 OpenMLDB

   你可以从 [OpenMLDB release](https://github.com/4paradigm/OpenMLDB/releases) 页面下载预编译的安装包.

2. 启动 OpenMLDB

   参见 [install_deploy](../deploy/install_deploy.md) 如何搭建 OpenMLDB。组件启动时需要保证有 flag `--enable_status_service=true`, 或者确认启动 flag 文件 (`conf/(tablet|nameserver).flags`) 中有 `--enable_status_service=true`。

   默认启动脚本 `bin/start.sh` 开启了 server status, 不需要额外配置。
   
3. 注意：合理选择 OpenMLDB 各组件和 OpenMLDB exporter, 以及 prometheus, grafana 的绑定 IP 地址，确保 grafana 可以访问到 prometheus, 并且 prometheus，OpenMLDB exporter 和 OpenMLDB 各个组件之间可以相互访问。

### 部署 OpenMLDB exporter

1. 从 PyPI 安装 openmldb-exporter

   ```bash
   pip install openmldb-exporter
   ```

2. 运行

   默认会安装一个可执行文件 `openmldb-exporter`, 确认 pip 安装路径在你的 $PATH 环境变量里面。

   ```bash
   openmldb-exporter
   ```

   注意传入合适的参数，`openmldb-exporter -h` 查看 help:

   ```bash
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

3. 查看 metrics 列表

   ```bash
      $ curl http://127.0.0.1:8000/metrics
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

## 部署 node exporter

[node_exporter](https://github.com/prometheus/node_exporter) 是 prometheus 官方实现的暴露系统指标的组件。

进入 [release](https://github.com/prometheus/node_exporter/releases) 页面，下载并解压对应平台的压缩包。例如 linux amd64 平台下:
```sh
curl -SLO https://github.com/prometheus/node_exporter/releases/download/v1.5.0/node_exporter-1.5.0.linux-amd64.tar.gz
tar xzf node_exporter-*.tar.gz
cd node_exporter-*/

# 启动 node_exporter
./node_exporter
```

## 部署 Prometheus 和 Grafana

如何安装部署 prometheus, grafana 详见官方文档 [promtheus get started](https://prometheus.io/docs/prometheus/latest/getting_started/) 和 [grafana get started](https://grafana.com/docs/grafana/latest/getting-started/getting-started-prometheus/) 。
OpenMLDB 提供了 prometheus 和 grafana 配置文件以作参考，详见 [OpenMLDB mixin](https://github.com/4paradigm/OpenMLDB/tree/main/monitoring/openmldb_mixin/README.md):

- prometheus_example.yml: prometheus 配置示例, 注意修改 `node`, `openmldb_components` 和 `openmldb_exporter` job 中的 target 地址
- openmldb_dashboard.json: OpenMLDB metrics 的 grafana dashboard 配置, 分为两步:
   1. 在 grafana data source 页面下，添加启动的 prometheus server 地址作为数据源
   2. 在 dashboard 浏览页面下，点击导入一个 dashboard, 输入 dashboard ID `17843`, 或者直接上传该 json 配置文件
      - 导入详细说明见 [Grafana import dashboard](https://grafana.com/docs/grafana/latest/dashboards/manage-dashboards/#import-a-dashboard)
      - Grafana dashboard 配置见 https://grafana.com/grafana/dashboards/17843

## 理解现有的监控指标

以 OpenMLDB 集群系统为例，监控指标根据 prometheus pull job 不同，分为两类：

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
   - deploy query response time: deployment query 在 tablet 内部的运行时间

   **除了 deploy query response time 指标外, 成功配置监控之后都可以直接查询到指标. Deploy query response time 需要全局变量 `deploy_stats` 开启后才会有数据, 在 OpenMLDB CLI 中输入 SQL:**

   ```sql
   SET GLOBAL deploy_stats = 'on';
   ```

   你可以通过

   ```bash
   curl http://172.17.0.15:8000/metrics
   ```

   查看完整 DB-Level 指标和帮助信息。

### 2. Component-Level 指标

OpenMLDB 的相关组件（即 nameserver, tablet, etc), 本身作为 BRPC server，暴露了 [prometheus 相关指标](https://github.com/apache/incubator-brpc/blob/master/docs/en/bvar.md#export-to-prometheus)， 只需要配置 prometheus server 从对应地址拉取指标即可。对应 `prometheus_example.yml`中 `job_name=openmldb_components` 项：

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

   通过

   ```bash
   curl http://${COMPONENT_IP}:${COMPONENT_PORT}/brpc_metrics
   ```

   查看指标和帮助信息。注意不同的组件暴露的指标会有所不同。
