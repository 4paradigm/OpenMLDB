# Monitoring

## Overview


The monitoring scheme of OpenMLDB is outlined as follows：

- Use [Prometheus](https://prometheus.io) to collect monitoring metrics, [Grafana](https://grafana.com/oss/grafana/) to visualize metrics
- OpenMLDB exporter exposes database-level metrics
- Each component as a server itself expose component-level metrics
- Uses [node_exporter](https://github.com/prometheus/node_exporter) to expose machine and operating system related metrics

## Install and Run OpenMLDB Exporter

### Introduction

[![PyPI](https://img.shields.io/pypi/v/openmldb-exporter?label=openmldb-exporter)](https://pypi.org/project/openmldb-exporter/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/openmldb-exporter?style=flat-square)

The OpenMLDB exporter is a Prometheus exporter implemented in Python. The core connects the OpenMLDB instance through the database SDK and will query the exposed monitoring indicators through SQL statements. Exporter publish into PyPI. You can install the latest `openmldb-exporter` through pip. For development instructions, please refer to the code directory [README](https://github.com/4paradigm/openmldb-exporter).


### Environmental Requirements

- Python >= 3.8
- OpenMLDB >= 0.5.0

### Preparation

1. Get OpenMLDB

   You can download precompiled packages from the [OpenMLDB release](https://github.com/4paradigm/OpenMLDB/releases) page. 

2. Start OpenMLDB

    See [install_deploy](../deploy/install_deploy.md) How to start OpenMLDB components

    OpenMLDB exporter requires OpenMLDB starts with the server status feature, make sure there is the startup parameter `--enable_status_service=true`, or `--enable_status_service=true` appears in startup flag files (usually `conf/(tablet|nameserver).flags`).

   The startup script `bin/start.sh` should enable server status by default.
   
3. Note: Make sure to select the binding IP addresses of OpenMLDB components OpenMLDB exporter as well as Prometheus and Grafana to ensure that Grafana can access Prometheus, and that Prometheus, OpenMLDB exporter, and OpenMLDB components can access each other.


### Deploy the OpenMLDB exporter

**You can run openmdlb-exporter from docker, or install and run directly from PyPI.**

<details open=true><summary>Use docker</summary>

```sh
docker run ghcr.io/4paradigm/openmldb-exporter \
    --config.zk_root=<openmldb_zk_addr> \
    --config.zk_path=<openmldb_zk_path>
```

</details>

<details open=true><summary>Install and Run from PyPI</summary>

```sh
pip install openmldb-exporter

# start
openmldb-exporter \
    --config.zk_root=<openmldb_zk_addr> \
    --config.zk_path=<openmldb_zk_path>
```
</details></br>

And replace `<openmdlb_zk_addr>` and `<openmldb_zk_path>` to correct value. Afterwards, you can check metrics with curl:

```sh
curl http://<IP>:8000/metrics
```
`<IP>` is docker container IP, or `127.0.0.1` if installing from PyPI.

<details><summary>Example output</summary>

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

### Configuration

You can view the help from:
```sh
openmldb-exporter -h
```
`--config.zk_root` and `--config.zk_path` are mandatory.

<details><summary>Available options</summary>

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

## Deploy Node Exporter

[node_exporter](https://github.com/prometheus/node_exporter) is an official implementation of Prometheus that exposes system metrics, read their README about setup.

## Deploy Prometheus and Grafana

For installation and deployment of Prometheus and Grafana, please refer to the official documents [promtheus get started](https://prometheus.io/docs/prometheus/latest/getting_started/) and [Grafana get started](https://grafana.com/docs/ grafana/latest/getting-started/getting-started-prometheus/). We recommend quick start with docker images, and use Grafana >= 8.3 and Prometheus >= 1.0.0 .

OpenMLDB provides Prometheus and Grafana configuration files for reference, see [OpenMLDB mixin](https://github.com/4paradigm/openmldb-exporter/tree/main/openmldb_mixin):

- prometheus_example.yml: Prometheus configuration example, remember to modify the target address in `node`, `openmldb_components` and `openmldb_exporter` jobs for your environment
- openmldb_dashboard.json: Grafana dashboard configuration for OpenMLDB metrics, import it two steps:
   1. Under the Grafana data source page, add the started Prometheus server address as the data source
   2. Under the dashboard browsing page, click the `Import` button, paste the dashboard ID `17843`, or upload this json file directly
      - Rad more in [Grafana import dashboard](https://grafana.com/docs/grafana/latest/dashboards/manage-dashboards/#import-a-dashboard)
      - Page in Grafana dashboard: https://grafana.com/grafana/dashboards/17843

## Understand Existing Monitoring Metrics

Taking the OpenMLDB cluster system as an example, there are two type of metrics categorized by Prometheus pull jobs:

### 1. DB-Level metrics

Exposed through the OpenMLDB exporter, the `job_name=openmldb_exporter` entry in the `prometheus_example.yml`:

   ```yaml
     - job_name: openmldb_exporter
       # pull OpenMLDB DB-Level specific metric
       # change the 'targets' value to your deployed OpenMLDB exporter endpoint
       static_configs:
         - targets:
           - 172.17.0.15:8000
   ```

   In detail three type of metrics:

   - Component status: table/nameserver/... information and status
   - Table status: database table related information, such as `rows_count`, `memory_bytes`
   - Deploy query response time

   **All metrics should visible already after a successful setup, excepting deploy query response time. Deploy query response time requires a extra global variable `deploy_stats` on, from OpenMLDB CLI:**

   ```sql
   SET GLOBAL deploy_stats = 'on';
   ```

   The full DB-Level metrics can be listed through the following command:

   ```bash
   curl http://172.17.0.15:8000/metrics
   ```

### 2. Component-Level metrics

The related components of OpenMLDB (nameserver, tablet, etc), themselves as BRPC server, and expose [prometheus related metrics](https://github.com/apache/incubator-brpc/blob/master/docs/en/bvar .md#export-to-prometheus), you only need to configure the Prometheus server to pull metrics from the corresponding address. It corresponds to the `job_name=openmldb_components` item in `prometheus_example.yml`:

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

   The metrics of exposure are mainly

   - BRPC server process related information
   - Corresponding to the RPC method related metrics defined by the BRPC server, such as the RPC request `count`, `error_count`, `qps` and `response_time`

   Metrics and help information can be shown through the following command (Note that the metrics exposed by different components will vary):

   ```bash
   curl http://${COMPONENT_IP}:${COMPONENT_PORT}/brpc_metrics
   ```
