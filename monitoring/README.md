# OpenMLDB Prometheus Exporter

[![PyPI](https://img.shields.io/pypi/v/openmldb-exporter?label=openmldb-exporter)](https://pypi.org/project/openmldb-exporter/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/openmldb-exporter)

## Intro

This directory contains

1. OpenMLDB Exporter exposing prometheus metrics
2. OpenMLDB mixin provides well-configured examples for prometheus server and grafana dashboard

## Requirements

- A runnable OpenMLDB instance that is accessible from your network
- OpenMLDB version >= 0.5.0
- Python >= 3.8


## Setup

For those want to try, simply install from pip, it will install the latest release:

```bash
pip install openmldb-exporter
```

Then type

```sh
openmldb-exporter -h
```

To see which flags should provided.

Developers may refer [Development](#development) for how to setup openmldb exporter from source code.


## Development

### Extra Requirements

- Same in [Requirements](#requirements)
- [poetry](https://github.com/python-poetry/poetry) as build tool
- cmake (optional if want to test latest commit)

### Build python SDK (Optional)

openmldb exporter depends on [openmldb python SDK](https://pypi.org/project/openmldb/). For those introducing changes in python SDK or native code as well, this steps is required in order to take the latest Python SDK locally instead of the published one.

#### 1. Build native code

`cd` to root directory of OpenMLDB, and run:

```bash
make SQL_PYSDK_ENABLE=ON
```

This will generate compiled shared library in `python` source directory.

#### 2. Fix dependency list

Back to openmldb exporter directory, modify `pyproject.toml` and make content like below:

```toml
[tool.poetry.dependencies]
...
# openmldb = "^0.6.0"
# uncomment below to use openmldb sdk built from source
# set develop = true so changes in python will take effect immediately
openmldb = { path = "../python/", develop = true }
```

### Run

1. Setup python dependencies:

   ```bash
   poetry install
   ```

2. start openmldb exporter

   ```bash
   poetry run openmldb-exporter
   ```

   You need pass necessary flags after `openmldb-exporter`. Run `poetry run openmldb-exporter --help` to get the help info

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

3. View the available metrics, you can pull through `curl`

   ```bash
   curl http://127.0.0.1:8000/metrics
   ```

   A example output:

   ```bash
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
