# Diagnostic Tool

## Overview

OpenMLDB provides a diagnostic tool to diagnose problems conveniently for users. It can check items as below:

- Version
- Configuration
- Log
- Run test SQL

## Usage

1. Download diagnostic tool
```bash
pip install openmldb-tool
```

2. Config cluster distribution

standalone yaml conf
```yaml
mode: standalone
nameserver:
  -
    endpoint: 127.0.0.1:6527
    path: /work/openmldb
tablet:
  -
    endpoint: 127.0.0.1:9527
    path: /work/openmldb
```

cluster yaml conf
```yaml
mode: cluster
zookeeper:
  zk_cluster: 127.0.0.1:2181
  zk_root_path: /openmldb
nameserver:
  -
    endpoint: 127.0.0.1:6527
    path: /work/ns1
tablet:
  -
    endpoint: 127.0.0.1:9527
    path: /work/tablet1
  -
    endpoint: 127.0.0.1:9528
    path: /work/tablet2
taskmanager:
  -
    endpoint: 127.0.0.1:9902
    path: /work/taskmanager1
```

3. Setup SSH Passwordless Login

As diagnostic tool will pull conf and log files from remote nodes when checking cluster, SSH passwordless shoud be setup. If you do not konw how to set, you can refer [here]((https://www.itzgeek.com/how-tos/linux/centos-how-tos/ssh-passwordless-login-centos-7-rhel-7.html))

4. Run diagnostic tool

```bash
openmldb_tool --dist_conf=/tmp/standalone_dist.yml
```

There are some advanced options can be specified as below:

- --dist_conf: To config the distribution of cluster
- --data_dir: The data dir to store the conf and log files pulled from remote. The default value is `/tmp/diagnose_tool_data`
- --check: The item to check. The default value is `ALL`. It can be specified as `CONF/LOG/SQL/VERSION`
- --exclude: The item do not check. Only work if `check` option is `ALL`. It can be specified as `CONF/LOG/SQL/VERSION`
- --log_level: The default value is `info`. It can be specified as `debug/warn/info`
- --log_dir: Specific the output dir. It will print to stdout if not set
- --env: If the cluster is started with `start-all.sh` script, `onebox` should be setted.

For instance, we can check `conf` only and print the ouput to local dir as below:
```
openmldb_tool --dist_conf=/tmp/cluster_dist.yml --check=conf --log_dir=./
```

**Note**: If you want to diagnostie standalone mode OpenMLDB, you need to run diagnostic tool on the OpenMLDB node.

You can use `openmldb_tool --helpfull` to check all options. e.g. `--sdk_log` can print the log in sdk(zk, glog) for debug.