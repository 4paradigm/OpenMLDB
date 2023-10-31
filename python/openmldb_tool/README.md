# Diag Tool

In `diagnostic_tool/`:

```
├── collector.py # collect version/config/logs (local or remote ssh/scp, defined by distribution conf file)
├── common_err.yml
├── conf_validator.py
├── connector.py # openmldb singleton connection
├── diagnose.py # main
├── dist_conf.py # read distribution conf file, dist.yml or hosts
├── __init__.py
├── log_analyzer.py  analyze log, you can add your own rules
├── parser.py
├── __pycache__
├── rpc.py # optional module, rpc helper and executor for servers
├── server_checker.py # server status checker, sql tester, you can add more checks
├── table_checker.py
└── util.py
```

## Subcommands

core commands:
```
status
          [connect] # TODO http ping all servers
inspect   no sub means inspect all
          [online] check all table status
          [offline] offline jobs status
test   test online insert&select, test offline select if taskmanager exists
static-check needs config file(dist.yml or hosts)
              [-V,--version/-C,--conf/-L,--log/-VCL]
rpc    user-friendly rpc tool
```

For example:
```
openmldb_tool status --cluster=127.0.0.1:2181/openmldb
```

`--cluster=127.0.0.1:2181/openmldb` is the default cluster config, so `openmldb_tool status` is ok.

## Status
```
status [-h] [--helpfull] [--diff DIFF]

optional arguments:
  -h, --help   show this help message and exit
  --helpfull   show full help message and exit
  --diff       check if all endpoints in conf are in cluster. If set, need to set `-f,--conf_file`
```

Use `show components` to show servers(no apiserver now).

--conn:
- ping all servers, brpc /health to check ok，and
- online servers version and cost time, we can get from brpc http://<endpoint>/version. (ns,tablet, apiserver set_version in brpc server)

TODO: 
- brpc /flags to get all gflags(including openmldb), `--enable_flags_service=true` required

## Inspect

`inspect` for full report, no offline diag now. 

inspect online: Use `show table status like '%';` in all dbs, even the hidden db(system db).

inspect offline: failed jobs, no more info. TODO: check register table?

inspect job: full support of offline job, select jobs, parse job log

## Test

1. online: create table, insert and select
2. offline: if taskmanager exists, select

## Static Check

Check the onebox/distribute cluster.

1. version: local/ssh run `openmldb --version`
2. conf: copy to local, and check
3. log: read conf in host(local or remote), get the log path, copy logs to local, and check

collector.py collects version, config and log.

TODO: 
- `<cluster-name>-conf` is better than custom dest name?
- more analysis rules of conf and log

### version

1. exec `openmldb --version` to get cxx servers version
2. run jar to get taskmanager and batch

#### find batch jar
find spark home from remote taskmanager config file.

### config
```
<dest>/
  <ip:port>-nameserver/
    nameserver.flags
  <ip:port>-tablet/
    tablet.flags
  <ip:port>-tablet/
    tablet.flags
  <ip:port>-taskmanager/
    taskmanager.properties
```

### log
Find log path in remote config file.

```
<dest>/
  <ip:port>-nameserver/
    nameserver.info.log.1
    nameserver.info.log.2
    ...
  <ip:port>-tablet/
    ...
  <ip:port>-taskmanager/
    taskmanager.log.1
    job_1_error.log
    ...
```

TODO: custom filter, do not copy all logs

### analysis

log_analysis.py read logs from local collection path `<dest>`. 

- show warning logs in `nameserver.info.log`, `tablet.info.log`
- show warning logs and exceptions in `taskmanager.log`

## RPC

Optional module, rpc helper and executor for servers. You can install it by `pip install openmldb[rpc]`. You can execute rpc directly, but if you want rpc hint, you need to download or compile protobuf files in `OpenMLDB/src/proto`.

```bash
cd OpenMLDB
make thirdparty
# install to any dir
.deps/usr/bin/protoc --python_out=$(pwd)/pb2 --proto_path=src/proto/ src/proto/*.proto
```
Then use `openmldb_tool rpc --pbdir=<pb2_dir>` to run rpc commands.
