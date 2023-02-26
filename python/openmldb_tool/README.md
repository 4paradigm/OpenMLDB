# Diag Tool

In `diagnostic_tool/`:

main: diagnose.py

collect version/config/logs by collector.py (local or remote ssh/scp)

read distribution yaml/hosts by dist_conf.py

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
  --diff       check if all endpoints in conf are in cluster. If set, need to set `--conf_file`
```

Use `show components` to show servers(no apiserver now).

TODO: online servers version, we can get from brpc http://<endpoint>/version. (ns,tablet, apiserver set_version in brpc server)
brpc /health to check ok
brpc /flags to get all gflags(including openmldb), `--enable_flags_service=true` required

## Inspect

Use `show table status` in all dbs, even the hidden db(system db).

If you found some online tables are not behaving properly, do inspect online.

## Test

1. online: create table, insert and select
2. offline: if taskmanager exists, select

## Static Check

Check the onebox/distribute cluster.

1. version: local/ssh run `openmldb --version`
2. conf: copy to local, and check
3. log: read conf in host(local or remote), get the log path, copy logs to local, and check

collector.py collects version, config and log.

TODO: `<cluster-name>-conf` is better than custom dest name?

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

Get last 2 files.

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

### analysis

log_analysis.py read logs from local path `<dest>`. 

NOTE: if diag local cluster/standalone, directory structure is different.
