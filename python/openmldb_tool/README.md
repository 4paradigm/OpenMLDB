# Diag Tool

In `diagnostic_tool/`:

main: diagnose.py

ssh/scp by connections.py

read distribution yaml by dist_conf.py

## Collector

collector.py collects config, log and version

TODO: `<cluster-name>-conf` is better than custom dest name?

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

### version

exec openmldb

run jar taskmanager and batch

#### find batch jar
find spark home from remote taskmanager config file.

## analysis

log_analysis.py read logs from local path `<dest>`. 

NOTE: if diag local cluster/standalone, directory structure is different.
