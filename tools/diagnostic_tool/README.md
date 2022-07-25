main: diagnose.py

ssh/scp by connections.py

read distribution yaml by dist_conf.py

collector.py collects config, log, ...

config:
```
<cluster-name>-conf/
  <ip:port>-ns/
    nameserver.flags
  <ip:port>-tablet1/
    tablet.flags
  <ip:port>-tablet2/
    tablet.flags
  <ip:port>-taskmanager1/
    taskmanager.properties
```

log:
```
<cluster-name>-log/
  <ip:port>-ns/
    log.INFO.1
    log.INFO.2
    ...
  <ip:port>-tablet
  <ip:port>-taskmanager
```

versions:
exec ...


log_analysis.py read logs from local `<cluster-name>-log`
