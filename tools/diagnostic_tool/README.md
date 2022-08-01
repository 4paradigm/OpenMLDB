main: diagnose.py

ssh/scp by connections.py

read distribution yaml by dist_conf.py

collector.py collects config, log, ...

TODO: `<cluster-name>-conf` is better than custom dest name?

config:
```
<dest>/
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
<dest>/
  <ip:port>-ns/
    log.INFO.1
    log.INFO.2
    ...
  <ip:port>-tablet
  <ip:port>-taskmanager
```

versions:
exec ...

TODO: taskmanager version(not only the leader)
 
how to get one taskmanager version in local?

log_analysis.py read logs from local path `<dest>`.
