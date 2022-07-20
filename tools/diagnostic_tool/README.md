main: diagnose.py

ssh/scp by connections.py

read distribution yaml by config_reader.py

collector.py collects config, log, ...
```
<cluster-name>-log/
  <ip:port>-ns/
    log.INFO.1
    log.INFO.2
    ...
  <ip:port>-tablet
  <ip:port>-taskmanager
```
log_analysis.py read logs from local `<cluster-name>-log`
