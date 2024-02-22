# SHOW COMPONENTS
`SHOW COMPONENTS` is used to show the information of components.

```sql
SHOW COMPONENTS;
```

## Output Information

| Column       | Note                                                                                                                                                                                              |
| ------------ |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Endpoint     | It shows the endpoint of the component by providing the IP and the port, which is the same as the `--endpoint` flag in configuration files.                                                       |
| Role         | It indicates the role of the component, which is the same as the `--role` flag in configuration files. <br/> There are four types of roles: `tablet`, `nameserver`, `taskmanager` and `apiserver`. |
| Connect_time | It shows the timestamp (in milliseconds) of connection establishment of the component.                                                                                                                    |
| Status       | It shows the status of the component. There are three kinds of status: `online`, `offline` and `NULL`.                                                                                            |
| Ns_role      | It shows the role of the Namserver: `master` or `standby`. For other components, Ns_role is `NULL`.                                                                                           |


```{note}
Currently, there are certain limitations of `SHOW COMPONETS`:
- It does not include the information of the APIServer.
- It can only shows the information of one leader task manager, but is not working for followers.
- The `Connect_time` of nameserver in tha standalone version is inaccurate.  
```
## Example

```sql
SHOW COMPONENTS;
 ---------------- ------------ --------------- -------- --------- 
  Endpoint         Role         Connect_time    Status   Ns_role  
 ---------------- ------------ --------------- -------- --------- 
  127.0.0.1:9520   tablet       1654759517890   online   NULL     
  127.0.0.1:9521   tablet       1654759517942   online   NULL     
  127.0.0.1:9522   tablet       1654759517919   online   NULL     
  127.0.0.1:9622   nameserver   1654759519015   online   master   
  127.0.0.1:9623   nameserver   1654759521016   online   standby  
  127.0.0.1:9624   nameserver   1654759523030   online   standby  
 ---------------- ------------ --------------- -------- --------- 
```

