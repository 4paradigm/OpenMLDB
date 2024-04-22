# Authentication

## ZooKeeper Authentication

OpenMLDB 0.8.4 and later versions support ZooKeeper with passwords. You need to configure the username and password of ZooKeeper in the configuration files of the components when starting the cluster.

- Configure `zookeeper.cert=user:passwd` in TaskManager
- Configure `--zk_cert=user:passwd` in other servers
- Configure `--zk_cert=user:passwd` in CLI
- Configure `SdkOption.setZkCert("user:passwd")` in JAVA SDK or URL `jdbc:openmldb:///?...&zkCert=user:passwd`
- Configure `openmldb.dbapi.connect(..., zkCert="user:passwd")` in PYTHON SDK or URL `openmldb:///?...&zkCert=user:passwd`

## OpenMLDB Authentication

OpenMLDB 0.8.5 and later versions support username and password authentication in the cluster. Custom user and password creation is performed after the cluster is started. The initial cluster has only the root user with an empty password. By default, all servers and clients connect to the cluster using the root username and an empty password.

For security reasons, we usually need the root user to use a password and then create ordinary users. Due to the architecture, some servers also connect to the cluster as clients. If you need to change the root password, pay attention to these servers.

### Change the Root Password

We usually need to change the root password to prevent clients from connecting to the cluster without a password. When the root has no password, use the CLI or SDK to connect to the OpenMLDB cluster and change the password using SQL:
```sql
alter user root set options (password='123456');
```
The change does not affect the current connection. New connections using the root user require a password. For how to add a password to the CLI or SDK, see the [corresponding usage documentation](../quickstart/). Pay special attention to the following two components in the server:

- TaskManager
Some services of TaskManager need to connect to OpenMLDB as a client. Therefore, when the root user needs a password, TaskManager needs to be configured with `user=root` and `password=123456`. You can also specify another user for TaskManager, which takes effect after a restart.

- APIServer
APIServer also connects to OpenMLDB as a client. Therefore, when the root user needs a password, APIServer needs to be configured with `--user=root` and `--password=123456`. You can also specify another user for APIServer, which takes effect after a restart.

### Add a User

Add users for ordinary users using SQL:
```sql
CREATE USER user1 OPTIONS (password='123456');
```

Creating a user does not affect server components that authenticate with the root user. If you choose to use a non-root user for TaskManager or APIServer, update their configurations promptly after modifying or deleting this user to avoid connection problems after a service restart.
