# 身份认证

## ZooKeeper认证

OpenMLDB 0.8.4版本及以后，可以使用有密码的ZooKeeper。需要在启动集群时在各个组件的配置文件中配置ZooKeeper的用户名密码。

- TaskManager中配置`zookeeper.cert=user:passwd`
- 其他Server 配置`--zk_cert=user:passwd`
- CLI 配置`--zk_cert=user:passwd`
- JAVA SDK 配置 `SdkOption.setZkCert("user:passwd")`或 URL `jdbc:openmldb:///?...&zkCert=user:passwd`
- PYTHON SDK 配置`openmldb.dbapi.connect(..., zkCert="user:passwd")`或 URL `openmldb:///?...&zkCert=user:passwd`

## OpenMLDB身份认证

### 0.9.0 中的 Alpha 功能
OpenMLDB 在 0.9.0 版本中作为 alpha 功能引入了服务器端身份验证。 从此版本开始，默认情况下不启用服务器端身份验证。 这种方法可确保向后兼容性并最大限度地降低破坏现有部署的风险。 用户可以通过在启动客户端和服务器时配置“--noskip_grant_tables”来启用服务端认证。 禁用身份验证时，不支持更改身份验证设置或修改用户凭据的语句，例如“CREATE USER”、“ALTER USER”和“DELETE USER”。

### 0.8.5 中的认证
OpenMLDB 0.8.5版本集群内可以使用用户名和密码进行身份认证。自定义用户和密码的创建都在集群启动之后，初始集群只有root用户，密码为空。默认情况下，任何的server和client都是以root用户名和空密码连接到集群。

为了安全，我们通常需要root用户使用密码，再创建普通用户。而由于架构上部分server也会作为client连接集群，如果修改root密码需要注意。

### 修改root密码

我们通常是需要修改root密码的，避免client可以在无密码时就能连接上集群。在root无密码时，使用CLI或SDK连接OpenMLDB集群，通过SQL修改密码：
```sql
alter user root set options (password='123456');
```
修改后并不影响当前连接继续使用。使用root用户的新连接需要密码，CLI或SDK如何添加密码参考[相应的使用文档](../quickstart/)。Server中需要特别注意两个组件：

- TaskManager
TaskManager的部分服务需要以client的形式连接OpenMLDB，因此，当root需要密码后，TaskManager需要配置`user=root`和`password=123456`。也可以指定TaskManager使用别的用户，重启生效。

- APIServer
APIServer也是以client形式连接OpenMLDB，因此，当root需要密码后，APIServer需要配置`--user=root`和`--password=123456`。也可以指定APIServer使用别的用户，重启生效。

### 增加用户

增加的用户给普通用户使用，使用SQL创建：
```sql
CREATE USER user1 OPTIONS (password='123456');
```

保持OpenMLDB的server组件以root用户进行认证的，创建用户对它们不会造成任何影响。如果你选择让TaskManager或APIServer使用非root用户，修改删除此用户后也要及时更新它们的配置，避免服务重启后连接不上。
