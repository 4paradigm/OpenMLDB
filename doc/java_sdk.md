# Java SDK 文档

## sdk maven版本

sdk目前发布到公司的仓库

```
    <dependency>
		<groupId>com._4paradigm</groupId>
		<artifactId>rtidb-client</artifactId>
		<version>1.2.0</version>
    </dependency>
```

## 单机版本sdk使用说明

### 通过代码初始化链接例子

```
// 初始一个客户端链接到TabletServer
// 参数1 为ip地址
// 参数2 为端口号
// 参数3 超时时间
// 参数4 为io线程数
RpcClient rpcClient = TabletClientBuilder.buildRpcClient("127.0.0.1", 9501, 100000, 3);
```
注意：一个TabletServer全局初始化一个RpcClient就行了

### 通过spring配置初始化链接例子

```

```

### 通过代码初始化一个同步客户端

```
RpcClient rpcClient = TabletClientBuilder.buildRpcClient("127.0.0.1", 9501, 100000, 3);
// 从rpc client里面创建一个同步客户端
TabletSyncClient syncClient = TabletClientBuilder.buildSyncClient(rpcClient);
```

### 通过代码初始化一个异步客户端

```
RpcClient rpcClient = TabletClientBuilder.buildRpcClient("127.0.0.1", 9501, 100000, 3);
// 从rpc client里面创建一个异步客户端
TabletAsyncClient asyncClient = TabletClientBuilder.buildAsyncClient(rpcClient); 
```

