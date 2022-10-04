# SDK Development Guidelines

## Overview

The OpenMLDB SDK can be divided into several layers, as shown in the figure. The introduction will go from the bottom up.
![sdk layers](images/sdk_layers.png)

### SDK Layer
The lowest layer is the SDK core layer, which is implemented as [SQLClusterRouter](https://github.com/4paradigm/OpenMLDB/blob/b6f122798f567adf2bb7766e2c3b81b633ebd231/src/sdk/sql_cluster_router.h#L110). It is the smallest implement unit of **client**. All operations on OpenMLDB clusters can be done by using the methods of `SQLClusterRouter` after proper configuration.

Developers need to care about the three core methods of this layer.

1. [ExecuteSQL](https://github.com/4paradigm/OpenMLDB/blob/b6f122798f567adf2bb7766e2c3b81b633ebd231/src/sdk/sql_cluster_router.h#L160) supports the execution of all SQL commands, including DDL, DML and DQL.
2. [ExecuteSQLParameterized](https://github.com/4paradigm/OpenMLDB/blob/b6f122798f567adf2bb7766e2c3b81b633ebd231/src/sdk/sql_cluster_router.h#L166)supports parameterized SQL.
3. [ExecuteSQLRequest](https://github.com/4paradigm/OpenMLDB/blob/b6f122798f567adf2bb7766e2c3b81b633ebd231/src/sdk/sql_cluster_router.h#L156)is the special methods for the OpenMLDB specific execution mode: [Online Request mode](../tutorial/modes.md#4-the-online-request-mode).



### Wrapper Layer
Due to the complexity of the implementation of the SDK Layer, we didn't develop the Java and Python SDKs from scratch, but to use Java and Python to call the **SDK Layer**. Specifically, we made a wrapper layer using Swig.

Java Wrapper is implemented as [SqlClusterExecutor](https://github.com/4paradigm/OpenMLDB/blob/main/java/openmldb-jdbc/src/main/java/com/_4paradigm/openmldb/sdk/impl/SqlClusterExecutor.java). It is a simple wrapper of `sql_router_sdk`, including the conversion of input types, the encapsulation of returned results, the encapsulation of returned errors.

Python Wrapper is implemented as [OpenMLDBSdk](https://github.com/4paradigm/OpenMLDB/blob/main/python/openmldb/sdk/sdk.py). Like the Java Wrapper, it is a simple wrapper as well.



### User Layer
Although the Wrapper Layer can be used directly, it is not convenient enough. So, we develop another layer, the User Layer of the Java/Python SDK.

The Java User Layer supports the JDBC. Users can use the JDBC protocol to access OpenMLDB without high access cost. See [jdbc](https://github.com/4paradigm/OpenMLDB/tree/main/java/openmldb-jdbc/src/main/java/com/_4paradigm/openmldb/jdbc) for detail about implementation. 

The Python User Layer supports the sqlalchemy, which can also reduce the users' access cost. See [sqlalchemy_openmldb](https://github.com/4paradigm/OpenMLDB/blob/main/python/openmldb/sqlalchemy_openmldb) and [dbapi](https://github.com/4paradigm/OpenMLDB/blob/main/python/openmldb/dbapi) for detail about implementation. 

## Note

We want an easier to use C++ SDK which doesn't need a Wrapper Layer.
Therefore, in theory, developers only need to design and implement the user layer, which calls the SDK layer.

但考虑到代码复用，可能会一定程度地改动SDK核心层的代码，或者是调整SDK核心代码结构（比如，暴露SDK核心层的部分头文件等）。

## Details of SDK Layer 

由于历史原因，SQLClusterRouter的创建方式有多种。下面一一介绍。
首先是使用两种Option创建，分别会创建连接Cluster和Standalone两种OpenMLDB服务端。
```
    explicit SQLClusterRouter(const SQLRouterOptions& options);
    explicit SQLClusterRouter(const StandaloneOptions& options);
```
这两种常见方式，不会暴露元数据相关的DBSDK，通常给普通用户使用。Java与Python SDK底层也是使用这两种方式。

第三种是基于DBSDK创建：
```
explicit SQLClusterRouter(DBSDK* sdk);
```
DBSDK有分为Cluster和Standalone两种，因此也可连接两种OpenMLDB服务端。
这种方式方便用户额外地读取操作元数据，否则DBSDK在SQLClusterRouter内部不会对外暴露。

例如，由于CLI可以直接通过DBSDK获得nameserver等元数据信息，我们在启动ClusterSQLClient或StandAloneSQLClient时是先创建BDSDK再创建SQLClusterRouter。

## Java Test

If you want to debug only in one submodule, please install the compiled packages since one submodule may depend on other submodules, for example, openmldb-spark-connector depends on openmldb-jdbc.
```
make SQL_JAVASDK_ENABLE=ON

# Or:

cd java
mvn install -DskipTests=true -Dscalatest.skip=true -Dwagon.skip=true -Dmaven.test.skip=true -Dgpg.skip
```

Then run:

```
mvn test -pl openmldb-spark-connector -Dsuites=com._4paradigm.openmldb.spark.TestWrite
```

```{warning}
If you update the codes in real time, the latest codes can not be test since there are `jars` complied from old codes in the local warehouse.
Please be careful when using '-pl'.
```

If you only want to run JAVA testing, try the commands below:
```
mvn test -pl openmldb-jdbc -Dtest="SQLRouterSmokeTest"
mvn test -pl openmldb-jdbc -Dtest="SQLRouterSmokeTest#AnyMethod"
```