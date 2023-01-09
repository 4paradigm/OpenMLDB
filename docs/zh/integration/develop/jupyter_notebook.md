# Jupyter Notebook

Jupyter Notebook 提供了基于浏览器网页的数据计算、代码开发、文档编辑、代码执行以及结果展示等功能，是目前最流行和最易用的开发环境之一。本篇文章介绍 OpenMLDB 与 Notebook 的深度整合，充分利用 OpenMLDB 的功能特性和 Notebook 的便利性，从而打造一个快捷易用的机器学习开发环境。

## 集成 SQL 魔法函数

Notebook 与 OpenMLDB 第一个集成点就是 SQL 魔法函数 (magic function)。魔法函数是 Notebook 的拓展功能，通过注册魔法函数，用户可以在 Notebook cell 中直接执行 SQL 命令，而不需要编写复杂的 Python 代码，并且可定制输出样式。OpenMLDB 提供了一个标准的 SQL 魔法函数，用户可以在 Notebook 上直接编写和运行 OpenMLDB 支持的 SQL 语句，对应的语句就会提交到 OpenMLDB 执行，并且在 Notebook 中预览返回到结果。

### 注册 OpenMLDB SQL 魔法函数

- 为了在 Notebook 中支持 OpenMLDB 魔法函数，首先需要通过如下方式进行注册：

- ```Python
  import openmldb
  db = openmldb.dbapi.connect('demo_db','0.0.0.0:2181','/openmldb')
  openmldb.sql_magic.register(db)
  ```

### 执行单行 SQL 语句

开发者可以使用提示符 `%` 来执行单行的 SQL 语句，如下图所示。

![img](https://openmldb.feishu.cn/space/api/box/stream/download/asynccode/?code=MWQ4YzAwM2UzOTIwY2JjZjljMmY2MjI3MGExMDc5YjJfTHVjV3hsNUltSTd4Rll5Nmd4VXJjWHFqSG5oRUEwZW9fVG9rZW46Ym94Y25CbE9vSVBaeUJKcU9oM0p3QnNtN0NoXzE2NzMyNTQ3Mjc6MTY3MzI1ODMyN19WNA)

### 执行多行 SQL 语句

开发者也可以用提示符 `%%`，书写多行的 SQL 语句，如下图所示。

![img](https://openmldb.feishu.cn/space/api/box/stream/download/asynccode/?code=MDkwMjkwZGNkZGU2NDNmOTU3OTI5MjQyYzIyZjhlMDJfcnh5ZXBuaUlEM0ExdnhnR3o5ZUNpUDNrM2JZMDJQdUlfVG9rZW46Ym94Y25oZzk5TkdLenVMalozaktkR3E5ZlJoXzE2NzMyNTQ3Mjc6MTY3MzI1ODMyN19WNA)

注意，目前尚不支持在一个 Notebook cell 内部同时执行多条 SQL 语句，需要分开在不同 cell 内执行。

### 魔法函数支持功能

OpenMLDB 提供的 SQL 魔法函数，可执行所有支持的 SQL 语法，包括使用 OpenMLDB 特有的离线模式，把复杂的需要处理大数据 SQL 语句异步提交到离线执行引擎去执行，如下图所示。

![img](https://openmldb.feishu.cn/space/api/box/stream/download/asynccode/?code=NDdkODE2MTBhNmFlYzNjZmU0ZjQzZDgwMWM1YmYzNDNfdVVhRjhYNm4wQTlveDY0dGxOaUw3OUlydEVqdGpvTkhfVG9rZW46Ym94Y25sV0FSMTVhZWtmM2hYTXN6Zml4aDhjXzE2NzMyNTQ3Mjc6MTY3MzI1ODMyN19WNA)

详细的 OpenMLDB 魔法函数使用方法，请参考[使用 Notebook Magic Function](https://openmldb.ai/docs/zh/main/quickstart/sdk/python_sdk.html#notebook-magic-function)。

## Notebook 集成 OpenMLDB Python SDK

OpenMLDB 和 Notebook 整合的第二个功能点就是与 OpenMLDB Python SDK 的集成。Notebook 支持 Python 运行内核，因此可以通过 import 方式导入各种 Python 库使用，OpenMLDB 提供了功能完整的 Python SDK，用于在 Notebook 内调用。OpenMLDB 不仅提供了基于 Python PEP249 标准的 DBAPI，也支持了 Python 业界主流的 SQLAlchemy 接口，只需一行代码就可以连接已有的 OpenMLDB 集群。

### 使用 OpenMLDB DBAPI

使用 DBAPI 接口非常简单，只需指定 ZooKeeper 地址以及节点路径即可连接，连接成功后会有对应的日志信息。即可在 Notebook 内调用 OpenMLDB Python SDK 的 DBAPI 接口进行开发，详见[使用 OpenMLDB DBAPI](https://openmldb.ai/docs/zh/main/quickstart/sdk/python_sdk.html#openmldb-dbapi)。

```Python
import openmldb.dbapi
db = openmldb.dbapi.connect('demo_db','0.0.0.0:2181','/openmldb')
```

### 使用 OpenMLDB SQLAlchemy

如果想使用 SQLAlchemy 也非常简单，通过 SQLAlchemy 库指定 OpenMLDB 的 URI 即可完成连接；同样可以通过 IP 和端口作为参数连接单机版 OpenMLDB 数据库，如下所示。

```Python
import sqlalchemy as db
engine = db.create_engine('openmldb://demo_db?zk=127.0.0.1:2181&zkPath=/openmldb')
connection = engine.connect()
```

连接成功后，即可以通过 OpenMLDB Python SDK 的 SQLAlchemy 接口来进行开发，详见[使用 OpenMLDB SQLAlchemy](https://openmldb.ai/docs/zh/main/quickstart/sdk/python_sdk.html#openmldb-sqlalchemy)。
