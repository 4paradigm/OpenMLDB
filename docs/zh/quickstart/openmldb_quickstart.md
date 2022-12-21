# 快速上手

OpenMLDB 提供[单机版和集群版](../tutorial/standalone_vs_cluster.md)。本文将演示集群版 OpenMLDB 的基本使用流程：建立数据库、导入数据、离线特征计算、SQL 方案上线、在线实时特征计算。单机版使用流程演示可参考[单机版使用流程文档](../tutorial/standalone_use.md)。

## 准备

本文基于 OpenMLDB CLI 进行开发和部署，首先需要下载样例数据并且启动 OpenMLDB CLI。

推荐使用准备好的 Docker 镜像来快速体验。

- Docker（最低版本：18.03）

### 拉取镜像

执行以下命令拉取 OpenMLDB 镜像，并启动 Docker 容器：

```Shell
docker run -it 4pdosc/openmldb:0.6.9 bash
```

成功启动容器以后，本教程中的后续命令默认均在容器内执行。

如果你需要从容器外访问容器内的 OpenMLDB 服务端，请参考[CLI/SDK-容器onebox](../reference/ip_tips.md#clisdk-容器onebox)。

### 下载样例数据

执行以下命令下载后续流程中使用的样例数据：

```Shell
curl https://openmldb.ai/demo/data.parquet --output ./taxi-trip/data/data.parquet
```

### 启动服务端和客户端

- 启动集群版 OpenMLDB 服务端

```Shell
/work/init.sh
```

- 启动集群版 OpenMLDB CLI 客户端

```Shell
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

成功启动集群版 OpenMLDB CLI 后如下图显示：

![image-20220111141358808](./images/cli_cluster.png)

## 使用流程

OpenMLDB 的工作流程一般包含：建立数据库和表、离线数据准备、离线特征计算、SQL 方案上线、在线数据准备、在线实时特征计算六个阶段。

集群版 OpenMLDB 需要分别管理离线数据和在线数据。因此在完成 SQL 方案上线后，必须做在线数据的准备步骤。

以下演示的命令如无特别说明，默认均集群版 OpenMLDB CLI 下执行（CLI 命令以提示符 `>` 开头以作区分）。

### 1. 创建数据库和表

创建数据库 `demo_db` 和表 `demo_table1`：

```Shell
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date);
```

查看数据表 `demo_table1` 数据：

```Shell
> desc demo_table1;
 --- ------- ----------- ------ ---------
  #   Field   Type        Null   Default
 --- ------- ----------- ------ ---------
  1   c1      Varchar     YES
  2   c2      Int         YES
  3   c3      BigInt      YES
  4   c4      Float       YES
  5   c5      Double      YES
  6   c6      Timestamp   YES
  7   c7      Date        YES
 --- ------- ----------- ------ ---------
 --- -------------------- ------ ---- ------ ---------------
  #   name                 keys   ts   ttl    ttl_type
 --- -------------------- ------ ---- ------ ---------------
  1   INDEX_0_1641939290   c1     -    0min   kAbsoluteTime
 --- -------------------- ------ ---- ------ ---------------
```

### 2. 离线数据准备

首先，切换到离线执行模式。在该模式下，只会处理离线数据导入或查询操作。接着，导入之前下载的样例数据作为离线数据，用于离线特征计算。

```Shell
> USE demo_db;
> SET @@execute_mode='offline';
> LOAD DATA INFILE 'file:///work/taxi-trip/data/data.parquet' INTO TABLE demo_table1 options(format='parquet', mode='append');
```

注意，`LOAD DATA`命令为非阻塞命令（请参考 `LOAD DATA` 文档），可以通过 `SHOW JOBS` 等离线任务管理命令来查看任务进度。

如果希望预览数据，用户可以使用 `SELECT * FROM demo_table1` 语句，推荐 `SELECT` 前将离线命令设置为同步模式：

```Shell
SET @@sync_job=true;
-- 如果数据较多容易超时（默认1min），请调大job timeout: SET @@job_timeout=600000;
SELECT * FROM demo_table1;
```

使用非阻塞命令（异步模式）时，通过返回的JOB ID，可以查看任务状态和日志，确保离线特征顺利完成。

```Shell
SHOW JOB $JOB_ID
SHOW JOBLOG $JOB_ID
```

### 3. 离线特征计算

执行 SQL 进行特征抽取，并且将生成的特征存储在一个文件 `feature_data` 中，供后续的模型训练使用：

```Shell
> USE demo_db;
> SET @@execute_mode='offline';
> SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature_data';
```

注意，离线模式 `SELECT INTO` 命令为非阻塞，可以通过 `SHOW JOBS` 等离线任务管理命令来查看运行进度。

### 4. SQL 方案上线

将探索好的 SQL 方案 `demo_data_service` 部署到线上，注意部署上线的 SQL 方案需要与对应的离线特征计算的 SQL 方案保持一致。

```Shell
> SET @@execute_mode='online';
> DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```

上线后可以通过命令 `SHOW DEPLOYMENTS` 查看已部署的 SQL 方案；

```Shell
> SHOW DEPLOYMENTS;
 --------- -------------------
  DB        Deployment
 --------- -------------------
  demo_db   demo_data_service
 --------- -------------------
1 row in set
```

### 5. 在线数据准备

首先，切换到**在线**执行模式。在该模式下，只会处理在线数据导入/插入以及查询操作。接着在在线模式下，导入之前下载的样例数据作为在线数据，用于在线特征计算。

```Shell
> USE demo_db;
> SET @@execute_mode='online';
> LOAD DATA INFILE 'file:///work/taxi-trip/data/data.parquet' INTO TABLE demo_table1 options(format='parquet', header=true, mode='append');
```

注意，`LOAD DATA` 在线模式也是非阻塞命令，可以通过 `SHOW JOBS`等离线任务管理命令来查看运行进度。

等待任务完成以后，预览在线数据：

```Shell
> USE demo_db;
> SET @@execute_mode='online';
> SELECT * FROM demo_table1 LIMIT 10;
 ----- ---- ---- ---------- ----------- --------------- ------------
  c1    c2   c3   c4         c5          c6              c7
 ----- ---- ---- ---------- ----------- --------------- ------------
  aaa   12   22   2.200000   12.300000   1636097890000   1970-01-01
  aaa   11   22   1.200000   11.300000   1636097290000   1970-01-01
  dd    18   22   8.200000   18.300000   1636111690000   1970-01-01
  aa    13   22   3.200000   13.300000   1636098490000   1970-01-01
  cc    17   22   7.200000   17.300000   1636108090000   1970-01-01
  ff    20   22   9.200000   19.300000   1636270090000   1970-01-01
  bb    16   22   6.200000   16.300000   1636104490000   1970-01-01
  bb    15   22   5.200000   15.300000   1636100890000   1970-01-01
  bb    14   22   4.200000   14.300000   1636099090000   1970-01-01
  ee    19   22   9.200000   19.300000   1636183690000   1970-01-01
 ----- ---- ---- ---------- ----------- --------------- ------------
```

用户需要成功完成 SQL 上线部署后，才能准备在线数据，否则会上线失败。

### 6. 退出 CLI

```Shell
> quit;
```

至此，基于集群版 OpenMLDB CLI 的开发部署工作已经全部完成了，并且已经回到了操作系统命令行下。

### 7. 实时特征计算

按照默认的部署配置，apiserver 部署的 http 端口为 9080。

实时线上服务可以通过如下 Web API 提供服务：

```Shell
http://127.0.0.1:9080/dbs/demo_db/deployments/demo_data_service
        \___________/      \____/              \_____________/
              |               |                        |
        APIServer地址     Database名字            Deployment名字
```

实时请求接受 JSON 格式的输入数据。以下将给出两个例子，把一行数据放到请求的 `input` 域中。

示例 1：

```Shell
curl http://127.0.0.1:9080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]}'
```

如下为该查询预期的返回结果（计算得到的特征被存放在 `data` 域）：

```JSON
{"code":0,"msg":"ok","data":{"data":[["aaa",11,22]]}}
```

示例 2：

```Shell
curl http://127.0.0.1:9080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1637000000000, "2021-11-16"]]}'
```

预期返回返回结果：

```JSON
{"code":0,"msg":"ok","data":{"data":[["aaa",11,66]]}}
```

### 实时特征计算的结果说明

实时请求（执行 deployment），是请求模式（request 模式）的 SQL 执行。与批处理模式（batch模式）不同，请求模式只会对请求行（request row）进行 SQL 计算。在前面的示例中，就是 POST 的 input 作为请求行，假设这行数据存在于表demo_table1中，并对它执行SQL：

```SQL
SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```

示例 1 的具体计算逻辑如下（实际计算中会进行优化，减少计算量）：

1. 根据请求行与窗口的`PARTITION BY`分区，筛选出c1为"aaa"的行，并按c6从小到大排序。所以理论上，分区排序后的中间数据表，如下表所示。其中，请求行为排序后的第一行。

      ```SQL
      ----- ---- ---- ---------- ----------- --------------- ------------
      c1    c2   c3   c4         c5          c6              c7
      ----- ---- ---- ---------- ----------- --------------- ------------
      aaa   11   22   1.2        1.3         1635247427000   2021-05-20
      aaa   11   22   1.200000   11.300000   1636097290000   1970-01-01
      aaa   12   22   2.200000   12.300000   1636097890000   1970-01-01
      ----- ---- ---- ---------- ----------- --------------- ------------
      ```

2. 窗口范围是 `2 PRECEDING AND CURRENT ROW`，所以我们在上表中截取出真正的窗口，请求行就是最小的一行，往前2行都不存在，但窗口包含当前行，因此，窗口只有请求行这一行。
3. 窗口聚合，对窗口内的数据（仅一行）进行 c3 求和，得到 22。于是输出结果为：

      ```SQL
      ----- ---- ----------- 
      c1    c2   w1_c3_sum   
      ----- ---- -----------
      aaa   11      22
      ----- ---- -----------
      ```

示例 2 的具体计算逻辑如下：

1. 根据请求行与窗口的 `PARTITION BY` 分区，筛选出 c1 为"aaa"的行，并按 c6 从小到大排序。所以理论上，分区排序后的中间数据表，如下表所示。其中，请求行为排序后的最后一行。

      ```SQL
      ----- ---- ---- ---------- ----------- --------------- ------------
      c1    c2   c3   c4         c5          c6              c7
      ----- ---- ---- ---------- ----------- --------------- ------------
      aaa   11   22   1.200000   11.300000   1636097290000   1970-01-01
      aaa   12   22   2.200000   12.300000   1636097890000   1970-01-01
      aaa   11   22   1.2        1.3         1637000000000   2021-11-16
      ----- ---- ---- ---------- ----------- --------------- ------------
      ```

2. 窗口范围是 `2 PRECEDING AND CURRENT ROW`，所以我们在上表中截取出真正的窗口，请求行往前两行都均存在，同时也包含当前行，因此，窗口内有三行数据。
3. 窗口聚合，对窗口内的数据（三行）进行 c3 求和，得到 22*3=66。于是输出结果为：

      ```SQL
      ----- ---- ----------- 
      c1    c2   w1_c3_sum   
      ----- ---- -----------
      aaa   11      66
      ----- ---- -----------
      ```
