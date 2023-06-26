# 出租车行程时间预测 (OpenMLDB + LightGBM)

本文将以 [Kaggle 上的出租车行车时间预测问题](https://www.kaggle.com/c/nyc-taxi-trip-duration/overview)为例，示范如何使用 OpenMLDB 和 LightGBM 联合来打造一个完整的机器学习应用。

注意，本文档使用的是预编译好的 Docker 镜像。如果希望在自己编译和搭建的 OpenMLDB 环境下进行测试，需要配置使用[面向特征工程优化的 Spark 发行版](https://openmldb.ai/docs/zh/main/tutorial/openmldbspark_distribution.html)。请参考[针对 OpenMLDB 优化的 Spark 发行版文档](../tutorial/openmldbspark_distribution.md#openmldb-spark-发行版)和[安装部署文档](../deploy/install_deploy.md#2-修改配置文件conftaskmanagerproperties)。

## 准备和预备知识

本文基于 OpenMLDB CLI 进行开发和部署，首先需要下载样例数据并且启动 OpenMLDB CLI。推荐使用 Docker 镜像来快速体验。

- Docker 版本：>= 18.03

### 拉取镜像

在命令行执行以下命令拉取 OpenMLDB 镜像，并启动 Docker 容器：

```bash
docker run -it 4pdosc/openmldb:0.8.1 bash
```

该镜像预装了OpenMLDB，并预置了本案例所需要的所有脚本、三方库、开源工具以及训练数据。

```{note}
注意，本教程以下的 OpenMLDB 部分的演示命令默认均在启动的 Docker 容器内运行。
```

### 初始化环境

```bash
./init.sh
cd taxi-trip
```
镜像内提供的 init.sh 脚本帮助用户快速初始化环境，包括：

- 配置 zookeeper
- 启动集群版 OpenMLDB

### 启动 OpenMLDB CLI

```bash
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

### 预备知识：异步任务

OpenMLDB 部分命令是异步的，如：在线/离线模式的 `LOAD DATA`、`SELECT`、`SELECT INTO` 命令。提交任务以后可以使用相关命令如 `SHOW JOBS`、`SHOW JOB` 来查看任务进度，详情参见[离线任务管理文档](../openmldb_sql/task_manage/index.rst)。

## 机器学习全流程

### 步骤 1：创建数据库和表

创建数据库 `demo_db` 和数据表 `t1`：

```sql
--OpenMLDB CLI
CREATE DATABASE demo_db;
USE demo_db;
CREATE TABLE t1(id string, vendor_id int, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count int, pickup_longitude double, pickup_latitude double, dropoff_longitude double, dropoff_latitude double, store_and_fwd_flag string, trip_duration int);
```

### 步骤 2：导入离线数据

首先，切换到离线执行模式。接着，导入样例数据 `/work/taxi-trip/data/taxi_tour_table_train_simple.snappy.parquet` 作为离线数据，用于离线特征计算。

```sql
--OpenMLDB CLI
USE demo_db;
SET @@execute_mode='offline';
LOAD DATA INFILE '/work/taxi-trip/data/taxi_tour_table_train_simple.snappy.parquet' INTO TABLE t1 options(format='parquet', header=true, mode='append');
```
```{note}
`LOAD DATA` 为异步任务，请使用命令 `SHOW JOBS` 查看任务运行状态，等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。
```

### 步骤 3：特征设计

通常在设计特征前，用户需要根据机器学习的目标对数据进行分析，然后根据分析设计和调研特征。然而，机器学习的数据分析和特征研究并不是本文讨论的范畴。本文假定用户具备机器学习的基本理论知识，有解决机器学习问题的能力，能够理解 SQL 语法，并能够使用 SQL 语法构建特征。针对本案例，假设用户经过分析和调研设计了以下若干特征：

| 特征名          | 特征含义                                                  | SQL特征表示                             |
| --------------- | --------------------------------------------------------- | --------------------------------------- |
| trip_duration   | 单次行程的行车时间                                        | `trip_duration`                         |
| passenger_count | 乘客数                                                    | `passenger_count`                       |
| vendor_sum_pl   | 最近1天时间窗口内，同品牌出租车的累计pickup_latitude      | `sum(pickup_latitude) OVER w`           |
| vendor_max_pl   | 最近1天时间窗口内，同品牌出租车的最大pickup_latitude      | `max(pickup_latitude) OVER w`           |
| vendor_min_pl   | 最近1天时间窗口内，同品牌出租车的最小pickup_latitude      | `min(pickup_latitude) OVER w`           |
| vendor_avg_pl   | 最近1天时间窗口内，同品牌出租车的平均pickup_latitude      | `avg(pickup_latitude) OVER w`           |
| pc_sum_pl       | 最近1天时间窗口内，相同载客量trips的累计pickup_latitude   | `sum(pickup_latitude) OVER w2`          |
| pc_max_pl       | 最近1天时间窗口内，相同载客量trips的的最大pickup_latitude | `max(pickup_latitude) OVER w2`          |
| pc_min_pl       | 最近1天时间窗口内，相同载客量trips的的最小pickup_latitude | `min(pickup_latitude) OVER w2`          |
| pc_avg_pl       | 最近1天时间窗口内，相同载客量trips的平均pickup_latitude   | `avg(pickup_latitude) OVER w2`          |
| pc_cnt          | 最近1天时间窗口内，相同载客量trips总数                    | `count(vendor_id) OVER w2`              |
| vendor_cnt      | 最近1天时间窗口内，同品牌出租车trips总数                  | `count(vendor_id) OVER w AS vendor_cnt` |

在实际的机器学习特征调研过程中，科学家对特征进行反复试验，寻求模型效果最好的特征集。所以会不断地重复多次“特征设计->离线特征抽取->模型训练”过程，并不断调整特征以达到预期效果。

### 步骤 4：离线特征抽取

用户在离线模式下，进行特征抽取，并将特征结果输出到 `/tmp/feature_data` 目录下保存，以供后续的模型训练。 `SELECT` 命令对应了基于上述特征设计所产生的 SQL 特征计算脚本。

```sql
--OpenMLDB CLI
USE demo_db;
SET @@execute_mode='offline';
SELECT trip_duration, passenger_count,
sum(pickup_latitude) OVER w AS vendor_sum_pl,
max(pickup_latitude) OVER w AS vendor_max_pl,
min(pickup_latitude) OVER w AS vendor_min_pl,
avg(pickup_latitude) OVER w AS vendor_avg_pl,
sum(pickup_latitude) OVER w2 AS pc_sum_pl,
max(pickup_latitude) OVER w2 AS pc_max_pl,
min(pickup_latitude) OVER w2 AS pc_min_pl,
avg(pickup_latitude) OVER w2 AS pc_avg_pl,
count(vendor_id) OVER w2 AS pc_cnt,
count(vendor_id) OVER w AS vendor_cnt
FROM t1
WINDOW w AS (PARTITION BY vendor_id ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW),
w2 AS (PARTITION BY passenger_count ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature_data';
```

```{note}
`SELECT INTO` 为异步任务，请使用命令 `SHOW JOBS` 查看任务运行状态，等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。
```

### 步骤 5：模型训练

1. 模型训练不在 OpenMLDB 内完成，因此首先通过以下 `quit` 命令退出 OpenMLDB CLI。

    ```
    quit;
    ```

2. 在普通命令行下，执行 train.py（`/work/taxi-trip` 目录中），使用开源训练工具 `LightGBM` 基于上一步生成的离线特征表进行模型训练，训练结果存放在 `/tmp/model.txt` 中。

    ```bash
    python3 train.py /tmp/feature_data /tmp/model.txt
    ```

### 步骤 6：特征抽取 SQL 脚本上线

假定[步骤 3 所设计的特征](#步骤-3特征设计)在上一步的模型训练中产出的模型符合预期，那么下一步就是将该特征抽取 SQL 脚本部署到线上去，以提供在线特征抽取服务。

1. 重新启动 OpenMLDB CLI，以进行 SQL 上线部署：

   ```bash
   /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
   ```

2. 执行上线部署：

    ```sql
    --OpenMLDB CLI
    USE demo_db;
    SET @@execute_mode='online';
    DEPLOY demo SELECT trip_duration, passenger_count,
    sum(pickup_latitude) OVER w AS vendor_sum_pl,
    max(pickup_latitude) OVER w AS vendor_max_pl,
    min(pickup_latitude) OVER w AS vendor_min_pl,
    avg(pickup_latitude) OVER w AS vendor_avg_pl,
    sum(pickup_latitude) OVER w2 AS pc_sum_pl,
    max(pickup_latitude) OVER w2 AS pc_max_pl,
    min(pickup_latitude) OVER w2 AS pc_min_pl,
    avg(pickup_latitude) OVER w2 AS pc_avg_pl,
    count(vendor_id) OVER w2 AS pc_cnt,
    count(vendor_id) OVER w AS vendor_cnt
    FROM t1
    WINDOW w AS (PARTITION BY vendor_id ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW),
    w2 AS (PARTITION BY passenger_count ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW);
    ```

### 步骤 7：导入在线数据

首先，请切换到**在线**执行模式。接着在在线模式下，导入样例数据 `/work/taxi-trip/data/taxi_tour_table_train_simple.csv` 作为在线数据，用于在线特征计算。

```sql
--OpenMLDB CLI
USE demo_db;
SET @@execute_mode='online';
LOAD DATA INFILE 'file:///work/taxi-trip/data/taxi_tour_table_train_simple.csv' INTO TABLE t1 options(format='csv', header=true, mode='append');
```

```{note}
`LOAD DATA` 为异步任务，请使用命令 `SHOW JOBS` 查看任务运行状态，等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作。
```

### 步骤 8：启动预估服务

1. 如果尚未退出 OpenMLDB CLI，先退出 OpenMLDB CLI。

    ```
    quit;
    ```
2. 在普通命令行下启动预估服务：

    ```bash
    ./start_predict_server.sh 127.0.0.1:9080 /tmp/model.txt
    ```
### 步骤 9：发送预估请求

在普通命令行下执行内置的 `predict.py` 脚本。该脚本发送一行请求数据到预估服务，接收返回的预估结果，并打印出来。

```bash
# Run inference with a HTTP request
python3 predict.py
# The following output is expected (the numbers might be slightly different)
----------------ins---------------
[[ 2.       40.774097 40.774097 40.774097 40.774097 40.774097 40.774097
  40.774097 40.774097  1.        1.      ]]
---------------predict trip_duration -------------
848.014745715936 s
```
