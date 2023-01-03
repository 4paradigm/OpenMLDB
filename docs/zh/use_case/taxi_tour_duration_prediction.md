#  OpenMLDB + LightGBM：出租车行程时间预测

本文我们将以[Kaggle上的出租车行车时间预测问题为例](https://www.kaggle.com/c/nyc-taxi-trip-duration/overview)，示范如何使用[OpenMLDB](https://github.com/4paradigm/OpenMLDB)和 LightGBM 联合来打造一个完整的机器学习应用。本案例基于 OpenMLDB 集群版进行教程演示。

注意，本文档使用的是预编译好的 docker 镜像。如果希望在自己编译和搭建的 OpenMLDB 环境下进行测试，需要配置使用我们[面向特征工程优化的 Spark 发行版](../tutorial/openmldbspark_distribution.md)。请参考相关[编译](../deploy/compile.md)（参考章节：“针对OpenMLDB优化的Spark发行版”）和[安装部署文档](../deploy/install_deploy.md)（参考章节：“部署TaskManager” - “2 修改配置文件conf/taskmanager.properties”）。

## 1. 环境准备和预备知识

### 1.1 拉取和启动 OpenMLDB Docker 镜像

- 注意，请确保 Docker Engine 版本号 >= 18.03
- 拉取 OpenMLDB docker 镜像，并且运行相应容器：

```bash
docker run -it 4pdosc/openmldb:0.7.0 bash
```
该镜像预装了OpenMLDB，并预置了本案例所需要的所有脚本、三方库、开源工具以及训练数据。

```{note}
注意，本教程以下的所有演示命令默认均在该已经启动的 docker 容器内运行。
```

### 1.2 初始化环境

```bash
./init.sh
cd taxi-trip
```
我们在镜像内提供了init.sh脚本帮助用户快速初始化环境，包括：

- 配置 zookeeper
- 启动集群版 OpenMLDB

### 1.3  启动 OpenMLDB CLI 客户端

```bash
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```
```{note}
注意，本教程大部分命令在 OpenMLDB CLI 下执行，为了跟普通 shell 环境做区分，在 OpenMLDB CLI 下执行的命令均使用特殊的提示符 `>` 。
```

### 1.4 预备知识：集群版的非阻塞任务

集群版的部分命令是非阻塞任务，包括在线模式的 `LOAD DATA`，以及离线模式的 `LOAD DATA` ，`SELECT`，`SELECT INTO` 命令。提交任务以后可以使用相关的命令如 `SHOW JOBS`, `SHOW JOB` 来查看任务进度，详情参见[离线任务管理文档](../openmldb_sql/task_manage)。

## 2. 机器学习全流程

### 2.1 创建数据库和数据表

以下命令均在 OpenMLDB CLI 环境下执行。

```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE t1(id string, vendor_id int, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count int, pickup_longitude double, pickup_latitude double, dropoff_longitude double, dropoff_latitude double, store_and_fwd_flag string, trip_duration int);
```

### 2.2 离线数据准备

首先，切换到离线执行模式。接着，导入样例数据 `/work/taxi-trip/data/taxi_tour_table_train_simple.csv` 作为离线数据，用于离线特征计算。

以下命令均在 OpenMLDB CLI 下执行。

```sql
> USE demo_db;
> SET @@execute_mode='offline';
> LOAD DATA INFILE '/work/taxi-trip/data/taxi_tour_table_train_simple.snappy.parquet' INTO TABLE t1 options(format='parquet', header=true, mode='append');
```
```{note}
注意，集群版 `LOAD DATA` 为非阻塞任务，可以使用命令 `SHOW JOBS` 查看任务运行状态，请等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。
```

### 2.3 特征设计

通常在设计特征前，用户需要根据机器学习的目标对数据进行分析，然后根据分析设计和调研特征。机器学习的数据分析和特征研究不是本文讨论的范畴，我们将不作展开。本文假定用户具备机器学习的基本理论知识，有解决机器学习问题的能力，能够理解SQL语法，并能够使用SQL语法构建特征。

针对本案例，用户经过分析和调研设计了若干特征：

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

请注意，在实际的机器学习特征调研过程中，科学家对特征进行反复试验，寻求模型效果最好的特征集。所以会不断的重复多次[特征设计](#2.3-特征设计)->[离线特征抽取](#2.4-离线特征抽取)->[模型训练](#2.5-模型训练)过程，并不断调整特征以达到预期效果。

### 2.4 离线特征抽取

用户在离线模式下，进行特征抽取，并将特征结果输出到`/tmp/feature_data`目录下保存，以供后续的模型训练。 `SELECT` 命令对应了基于上述特征设计所产生的 SQL 特征计算脚本。以下命令均在 OpenMLDB CLI 下执行。

```sql
> USE demo_db;
> SET @@execute_mode='offline';
> SELECT trip_duration, passenger_count,
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
注意，集群版 `SELECT INTO` 为非阻塞任务，可以使用命令 `SHOW JOBS` 查看任务运行状态，请等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。
```

### 2.5 模型训练

1. 模型训练不在 OpenMLDB 内完成，因此首先通过以下 `quit` 命令退出 OpenMLDB CLI。

```
> quit
```

2. 在普通命令行下，执行 train.py(`/work/taxi-trip`目录中)，使用开源训练工具 `lightgbm` 基于上一步生成的离线特征表进行模型训练，训练结果存放在 `/tmp/model.txt`中。

```bash
python3 train.py /tmp/feature_data /tmp/model.txt
```
### 2.6 特征抽取SQL脚本上线

假定[2.3节中所设计的特征](#2.3-特征设计)在上一步的模型训练中产出的模型符合预期，那么下一步就是将该特征抽取SQL脚本部署到线上去，以提供在线的特征抽取。

1. 重新启动 OpenMLDB CLI，以进行 SQL 上线部署

   ```bash
   /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
   ```

2. 执行上线部署，以下命令在 OpenMLDB CLI 内执行。

```sql
> USE demo_db;
> SET @@execute_mode='online';
> DEPLOY demo SELECT trip_duration, passenger_count,
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
### 2.7 在线数据准备

首先，请切换到**在线**执行模式。接着在在线模式下，导入样例数据 `/work/taxi-trip/data/taxi_tour_table_train_simple.csv` 作为在线数据，用于在线特征计算。以下命令均在 OpenMLDB CLI 下执行。

```sql
> USE demo_db;
> SET @@execute_mode='online';
> LOAD DATA INFILE 'file:///work/taxi-trip/data/taxi_tour_table_train_simple.csv' INTO TABLE t1 options(format='csv', header=true, mode='append');
```
```{note}
注意，集群版 `LOAD DATA` 为非阻塞任务，可以使用命令 `SHOW JOBS` 查看任务运行状态，请等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。
```

### 2.8 启动预估服务

1. 如果尚未退出 OpenMLDB CLI，请使用 `quit` 命令退出 OpenMLDB CLI。
2. 在普通命令行下启动预估服务：

```bash
./start_predict_server.sh 127.0.0.1:9080 /tmp/model.txt
```
### 2.9 发送预估请求

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
