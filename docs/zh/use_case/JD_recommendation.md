
#  OpenMLDB + OneFlow: 高潜用户购买意向预测

本文我们将以[京东高潜用户购买意向预测问题](https://jdata.jd.com/html/detail.html?id=1)为例，示范如何使用[OpenMLDB](https://github.com/4paradigm/OpenMLDB)和 [OneFlow](https://github.com/Oneflow-Inc/oneflow) 联合来打造一个完整的机器学习应用。

如何从历史数据中找出规律，去预测用户未来的购买需求，让最合适的商品遇见最需要的人，是大数据应用在精准营销中的关键问题，也是所有电商平台在做智能化升级时所需要的核心技术。京东作为中国最大的自营式电商，沉淀了数亿的忠实用户，积累了海量的真实数据。本案例以京东商城真实的用户、商品和行为数据（脱敏后）为基础，通过数据挖掘的技术和机器学习的算法，构建用户购买商品的预测模型，输出高潜用户和目标商品的匹配结果，为精准营销提供高质量的目标群体，挖掘数据背后潜在的意义，为电商用户提供更简单、快捷、省心的购物体验。本案例使用OpenMLDB进行数据挖掘，使用OneFlow中的[DeepFM](https://github.com/Oneflow-Inc/models/tree/main/RecommenderSystems/deepfm)模型进行高性能训练推理，提供精准的商品推荐。

本案例基于 OpenMLDB 集群版进行教程演示。注意，本文档使用的是预编译好的 docker 镜像。如果希望在自己编译和搭建的 OpenMLDB 环境下进行测试，需要配置使用我们[面向特征工程优化的 Spark 发行版](https://openmldb.ai/docs/zh/main/tutorial/openmldbspark_distribution.html)。请参考相关[编译](https://openmldb.ai/docs/zh/main/deploy/compile.html)（参考章节：“针对OpenMLDB优化的Spark发行版”）和[安装部署文档](https://openmldb.ai/docs/zh/main/deploy/install_deploy.html)（参考章节：“部署TaskManager” - “2 修改配置文件conf/taskmanager.properties”）。

## 1.  环境准备和预备知识

### 1.1 OneFlow工具包安装
OneFlow工具依赖GPU的强大算力，所以请确保部署机器具备Nvidia GPU，并且保证驱动版本 >=460.X.X  [驱动版本需支持CUDA 11.0](https://docs.nvidia.com/cuda/cuda-toolkit-release-notes/index.html#cuda-major-component-versions)。
使用一下指令安装OneFlow：
```bash
conda activate oneflow
python3 -m pip install --pre oneflow -f https://staging.oneflow.info/branch/support_oneembedding_serving/cu102
```
还需要安装以下Python工具包：
```bash
pip install psutil petastorm pandas sklearn
```

### 1.2 拉取和启动 OpenMLDB Docker 镜像
- 注意，请确保 Docker Engine 版本号 >= 18.03
- 拉取 OpenMLDB docker 镜像，并且运行相应容器
- 下载demo文件包，并映射demo文件夹至`/root/project`，这里我们使用的路径为`demodir=/home/gtest/demo`
```bash
export demodir=/home/gtest/demo
docker run -dit --name=demo --network=host -v $demodir:/root/project 4pdosc/openmldb:0.5.2 bash
docker exec -it demo bash
```
- 上述镜像预装了OpenMLDB的工具等，我们需要进一步安装OneFlow推理所需依赖。

因为我们将在OpenMLDB的容器中嵌入OneFlow模型推理的预处理及调用，需要安装以下的依赖。
```bash
pip install tritonclient xxhash geventhttpclient
```

```{note}
注意，本教程以下的OpenMLDB部分的演示命令默认均在该已经启动的 docker 容器内运行。OneFlow命令默认在 1.1 安装的OneFlow环境下运行。
```

### 1.3 初始化环境

```bash
./init.sh
```
我们在镜像内提供了init.sh脚本帮助用户快速初始化环境，包括：
-   配置 zookeeper
-   启动集群版 OpenMLDB

### 1.4 启动 OpenMLDB CLI 客户端
```bash
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```
```{note}
注意，本教程大部分命令在 OpenMLDB CLI 下执行，为了跟普通 shell 环境做区分，在 OpenMLDB CLI 下执行的命令均使用特殊的提示符  `>`  。
```

### 1.5 预备知识：集群版的非阻塞任务
集群版的部分命令是非阻塞任务，包括在线模式的  `LOAD  DATA`，以及离线模式的  `LOAD  DATA`  ，`SELECT`，`SELECT  INTO`  命令。提交任务以后可以使用相关的命令如  `SHOW  JOBS`,  `SHOW  JOB`  来查看任务进度，详情参见离线任务管理文档。


## 2. 机器学习训练流程
### 2.1 流程概览
使用OpenMLDB+OneFlow进行机器学习训练可总结为以下大致步骤。
接下来会介绍每一个步骤的具体操作细节。

### 2.2 使用OpenMLDB进行离线特征抽取
#### 2.2.1 创建数据库和数据表
以下命令均在 OpenMLDB CLI 环境下执行。
```sql
> CREATE DATABASE JD_db;
> USE JD_db;
> CREATE TABLE action(reqId string, eventTime timestamp, ingestionTime timestamp, actionValue int);
> CREATE TABLE flattenRequest(reqId string, eventTime timestamp, main_id string, pair_id string, user_id string, sku_id string, time bigint, split_id int, time1 string);
> CREATE TABLE bo_user(ingestionTime timestamp, user_id string, age string, sex string, user_lv_cd string, user_reg_tm bigint);
> CREATE TABLE bo_action(ingestionTime timestamp, pair_id string, time bigint, model_id string, type string, cate string, br string);
> CREATE TABLE bo_product(ingestionTime timestamp, sku_id string, a1 string, a2 string, a3 string, cate string, br string);
> CREATE TABLE bo_comment(ingestionTime timestamp, dt bigint, sku_id string, comment_num int, has_bad_comment string, bad_comment_rate float);
```
也可使用sql脚本(`/root/project/create_tables.sql`)运行：

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/create_tables.sql
```
#### 2.2.2 离线数据准备
首先，切换到离线执行模式。接着，导入数据作为离线数据，用于离线特征计算。

以下命令均在 OpenMLDB CLI 下执行。
```sql
> USE JD_db;
> SET @@execute_mode='offline';
> LOAD DATA INFILE '/root/project/data/JD_data/action/*.parquet' INTO TABLE action options(format='parquet', header=true, mode='overwrite');
> LOAD DATA INFILE '/root/project/data/JD_data/flattenRequest_clean/*.parquet' INTO TABLE flattenRequest options(format='parquet', header=true, mode='overwrite');
> LOAD DATA INFILE '/root/project/data/JD_data/bo_user/*.parquet' INTO TABLE bo_user options(format='parquet', header=true, mode='overwrite');
> LOAD DATA INFILE '/root/project/data/JD_data/bo_action/*.parquet' INTO TABLE bo_action options(format='parquet', header=true, mode='overwrite');
> LOAD DATA INFILE '/root/project/data/JD_data/bo_product/*.parquet' INTO TABLE bo_product options(format='parquet', header=true, mode='overwrite');
> LOAD DATA INFILE '/root/project/data/JD_data/bo_comment/*.parquet' INTO TABLE bo_comment options(format='parquet', header=true, mode='overwrite');
```
或使用脚本执行，并通过以下命令快速查询jobs状态：

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/load_data.sql

echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```
```{note}
注意，集群版 `LOAD  DATA` 为非阻塞任务，可以使用命令 `SHOW  JOBS` 查看任务运行状态，请等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。
```

#### 2.2.3 特征设计
通常在设计特征前，用户需要根据机器学习的目标对数据进行分析，然后根据分析设计和调研特征。机器学习的数据分析和特征研究不是本文讨论的范畴，我们将不作展开。本文假定用户具备机器学习的基本理论知识，有解决机器学习问题的能力，能够理解SQL语法，并能够使用SQL语法构建特征。针对本案例，用户经过分析和调研设计了若干特征。

请注意，在实际的机器学习特征调研过程中，科学家对特征进行反复试验，寻求模型效果最好的特征集。所以会不断的重复多次特征设计->离线特征抽取->模型训练过程，并不断调整特征以达到预期效果。

#### 2.2.4 离线特征抽取
用户在离线模式下，进行特征抽取，并将特征结果输出到`'/root/project/out/1'`目录下保存（对应映射为`$demodir/out/1`），以供后续的模型训练。 `SELECT` 命令对应了基于上述特征设计所产生的 SQL 特征计算脚本。以下命令均在 OpenMLDB CLI 下执行。
```sql
> USE JD_db;
> select * from
(
select
    `reqId` as reqId_1,
    `eventTime` as flattenRequest_eventTime_original_0,
    `reqId` as flattenRequest_reqId_original_1,
    `pair_id` as flattenRequest_pair_id_original_24,
    `sku_id` as flattenRequest_sku_id_original_25,
    `user_id` as flattenRequest_user_id_original_26,
    distinct_count(`pair_id`) over flattenRequest_user_id_eventTime_0_10_ as flattenRequest_pair_id_window_unique_count_27,
    fz_top1_ratio(`pair_id`) over flattenRequest_user_id_eventTime_0_10_ as flattenRequest_pair_id_window_top1_ratio_28,
    fz_top1_ratio(`pair_id`) over flattenRequest_user_id_eventTime_0s_14d_200 as flattenRequest_pair_id_window_top1_ratio_29,
    distinct_count(`pair_id`) over flattenRequest_user_id_eventTime_0s_14d_200 as flattenRequest_pair_id_window_unique_count_32,
    case when !isnull(at(`pair_id`, 0)) over flattenRequest_user_id_eventTime_0_10_ then count_where(`pair_id`, `pair_id` = at(`pair_id`, 0)) over flattenRequest_user_id_eventTime_0_10_ else null end as flattenRequest_pair_id_window_count_35,
    dayofweek(timestamp(`eventTime`)) as flattenRequest_eventTime_dayofweek_41,
    case when 1 < dayofweek(timestamp(`eventTime`)) and dayofweek(timestamp(`eventTime`)) < 7 then 1 else 0 end as flattenRequest_eventTime_isweekday_43
from
    `flattenRequest`
    window flattenRequest_user_id_eventTime_0_10_ as (partition by `user_id` order by `eventTime` rows between 10 preceding and 0 preceding),
    flattenRequest_user_id_eventTime_0s_14d_200 as (partition by `user_id` order by `eventTime` rows_range between 14d preceding and 0s preceding MAXSIZE 200))
as out0
last join
(
select
    `flattenRequest`.`reqId` as reqId_3,
    `action_reqId`.`actionValue` as action_actionValue_multi_direct_2,
    `bo_product_sku_id`.`a1` as bo_product_a1_multi_direct_3,
    `bo_product_sku_id`.`a2` as bo_product_a2_multi_direct_4,
    `bo_product_sku_id`.`a3` as bo_product_a3_multi_direct_5,
    `bo_product_sku_id`.`br` as bo_product_br_multi_direct_6,
    `bo_product_sku_id`.`cate` as bo_product_cate_multi_direct_7,
    `bo_product_sku_id`.`ingestionTime` as bo_product_ingestionTime_multi_direct_8,
    `bo_user_user_id`.`age` as bo_user_age_multi_direct_9,
    `bo_user_user_id`.`ingestionTime` as bo_user_ingestionTime_multi_direct_10,
    `bo_user_user_id`.`sex` as bo_user_sex_multi_direct_11,
    `bo_user_user_id`.`user_lv_cd` as bo_user_user_lv_cd_multi_direct_12
from
    `flattenRequest`
    last join `action` as `action_reqId` on `flattenRequest`.`reqId` = `action_reqId`.`reqId`
    last join `bo_product` as `bo_product_sku_id` on `flattenRequest`.`sku_id` = `bo_product_sku_id`.`sku_id`
    last join `bo_user` as `bo_user_user_id` on `flattenRequest`.`user_id` = `bo_user_user_id`.`user_id`)
as out1
on out0.reqId_1 = out1.reqId_3
last join
(
select
    `reqId` as reqId_14,
    max(`bad_comment_rate`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_bad_comment_rate_multi_max_13,
    min(`bad_comment_rate`) over bo_comment_sku_id_ingestionTime_0_10_ as bo_comment_bad_comment_rate_multi_min_14,
    min(`bad_comment_rate`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_bad_comment_rate_multi_min_15,
    distinct_count(`comment_num`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_comment_num_multi_unique_count_22,
    distinct_count(`has_bad_comment`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_has_bad_comment_multi_unique_count_23,
    fz_topn_frequency(`has_bad_comment`, 3) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_has_bad_comment_multi_top3frequency_30,
    fz_topn_frequency(`comment_num`, 3) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_comment_num_multi_top3frequency_33
from
    (select `eventTime` as `ingestionTime`, bigint(0) as `dt`, `sku_id` as `sku_id`, int(0) as `comment_num`, '' as `has_bad_comment`, float(0) as `bad_comment_rate`, reqId from `flattenRequest`)
    window bo_comment_sku_id_ingestionTime_0s_64d_100 as (
UNION (select `ingestionTime`, `dt`, `sku_id`, `comment_num`, `has_bad_comment`, `bad_comment_rate`, '' as reqId from `bo_comment`) partition by `sku_id` order by `ingestionTime` rows_range between 64d preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    bo_comment_sku_id_ingestionTime_0_10_ as (
UNION (select `ingestionTime`, `dt`, `sku_id`, `comment_num`, `has_bad_comment`, `bad_comment_rate`, '' as reqId from `bo_comment`) partition by `sku_id` order by `ingestionTime` rows between 10 preceding and 0 preceding INSTANCE_NOT_IN_WINDOW))
as out2
on out0.reqId_1 = out2.reqId_14
last join
(
select
    `reqId` as reqId_17,
    fz_topn_frequency(`br`, 3) over bo_action_pair_id_ingestionTime_0s_10h_100 as bo_action_br_multi_top3frequency_16,
    fz_topn_frequency(`cate`, 3) over bo_action_pair_id_ingestionTime_0s_10h_100 as bo_action_cate_multi_top3frequency_17,
    fz_topn_frequency(`model_id`, 3) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_model_id_multi_top3frequency_18,
    distinct_count(`model_id`) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_model_id_multi_unique_count_19,
    distinct_count(`model_id`) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_model_id_multi_unique_count_20,
    distinct_count(`type`) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_type_multi_unique_count_21,
    fz_topn_frequency(`type`, 3) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_type_multi_top3frequency_40,
    fz_topn_frequency(`type`, 3) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_type_multi_top3frequency_42
from
    (select `eventTime` as `ingestionTime`, `pair_id` as `pair_id`, bigint(0) as `time`, '' as `model_id`, '' as `type`, '' as `cate`, '' as `br`, reqId from `flattenRequest`)
    window bo_action_pair_id_ingestionTime_0s_10h_100 as (
UNION (select `ingestionTime`, `pair_id`, `time`, `model_id`, `type`, `cate`, `br`, '' as reqId from `bo_action`) partition by `pair_id` order by `ingestionTime` rows_range between 10h preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    bo_action_pair_id_ingestionTime_0s_7d_100 as (
UNION (select `ingestionTime`, `pair_id`, `time`, `model_id`, `type`, `cate`, `br`, '' as reqId from `bo_action`) partition by `pair_id` order by `ingestionTime` rows_range between 7d preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    bo_action_pair_id_ingestionTime_0s_14d_100 as (
UNION (select `ingestionTime`, `pair_id`, `time`, `model_id`, `type`, `cate`, `br`, '' as reqId from `bo_action`) partition by `pair_id` order by `ingestionTime` rows_range between 14d preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW))
as out3
on out0.reqId_1 = out3.reqId_17
INTO OUTFILE '/root/project/out/1';
```
此处仅一个命令，可以使用阻塞式`LOAD DATA`，直接运行sql脚本`sync_select_out.sql`:

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/sync_select_out.sql
```
```{note}
注意，集群版 `SELECT INTO` 为非阻塞任务，可以使用命令 `SHOW JOBS` 查看任务运行状态，请等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。
```
### 2.3 预处理数据集以配合DeepFM模型要求
```{note}
注意，以下命令在docker外执行，使用安装了1.1所描述的OneFlow运行环境
```
根据 [DeepFM 论文](https://arxiv.org/abs/1703.04247), 类别特征和连续特征都被当作稀疏特征对待。

> χ may include categorical fields (e.g., gender, location) and continuous fields (e.g., age). Each categorical field is represented as a vector of one-hot encoding, and each continuous field is represented as the value itself, or a vector of one-hot encoding after discretization.

进入demo文件夹，运行以下指令进行数据处理
```bash
cd $demodir/openmldb_process/
sh process_JD_out_full.sh $demodir/out/1
```
对应生成parquet数据集将生成在 `$demodir/openmldb_process/out`。数据信息将被打印如下，该信息将被输入为训练的配置文件。
>train samples = 4007924
>val samples = 504398
>test samples = 530059
>table size array:
>11,42,1105,200,11,1295,1,1,5,3,23,23,7,5042381,3127923,5042381,3649642,28350,105180,7,2,5042381,5,4,4,41,2,2,8,3456,4,5,5042381,10,60,5042381,843,17,1276,101,100

### 2.4 启动OneFlow进行模型训练
```{note}
注意，以下命令在安装1.1所描述的OneFlow运行环境中运行
```
#### 2.4.1 修改对应`train_deepfm.sh`配置文件
```bash
cd $demodir/oneflow_process/
```
```bash
#!/bin/bash
DEVICE_NUM_PER_NODE=1
DATA_DIR=$demodir/openmldb_process/out
PERSISTENT_PATH=/$demodir/oneflow_process/persistent
MODEL_SAVE_DIR=$demodir/oneflow_process/model_out
MODEL_SERVING_PATH=$demodir/oneflow_process/model/embedding/1/model

python3 -m oneflow.distributed.launch \
--nproc_per_node $DEVICE_NUM_PER_NODE \
--nnodes 1 \
--node_rank 0 \
--master_addr 127.0.0.1 \
deepfm_train_eval_JD.py \
--disable_fusedmlp \
--data_dir $DATA_DIR \
--persistent_path $PERSISTENT_PATH \
--table_size_array "11,42,1105,200,11,1295,1,1,5,3,23,23,7,5042381,3127923,5042381,3649642,28350,105180,7,2,5042381,5,4,4,41,2,2,8,3456,4,5,5042381,10,60,5042381,843,17,1276,101,100" \
--store_type 'cached_host_mem' \
--cache_memory_budget_mb 1024 \
--batch_size 10000 \
--train_batches 75000 \
--loss_print_interval 100 \
--dnn "1000,1000,1000,1000,1000" \
--net_dropout 0.2 \
--learning_rate 0.001 \
--embedding_vec_size 16 \
--num_train_samples 4007924 \
--num_val_samples 504398 \
--num_test_samples 530059 \
--model_save_dir $MODEL_SAVE_DIR \
--save_best_model \
--save_graph_for_serving \
--model_serving_path $MODEL_SERVING_PATH \
--save_model_after_each_eval
```
#### 2.4.2 开始模型训练
```bash
bash train_deepfm.sh
```
生成模型将存放在`$demodir/oneflow_process/model_out`，用来serving的模型存放在`$demodir/oneflow_process/model/embedding/1/model`

## 3. 模型上线流程
### 3.1 流程概览
使用OpenMLDB+OneFlow进行模型serving可总结为以下大致步骤。
接下来会介绍每一个步骤的具体操作细节。

### 3.2 配置OpenMLDB进行在线特征抽取

#### 3.2.1 特征抽取SQL脚本上线
假定2.2.3节中所设计的特征在上一步的模型训练中产出的模型符合预期，那么下一步就是将该特征抽取SQL脚本部署到线上去，以提供在线的特征抽取。
1. 重新启动 OpenMLDB CLI，以进行 SQL 上线部署。
   ```bash
   docker exec -it demo bash
   /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
   ```
2. 执行上线部署，以下命令在 OpenMLDB CLI 内执行。
```sql
> USE JD_db;
> SET @@execute_mode='online';
> deploy demo select * from
(
select
    `reqId` as reqId_1,
    `eventTime` as flattenRequest_eventTime_original_0,
    `reqId` as flattenRequest_reqId_original_1,
    `pair_id` as flattenRequest_pair_id_original_24,
    `sku_id` as flattenRequest_sku_id_original_25,
    `user_id` as flattenRequest_user_id_original_26,
    distinct_count(`pair_id`) over flattenRequest_user_id_eventTime_0_10_ as flattenRequest_pair_id_window_unique_count_27,
    fz_top1_ratio(`pair_id`) over flattenRequest_user_id_eventTime_0_10_ as flattenRequest_pair_id_window_top1_ratio_28,
    fz_top1_ratio(`pair_id`) over flattenRequest_user_id_eventTime_0s_14d_200 as flattenRequest_pair_id_window_top1_ratio_29,
    distinct_count(`pair_id`) over flattenRequest_user_id_eventTime_0s_14d_200 as flattenRequest_pair_id_window_unique_count_32,
    case when !isnull(at(`pair_id`, 0)) over flattenRequest_user_id_eventTime_0_10_ then count_where(`pair_id`, `pair_id` = at(`pair_id`, 0)) over flattenRequest_user_id_eventTime_0_10_ else null end as flattenRequest_pair_id_window_count_35,
    dayofweek(timestamp(`eventTime`)) as flattenRequest_eventTime_dayofweek_41,
    case when 1 < dayofweek(timestamp(`eventTime`)) and dayofweek(timestamp(`eventTime`)) < 7 then 1 else 0 end as flattenRequest_eventTime_isweekday_43
from
    `flattenRequest`
    window flattenRequest_user_id_eventTime_0_10_ as (partition by `user_id` order by `eventTime` rows between 10 preceding and 0 preceding),
    flattenRequest_user_id_eventTime_0s_14d_200 as (partition by `user_id` order by `eventTime` rows_range between 14d preceding and 0s preceding MAXSIZE 200))
as out0
last join
(
select
    `flattenRequest`.`reqId` as reqId_3,
    `action_reqId`.`actionValue` as action_actionValue_multi_direct_2,
    `bo_product_sku_id`.`a1` as bo_product_a1_multi_direct_3,
    `bo_product_sku_id`.`a2` as bo_product_a2_multi_direct_4,
    `bo_product_sku_id`.`a3` as bo_product_a3_multi_direct_5,
    `bo_product_sku_id`.`br` as bo_product_br_multi_direct_6,
    `bo_product_sku_id`.`cate` as bo_product_cate_multi_direct_7,
    `bo_product_sku_id`.`ingestionTime` as bo_product_ingestionTime_multi_direct_8,
    `bo_user_user_id`.`age` as bo_user_age_multi_direct_9,
    `bo_user_user_id`.`ingestionTime` as bo_user_ingestionTime_multi_direct_10,
    `bo_user_user_id`.`sex` as bo_user_sex_multi_direct_11,
    `bo_user_user_id`.`user_lv_cd` as bo_user_user_lv_cd_multi_direct_12
from
    `flattenRequest`
    last join `action` as `action_reqId` on `flattenRequest`.`reqId` = `action_reqId`.`reqId`
    last join `bo_product` as `bo_product_sku_id` on `flattenRequest`.`sku_id` = `bo_product_sku_id`.`sku_id`
    last join `bo_user` as `bo_user_user_id` on `flattenRequest`.`user_id` = `bo_user_user_id`.`user_id`)
as out1
on out0.reqId_1 = out1.reqId_3
last join
(
select
    `reqId` as reqId_14,
    max(`bad_comment_rate`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_bad_comment_rate_multi_max_13,
    min(`bad_comment_rate`) over bo_comment_sku_id_ingestionTime_0_10_ as bo_comment_bad_comment_rate_multi_min_14,
    min(`bad_comment_rate`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_bad_comment_rate_multi_min_15,
    distinct_count(`comment_num`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_comment_num_multi_unique_count_22,
    distinct_count(`has_bad_comment`) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_has_bad_comment_multi_unique_count_23,
    fz_topn_frequency(`has_bad_comment`, 3) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_has_bad_comment_multi_top3frequency_30,
    fz_topn_frequency(`comment_num`, 3) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_comment_num_multi_top3frequency_33
from
    (select `eventTime` as `ingestionTime`, bigint(0) as `dt`, `sku_id` as `sku_id`, int(0) as `comment_num`, '' as `has_bad_comment`, float(0) as `bad_comment_rate`, reqId from `flattenRequest`)
    window bo_comment_sku_id_ingestionTime_0s_64d_100 as (
UNION (select `ingestionTime`, `dt`, `sku_id`, `comment_num`, `has_bad_comment`, `bad_comment_rate`, '' as reqId from `bo_comment`) partition by `sku_id` order by `ingestionTime` rows_range between 64d preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    bo_comment_sku_id_ingestionTime_0_10_ as (
UNION (select `ingestionTime`, `dt`, `sku_id`, `comment_num`, `has_bad_comment`, `bad_comment_rate`, '' as reqId from `bo_comment`) partition by `sku_id` order by `ingestionTime` rows between 10 preceding and 0 preceding INSTANCE_NOT_IN_WINDOW))
as out2
on out0.reqId_1 = out2.reqId_14
last join
(
select
    `reqId` as reqId_17,
    fz_topn_frequency(`br`, 3) over bo_action_pair_id_ingestionTime_0s_10h_100 as bo_action_br_multi_top3frequency_16,
    fz_topn_frequency(`cate`, 3) over bo_action_pair_id_ingestionTime_0s_10h_100 as bo_action_cate_multi_top3frequency_17,
    fz_topn_frequency(`model_id`, 3) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_model_id_multi_top3frequency_18,
    distinct_count(`model_id`) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_model_id_multi_unique_count_19,
    distinct_count(`model_id`) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_model_id_multi_unique_count_20,
    distinct_count(`type`) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_type_multi_unique_count_21,
    fz_topn_frequency(`type`, 3) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_type_multi_top3frequency_40,
    fz_topn_frequency(`type`, 3) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_type_multi_top3frequency_42
from
    (select `eventTime` as `ingestionTime`, `pair_id` as `pair_id`, bigint(0) as `time`, '' as `model_id`, '' as `type`, '' as `cate`, '' as `br`, reqId from `flattenRequest`)
    window bo_action_pair_id_ingestionTime_0s_10h_100 as (
UNION (select `ingestionTime`, `pair_id`, `time`, `model_id`, `type`, `cate`, `br`, '' as reqId from `bo_action`) partition by `pair_id` order by `ingestionTime` rows_range between 10h preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    bo_action_pair_id_ingestionTime_0s_7d_100 as (
UNION (select `ingestionTime`, `pair_id`, `time`, `model_id`, `type`, `cate`, `br`, '' as reqId from `bo_action`) partition by `pair_id` order by `ingestionTime` rows_range between 7d preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW),
    bo_action_pair_id_ingestionTime_0s_14d_100 as (
UNION (select `ingestionTime`, `pair_id`, `time`, `model_id`, `type`, `cate`, `br`, '' as reqId from `bo_action`) partition by `pair_id` order by `ingestionTime` rows_range between 14d preceding and 0s preceding MAXSIZE 100 INSTANCE_NOT_IN_WINDOW))
as out3
on out0.reqId_1 = out3.reqId_17;
```
```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/deploy.sql
```

可使用如下命令确认deploy信息:
```sql
show deployment demo;
```
#### 3.2.2 在线数据准备
首先，请切换到**在线**执行模式。接着在在线模式下，导入数据作为在线数据，用于在线特征计算。以下命令均在 OpenMLDB CLI 下执行。
```sql
> USE JD_db;
> SET @@execute_mode='online';
> LOAD DATA INFILE '/root/project/data/JD_data/action/*.parquet' INTO TABLE action options(format='parquet', header=true, mode='append');
> LOAD DATA INFILE '/root/project/data/JD_data/flattenRequest_clean/*.parquet' INTO TABLE flattenRequest options(format='parquet', header=true, mode='append');
> LOAD DATA INFILE '/root/project/data/JD_data/bo_user/*.parquet' INTO TABLE bo_user options(format='parquet', header=true, mode='append');
> LOAD DATA INFILE '/root/project/data/JD_data/bo_action/*.parquet' INTO TABLE bo_action options(format='parquet', header=true, mode='append');
> LOAD DATA INFILE '/root/project/data/JD_data/bo_product/*.parquet' INTO TABLE bo_product options(format='parquet', header=true, mode='append');
> LOAD DATA INFILE '/root/project/data/JD_data/bo_comment/*.parquet' INTO TABLE bo_comment options(format='parquet', header=true, mode='append');
```

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/load_online_data.sql
```
```{note}
注意，集群版 `LOAD  DATA` 为非阻塞任务，可以使用命令 `SHOW  JOBS` 查看任务运行状态，请等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。
```
### 3.3 配置OneFlow推理服务
OneFlow的推理服务需要[OneEmbedding](https://docs.oneflow.org/master/cookies/one_embedding.html)的支持。该支持目前还没有合入主框架中。若需要重新编译，可参考附录A进行编译测试。接下来步骤默认相关支持已编译完成，并且存放在`/home/gtest/work/oneflow_serving/`路径中。

#### 3.3.1 检查模型路径（`$demodir/oneflow_process/model`）中模型文件及组织方式是否正确
```
$ tree  -L 3 model/
model/
└── embedding
    ├── 1
    │   └── model
    └── config.pbtxt
 ```   
#### 3.3.2 确认`config.pbtxt`中的配置正确。
#### 3.3.3 确认persistent路径（`$demodir/oneflow_process/persistent`）正确
 
### 3.4 启动推理服务
#### 3.4.1 启动OpenMLDB推理服务
```{note}
注意，以下命令在demo docker中运行。
```
1.  如果尚未退出 OpenMLDB CLI，请使用  `quit`  命令退出 OpenMLDB CLI。
2. 在普通命令行下启动预估服务：
```bash
cd /root/project/serving/openmldb_serving
./start_predict_server.sh 0.0.0.0:9080
```

#### 3.4.2 启动OneFlow推理服务
```{note}
注意，以下命令在安装1.1所描述的OneFlow运行环境中运行
```
使用一下命令启动OneFlow推理服务：
```bash
docker run --runtime=nvidia --rm --network=host \
  -v $demodir/oneflow_process/model:/models \
  -v /home/gtest/work/oneflow_serving/serving/build/libtriton_oneflow.so:/backends/oneflow/libtriton_oneflow.so \
  -v /home/gtest/work/oneflow_serving/oneflow/build/liboneflow_cpp/lib/:/mylib \
  -v $demodir/oneflow_process/persistent:/root/demo/persistent \
  registry.cn-beijing.aliyuncs.com/oneflow/triton-devel \
  bash -c 'LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/mylib /opt/tritonserver/bin/tritonserver \
  --model-repository=/models --backend-directory=/backends'
```

### 3.5 发送预估请求
预估请求可在OpenMLDB的容器外执行。容器外部访问的具体信息可参见[IP 配置](https://openmldb.ai/docs/zh/main/reference/ip_tips.html)。
在普通命令行下执行内置的 `predict.py` 脚本。该脚本发送一行请求数据到预估服务，接收返回的预估结果，并打印出来。
```bash
python $demodir/serving/predict.py
```
范例输出：
```
----------------ins---------------
['200080_5505_2016-03-15 20:43:04' '1458045784000'
'200080_5505_2016-03-15 20:43:04' '200080_5505' '5505' '200080' '1' '1.0'
'1.0' '1' '1' '3' '1' '200080_5505_2016-03-15 20:43:04' '0' '3' '1' '1'
'214' '8' '1603438960564' '-1' '1453824000000' '2' '1'
'200080_5505_2016-03-15 20:43:04' '0.02879999950528145' '0.0' '0.0' '2'
'2' '1,,NULL' '4,0,NULL' '200080_5505_2016-03-15 20:43:04' ',NULL,NULL'
',NULL,NULL' ',NULL,NULL' '1' '1' '1' ',NULL,NULL' ',NULL,NULL']
---------------predict change of purchase -------------
[[b'0.025186:0']]
```


## 附录A -- OneFlow定制代码编译
此章节介绍为OneEmbedding推理服务所定制的代码修改的编译过程。该代码会尽快合入OneFlow中，届时以下步骤可省略。

### A.1 容器环境准备
使用以下容器环境进行编译。该容器已经安装编译所需的依赖等。
```
docker pull registry.cn-beijing.aliyuncs.com/oneflow/triton-devel:latest
```
启动容器并映射相关路径(此处可映射`/home/work/gtest`至`/root/project`)。接下来的操作均在容器内进行。

### A.2 编译OneFlow
```shell
cd /root/project
mkdir oneflow_serving && cd oneflow_serving
git clone -b support_oneembedding_serving --single-branch https://github.com/Oneflow-Inc/oneflow --depth=1 
cd oneflow
mkdir build && cd build
cmake -C ../cmake/caches/cn/cuda.cmake \
-DBUILD_CPP_API=ON \
-DWITH_MLIR=ON \
-G Ninja \
-DBUILD_SHARED_LIBS=ON \
-DBUILD_HWLOC=OFF \
-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=OFF \
-DCMAKE_EXE_LINKER_FLAGS_INIT="-fuse-ld=lld" \
-DCMAKE_MODULE_LINKER_FLAGS_INIT="-fuse-ld=lld" \
-DCMAKE_SHARED_LINKER_FLAGS_INIT="-fuse-ld=lld" ..
ninja
```

### A.3 编译Serving

```shell
git clone -b support_one_embedding --single-branchhttps://github.com/Oneflow-Inc/serving.git
cd serving
mkdir build && cd build
cmake -DCMAKE_PREFIX_PATH=/path/to/liboneflow_cpp/share -DTRITON_RELATED_REPO_TAG=r21.10 \
  -DTRITON_ENABLE_GPU=ON -G Ninja -DTHIRD_PARTY_MIRROR=aliyun ..
ninja
```
上述命令中的`/path/to/liboneflow_cpp/share`要替换成上边编译的oneflow的里面的路径，在`{oneflow路径}/build/liboneflow_cpp/share`


### A.4 测试TritonServer
复制backend库文件：
```shell
mkdir /root/project/oneflow_sering/backends && cd /root/project/oneflow_sering/backends
mkdir oneflow
cp /roor/prject/oneflow_serving/serving/build/libtriton_oneflow.so oneflow/.
```

在命令行启动 TritonServer测试：
```shell
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/root/project/oneflow_serving/oneflow/build/liboneflow_cpp/lib \
/opt/tritonserver/bin/tritonserver \
--model-repository=/root/project/oneflow_process/model \
--backend-directory=/root/project/oneflow_serving/backends
```

在另一个命令行运行如下指令，若成功，输出示例如下：
```
python $demodir/serving/client.py 
>>>
0.045439958572387695
(1, 1)
[[b'0.025343:0']]
```
注意：
- 如果出现`libunwind.so.8`未找到需要用`-v /lib/x86_64-linux-gnu:/unwind_path` 映射一下`libunwind.so.8`所在目录，然后添加到`LD_LIBRARY_PATH`里面: `... bash -c 'LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/mylib:/unwind_path ...`
- 如果出现`libcupti.so`未找到需要用`-v /usr/local/cuda-11.7/extras/CUPTI/lib64:/cupti_path` 映射一下`libcupti.so`所在目录，然后添加到`LD_LIBRARY_PATH`里面: `... bash -c 'LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/mylib:/cupti_path ...`, 其中具体的cuda的路径按实际安装的位置，可以用`ldd {oneflow路径}/build/liboneflow.so | grep cupti`来找到

