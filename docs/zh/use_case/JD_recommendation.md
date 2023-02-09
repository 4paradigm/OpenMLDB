# OpenMLDB + OneFlow：高潜用户购买意向预测

本文将以[京东高潜用户购买意向预测问题](https://jdata.jd.com/html/detail.html?id=1)为例，示范如何使用 OpenMLDB 和 [OneFlow](https://github.com/OneFlow-Inc/oneflow) 联合来打造一个完整的机器学习应用。

## 背景

如何从历史数据中找出规律，去预测用户未来的购买需求，让最合适的商品遇见最需要的人，是大数据应用在精准营销中的关键问题，也是所有电商平台在做智能化升级时所需要的核心技术。京东作为中国最大的自营式电商，沉淀了数亿的忠实用户，积累了海量的真实数据。

本案例以京东商城真实的用户、商品和行为数据（脱敏后）为基础，通过数据挖掘技术和机器学习算法，构建用户购买商品的预测模型，输出高潜用户和目标商品的匹配结果，为精准营销提供高质量的目标群体，挖掘数据背后潜在的意义，为电商用户提供更简单、快捷、省心的购物体验。

本案例使用 OpenMLDB 进行数据挖掘，使用 OneFlow 中的 [DeepFM](https://github.com/OneFlow-Inc/models/tree/main/RecommenderSystems/deepfm) 模型进行高性能训练推理，提供精准的商品推荐。

```{note}

注意，本文档使用的是预编译好的 Docker 镜像。如果希望在自己编译和搭建的 OpenMLDB 环境下进行测试，需要配置使用[面向特征工程优化的 Spark 发行版](https://openmldb.ai/docs/zh/main/tutorial/openmldbspark_distribution.html)。请参考[针对 OpenMLDB 优化的 Spark 发行版文档](https://openmldb.ai/docs/zh/main/deploy/compile.html#openmldb-spark)和[安装部署文档](https://openmldb.ai/docs/zh/main/deploy/install_deploy.html)。

```

## 准备

### 下载数据与脚本

下载数据、脚本以及推理所需的 OneEmbedding 库（详情见[配置 OneFlow 推理服务](#配置-OneFlow-推理服务)），在后面的步骤中可以直接使用。

```
wget http://openmldb.ai/download/jd-recommendation/demo.tgz
tar xzf demo.tgz
ls demo
```

也可以 checkout GitHub 仓库中的 `demo/jd-recommendation`。
这个 `demo` 目录定为环境变量 `demodir`，之后的脚本中多会使用这一环境变量。所以，你需要配置这一变量：

```
export demodir=<your_path>/demo
```

本例仅使用小数据集做演示。如果你想要使用全量数据集，请下载 [JD_data](http://openmldb.ai/download/jd-recommendation/JD_data.tgz)。

### 安装 OneFlow 工具包

OneFlow 工具依赖 GPU 的强大算力，所以请确保部署机器具备 Nvidia GPU，并且保证驱动版本 >=460.X.X。[驱动版本需支持 CUDA 11.0](https://docs.nvidia.com/cuda/cuda-toolkit-release-notes/index.html#cuda-major-component-versions)。

推荐使用 conda 来管理 OneFlow 环境，安装 OneFlow 开发版（支持 OneEmbedding），以及本案例演示所需的其他依赖，手动创建方法如下：

```bash
conda create -y -n oneflow python=3.9.2
conda activate oneflow
pip install -f https://staging.oneflow.info/branch/master/cu112 --pre oneflow
pip install psutil petastorm pandas sklearn xxhash "tritonclient[all]" geventhttpclient tornado
```

拉取 oneflow_serving 镜像：

```bash
docker pull oneflowinc/oneflow_serving:nightly
```

```{note}

注意，此处安装的为 OneFlow nightly 版本。本案例使用的版本 commit 如下：

Oneflow：https://github.com/Oneflow-Inc/oneflow/tree/fcf205cf57989a5ecb7a756633a4be08444d8a28

Oneflow-serving：https://github.com/Oneflow-Inc/serving/tree/ce5d667468b6b3ba66d3be6986f41f965e52cf16

```

### 启动 OpenMLDB Docker 容器

为了快速运行 OpenMLDB 集群，推荐使用镜像启动的方式。

- Docker：>= 18.03

由于 OpenMLDB 集群需要和其他组件网络通信，我们直接使用 host 网络。本例将在容器中使用已下载的脚本，所以请将数据脚本所在目录 `demodir` 映射为容器中的目录：

```bash
docker run -dit --name=openmldb --network=host -v $demodir:/work/oneflow_demo 4pdosc/openmldb:0.7.0 bash
docker exec -it openmldb bash
```

```{note}

注意，本教程以下的 OpenMLDB 部分的演示命令默认均在启动的 Docker 容器 `openmldb` 内运行。OneFlow 命令默认在安装的虚拟环境 `oneflow` 下运行。

```

### 创建 OpenMLDB 集群

```bash

/work/init.sh

```

镜像内提供了 init.sh 脚本帮助用户快速创建集群。

### 启动 OpenMLDB CLI

在 OpenMLDB CLI 内操作 OpenMLDB 集群，使用命令如下：

```bash

/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client

```

### 预备知识

集群版 OpenMLDB 部分命令是异步的，如：在线/离线模式的 `LOAD DATA`、`SELECT`、`SELECT INTO` 命令。提交任务以后可以使用相关的命令如 `SHOW JOBS`、`SHOW JOB` 来查看任务进度，详情参见[离线任务管理文档](../openmldb_sql/task_manage/SHOW_JOB.md)。


## 机器学习训练流程

### 流程概览

使用 OpenMLDB + OneFlow 进行机器学习训练可概括为:

1. OpenMLDB 离线特征设计与抽取（SQL）

2. OneFlow 模型训练

3. SQL 和模型上线

接下来会介绍每一个步骤的具体操作细节。

### 离线特征抽取

以下命令均在 OpenMLDB CLI 中执行。

#### 创建数据库和数据表

使用以下命令创建数据库和表：

```sql
-- OpenMLDB CLI
CREATE DATABASE JD_db;
USE JD_db;
CREATE TABLE action(reqId string, eventTime timestamp, ingestionTime timestamp, actionValue int);
CREATE TABLE flattenRequest(reqId string, eventTime timestamp, main_id string, pair_id string, user_id string, sku_id string, time bigint, split_id int, time1 string);
CREATE TABLE bo_user(ingestionTime timestamp, user_id string, age string, sex string, user_lv_cd string, user_reg_tm bigint);
CREATE TABLE bo_action(ingestionTime timestamp, pair_id string, time bigint, model_id string, type string, cate string, br string);
CREATE TABLE bo_product(ingestionTime timestamp, sku_id string, a1 string, a2 string, a3 string, cate string, br string);
CREATE TABLE bo_comment(ingestionTime timestamp, dt bigint, sku_id string, comment_num int, has_bad_comment string, bad_comment_rate float);

```

也可直接运行 sql 脚本(`/work/oneflow_demo/sql_scripts/create_tables.sql`)来创建：

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/create_tables.sql
```

#### 导入离线数据

需要将源数据导入到 OpenMLDB 中作为离线数据，用于离线特征计算。如果你导入较大的数据集，可以使用软链接的方式，减少导入消耗时间。本次演示中仅导入很少的数据，因此使用硬拷贝。

```sql
-- OpenMLDB CLI
USE JD_db;
SET @@execute_mode='offline';
LOAD DATA INFILE '/work/oneflow_demo/data/action/*.parquet' INTO TABLE action options(format='parquet', header=true, mode='overwrite');
LOAD DATA INFILE '/work/oneflow_demo/data/flattenRequest_clean/*.parquet' INTO TABLE flattenRequest options(format='parquet', header=true, mode='overwrite');
LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_user/*.parquet' INTO TABLE bo_user options(format='parquet', header=true, mode='overwrite');
LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_action/*.parquet' INTO TABLE bo_action options(format='parquet', header=true, mode='overwrite');
LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_product/*.parquet' INTO TABLE bo_product options(format='parquet', header=true, mode='overwrite');
LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_comment/*.parquet' INTO TABLE bo_comment options(format='parquet', header=true, mode='overwrite');
```

或直接运行脚本导入，并快速查询 jobs 状态：

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/load_offline_data.sql
echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

```{important}

请等待 `SHOW JOBS` 中所有任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。

```

#### 特征设计

通常在设计特征前，用户需要根据机器学习的目标对数据进行分析，然后根据分析设计和调研特征。然而，机器学习的数据分析和特征研究并不是本文讨论的范畴。本文假定用户具备机器学习的基本理论知识，有解决机器学习问题的能力，能够理解 SQL 语法，并能够使用 SQL 语法构建特征。针对本案例，用户经过分析和调研设计了若干特征。

在实际的机器学习特征调研过程中，科学家对特征进行反复试验，寻求模型效果最好的特征集。所以会不断地重复多次“特征设计->离线特征抽取->模型训练”过程，并不断调整特征以达到预期效果。

#### 离线特征抽取

源数据准备好以后，就可以进行离线特征抽取，将特征结果输出到 `'/work/oneflow_demo/out/1'` 目录下保存（对应映射为 `$demodir/out/1`，方便容器外部使用特征数据），以供后续的模型训练。`SELECT` 命令对应了基于上述特征设计所产生的 SQL 特征计算脚本。

```sql
-- OpenMLDB CLI
USE JD_db;
select * from
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
INTO OUTFILE '/work/oneflow_demo/out/1' OPTIONS(mode='overwrite');
```

```{note}

`SELECT INTO` 为异步任务，可以使用命令 `SHOW JOBS` 查看任务运行状态，请等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。耗时大概一分半。

```

因为这里只有一个特征抽取任务，可以使用同步的运行方式，命令完成即特征抽取完成。直接运行 sql 脚本 `sync_select_out.sql`:

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/sync_select_out.sql
```

### 预处理特征数据以配合 DeepFM 模型要求

```{note}

注意，以下命令在 Docker 外执行，使用准备阶段安装的 OneFlow 运行环境。

```

根据 [DeepFM 论文](https://arxiv.org/abs/1703.04247)，类别特征和连续特征都被当作稀疏特征对待。

> χ may include categorical fields (e.g., gender, location) and continuous fields (e.g., age). Each categorical field is represented as a vector of one-hot encoding, and each continuous field is represented as the value itself, or a vector of one-hot encoding after discretization.

进入 demodir 文件夹，并使用预处理脚本做特征数据预处理。运行需要 pandas、xxhash 等依赖，推荐在 `oneflow` 虚拟环境中运行。

```bash

cd $demodir/feature_preprocess/
python preprocess.py $demodir/out/1

```

`$demodir/out/1` 即上一步 OpenMLDB 计算得到的特征数据目录。

脚本将在 `$demodir/feature_preprocess/out` 对应生成 train、test、valid 三个 Parquet 数据集，并将三者的行数和 `table_size_array` 保存在文件 `data_info.txt` 中（下一步可以直接使用 info 文件，不需要手动填写参数）。运行结果打印类似：

```
feature total count: 13916
train count: 11132
saved to <demodir>/feature_preprocess/out/train
test count: 1391
saved to <demodir>/feature_preprocess/out/test
val count: 1393
saved to <demodir>/feature_preprocess/out/valid
table size array:
 4,26,16,4,11,809,1,1,5,3,17,16,7,13916,13890,13916,10000,3674,9119,7,2,13916,5,4,4,33,2,2,7,2580,3,5,13916,10,47,13916,365,17,132,32,37
saved to <demodir>/feature_preprocess/out/data_info.txt
```

得到的文件结构类似：

```
out/
├── data_info.txt
├── test
│   └── test.parquet
├── train
│   └── train.parquet
└── valid
​    └── valid.parquet

3 directories, 4 files

```

### 启动 OneFlow 进行模型训练

```{note}

注意，以下命令在 OneFlow 运行环境中执行。

```

```bash

cd $demodir/oneflow_process/
sh train_deepfm.sh -h
Usage: train_deepfm.sh DATA_DIR(abs)
​   We'll read required args in $DATA_DIR/data_info.txt, and save results in path ./

```

OneFlow 模型训练使用该目录中的 `train_deepfm.sh` 脚本，使用说明如上所示。通常情况下，不用特别配置。脚本会自动读取 `$DATA_DIR/data_info.txt` 的参数，包括 `num_train_samples`、`num_val_samples`、`num_test_samples` 和 `table_size_array`。请使用预处理后的特征数据集（绝对路径），命令如下：

```bash

bash train_deepfm.sh $demodir/feature_preprocess/out

```

生成模型将存放在 `$demodir/oneflow_process/model_out`，用来 serving 的模型存放在 `$demodir/oneflow_process/model/embedding/1/model`。

## 模型上线流程

### 流程概览

使用 OpenMLDB+OneFlow 进行模型上线，主要步骤为：

1. OpenMLDB 上线： SQL 上线，准备在线数据
2. OneFlow 上线：加载模型
3. 启动预测服务：使用一个简单的 predict server 进行展示

接下来会介绍每一个步骤的具体操作细节。

### OpenMLDB 上线

#### 特征抽取 SQL 脚本上线

假定离线训练效果理想，那么就可以将该特征抽取 SQL 脚本上线，提供实时在线的特征抽取服务。在OpenMLDB 容器内（如已退出，`docker exec -it openmldb bash` 重新进入）：

1. 登录 OpenMLDB CLI。

   ```bash
   /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
   ```

2. 执行 SQL 上线部署。在 OpenMLDB CLI 中部署离线特征抽取使用的 SQL（SQL 较长，见[离线特征抽取](#离线特征抽取)，此处不展示 SQL）。

    ```sql
    -- OpenMLDB CLI
    USE JD_db;
    DEPLOY demo <SQL>;
    ```

    也可以在 Docker 容器内直接运行部署脚本：

    ```
    /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/deploy.sql
    ```

    可使用如下命令确认部署信息:

    ```sql
    show deployment demo;
    ```

    部署后，可通过访问 OpenMLDB ApiServer `127.0.0.1:9080` 进行实时特征计算。

#### 导入在线数据

在线执行模式下导入在线数据，用于在线特征计算。为了简单起见，我们直接导入和离线一致的数据集。生产中，通常是离线是大量的冷数据，在线是近期的热数据。

以下命令均在 OpenMLDB CLI 下执行。

```sql
-- OpenMLDB CLI
USE JD_db;
SET @@execute_mode='online';
LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/action/*.parquet' INTO TABLE action options(format='parquet', mode='append');
LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/flattenRequest_clean/*.parquet' INTO TABLE flattenRequest options(format='parquet', mode='append');
LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_user/*.parquet' INTO TABLE bo_user options(format='parquet', mode='append');
LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_action/*.parquet' INTO TABLE bo_action options(format='parquet', mode='append');
LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_product/*.parquet' INTO TABLE bo_product options(format='parquet', mode='append');
LOAD DATA INFILE '/work/oneflow_demo/data/JD_data/bo_comment/*.parquet' INTO TABLE bo_comment options(format='parquet', mode='append');
```

也可以执行脚本导入：

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/load_online_data.sql
```

使用以下命令快速查看 jobs 的状态：

```
 echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

```{note}

注意，在线 `LOAD DATA` 也是异步任务，请等待任务运行成功（ `state` 转至 `FINISHED` 状态），再进行下一步操作 。

```

### 配置 OneFlow 推理服务

OneFlow 的推理服务需要 [OneEmbedding](https://docs.OneFlow.org/master/cookies/one_embedding.html) 的支持。该支持目前还没有合入主框架中。

我们提供预编译版本的库在 `$demodir/oneflow_serving/` 中。若与你的环境不兼容，你需要重新编译，可参考附录 A 进行编译测试。接下来步骤默认相关支持已编译完成，并且存放在 `$demodir/OneFlow_serving/` 路径中。

#### 检查

1. 模型路径 (`$demodir/oneflow_process/model`) 结果是否如下所示。

    ```
    cd $demodir/oneflow_process/
    tree -L 4 model/
    ```

    ```
    model/
    `-- embedding
        |-- 1
        |   `-- model
        |       |-- model.mlir
        |       |-- module.dnn_layer.linear_layers.0.bias
        |       |-- module.dnn_layer.linear_layers.0.weight
        |       |-- module.dnn_layer.linear_layers.12.bias
        |       |-- module.dnn_layer.linear_layers.12.weight
        |       |-- module.dnn_layer.linear_layers.15.bias
        |       |-- module.dnn_layer.linear_layers.15.weight
        |       |-- module.dnn_layer.linear_layers.3.bias
        |       |-- module.dnn_layer.linear_layers.3.weight
        |       |-- module.dnn_layer.linear_layers.6.bias
        |       |-- module.dnn_layer.linear_layers.6.weight
        |       |-- module.dnn_layer.linear_layers.9.bias
        |       |-- module.dnn_layer.linear_layers.9.weight
        |       |-- module.embedding_layer.one_embedding.shadow
        |       `-- one_embedding_options.json
        `-- config.pbtxt
    ```

2. 其中，`config.pbtxt` 中的 `name` 要和 config.pbtxt 所在目录的名字（本例中为 `embedding`）保持一致；`model/embedding/1/model/one_embedding_options.json` 的路径 `persistent_table.path` 会自动生成，可以再确认下路径是否正确，应为 `$demodir/oneflow_process/persistent` 绝对路径。

#### 启动 OneFlow 推理服务

使用以下命令启动 OneFlow 推理服务：

```
docker run --runtime=nvidia --rm -p 8001:8001 -p8000:8000 -p 8002:8002 \
  -v $demodir/oneflow_process/model:/models \
  -v $demodir/oneflow_process/persistent:/root/demo/persistent \
  oneflowinc/oneflow-serving:nightly \
  bash -c '/opt/tritonserver/bin/tritonserver --model-repository=/models'
```

若成功，将显示如下类似输出：

```
...
I0929 07:28:34.281655 1 grpc_server.cc:4117] Started GRPCInferenceService at 0.0.0.0:8001
I0929 07:28:34.282343 1 http_server.cc:2815] Started HTTPService at 0.0.0.0:8000
I0929 07:28:34.324662 1 http_server.cc:167] Started Metrics Service at 0.0.0.0:8002
```

启动后，可访问 `http://127.0.0.1:8000` 进行推理。

可通过以下方式测试服务是否启动，如果出现 `Connection refused`，说明服务启动失败：

```
curl -v localhost:8000/v2/health/ready
```

```{note}

如果 800x 端口出现冲突，可以更改端口映射中的主机端口，例如 `-p 18000:8000` 。如果更改了 8000 的映射，后续访问 8000 的都需要相应更改。

```

### 启动推理服务

```{note}

以下命令可在物理机环境执行，由于 Python 依赖，推荐 OneFlow 虚拟环境中执行。

```

脚本中参数使用 `127.0.0.1:9080` 作为 OpenMLDB ApiServer 地址，`127.0.0.1:8000` 作为 OneFlow Triton 地址。

```bash
sh $demodir/serving/start_predict_server.sh
```

你可以通过查看日志文件 `/tmp/p.log`，获得 predict server 的运行日志。

### 发送预估请求

执行 `predict.py` 脚本。该脚本发送一行请求数据到预估服务，接收返回的预估结果，并打印出来。

```bash
python $demodir/serving/predict.py
```

输出范例：

```
----------------ins---------------

['200080_5505_2016-03-15 20:43:04' 1458045784000
 '200080_5505_2016-03-15 20:43:04' '200080_5505' '5505' '200080' 1 1.0 1.0
 1 1 3 1 '200080_5505_2016-03-15 20:43:04' None '3' '1' '1' '214' '8'
 1603438960564 None None None None '200080_5505_2016-03-15 20:43:04'
 0.02879999950528145 0.0 0.0 2 2 '1,,NULL' '4,0,NULL'
 '200080_5505_2016-03-15 20:43:04' ',NULL,NULL' ',NULL,NULL' ',NULL,NULL'
 1 1 1 ',NULL,NULL' ',NULL,NULL']

---------------predict change of purchase -------------

[[b'0.007005:0']]

```
