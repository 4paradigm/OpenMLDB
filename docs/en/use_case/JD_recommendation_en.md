
#  OpenMLDB + OneFlow: Prediction of Purchase Intention for High Potential Customers

In this article, we will use [JD Prediction of purchase intention for high potential customers problem](https://jdata.jd.com/html/detail.html?id=1) as a demonstration，to show how we can use [OpenMLDB](https://github.com/4paradigm/OpenMLDB) and [OneFlow](https://github.com/Oneflow-Inc/oneflow) together to build a complete machine learning application. Full dataset [download here](https://openmldb.ai/download/jd-recommendation/JD_data.tgz).


Extracting patterns from historical data to predict the future purchase intentions, to bring together the most suitable products and customers who need them most, is the key issue in the application of big data in precision marketing, and is also the key technology in digitalization for all e-commerce platforms. As the largest self-operated e-commerce company in China, JD.com has accumulated hundreds of millions of loyal customers and massive amounts of real-life data. This demonstration is based on the real-life data, including real customers, product and behavior data (after desensitization) from Jingdong Mall, and utilizes data mining technology and machine learning algorithm to build a prediction model for user purchase intentions, and output matching results between high-potential customers and target products. This aims to provide high-quality target groups for precision marketing, mine the potential meaning behind the data, and provide e-commerce customers with a simpler, faster and more worry-free shopping experience. In this demonstration, OpenMLDB is used for data mining, and the [DeepFM](https://github.com/Oneflow-Inc/models/tree/main/RecommenderSystems/deepfm) model in OneFlow is used for high-performance training and inference to provide accurate product recommendations.

Note that: (1) this case is based on the OpenMLDB cluster version for tutorial demonstration; (2) this document uses the pre-compiled docker image. If you want to test it in the OpenMLDB environment compiled and built by yourself, you need to configure and use our [Spark Distribution for Feature Engineering Optimization](https://github.com/4paradigm/spark). Please refer to relevant documents of [compilation](https://openmldb.ai/docs/en/main/deploy/compile.html) (Refer to Chapter: "Spark Distribution Optimized for OpenMLDB") and the [installation and deployment documents](https://openmldb.ai/docs/en/main/deploy/install_deploy.html) (Refer to the section: [Deploy TaskManager](https://openmldb.ai/docs/en/main/deploy/install_deploy.html#deploy-taskmanager)).

## 1. Preparation and Preliminary Knowledge
### 1.1 OneFlow Installation
OneFlow framework leverage on the great computational power from GPU. Therefore please ensure that the machines for deployment are equipped with NVidia GPUs, and ensure the driver version is >=460.X.X  [driver version support for CUDA 11.0](https://docs.nvidia.com/cuda/cuda-toolkit-release-notes/index.html#cuda-major-component-versions).
Install OneFlow with the following commands：
```bash
conda activate oneflow
python3 -m pip install -f https://staging.oneflow.info/branch/master/cu112 --pre oneflow
```
In addition, following Python packages need to be installed:
```bash
pip install psutil petastorm pandas sklearn
```
 Pull Oneflow-serving docker image：
```bash
docker pull oneflowinc/oneflow-serving:nightly
```
```{note}
Note that we are installing Oneflow nightly versions here. The versions tested in this guide are as follows:
Oneflow：https://github.com/Oneflow-Inc/oneflow/tree/fcf205cf57989a5ecb7a756633a4be08444d8a28
Oneflow-serving：https://github.com/Oneflow-Inc/serving/tree/ce5d667468b6b3ba66d3be6986f41f965e52cf16
```


### 1.2 Pull and Start the OpenMLDB Docker Image
- Note: Please make sure that the Docker Engine version number is > = 18.03
- Pull the OpenMLDB docker image and run the corresponding container
- Download demo files, and map the demo directory to `/root/project`, here we use `demodir=/home/gtest/demo`. The demo files include scripts and sample training data required for this case.
```bash
export demodir=/home/gtest/demo
docker run -dit --name=demo --network=host -v $demodir:/root/project 4pdosc/openmldb:0.5.2 bash
docker exec -it demo bash
```
- The image is preinstalled with OpenMLDB and some third-party libraries and tools, we need to install the dependencies of OneFlow.

Since we embed the data pre-processing and invoking of OneFlow serving in the OpenMLDB docker, following dependencies needs to be installed.
```bash
pip install tritonclient[all] xxhash geventhttpclient
```

```{note}
Note that all the commands for OpenMLDB part below run in the docker container by default. All the commands for OneFlow are to run in the environment as installed in 1.1.
```


### 1.3  Initialize Environment

```bash
./init.sh
```
We provide the init.sh script in the image that helps users to quickly initialize the environment including:
- Configure zookeeper
- Start cluster version OpenMLDB

### 1.4 Start OpenMLDB CLI Client
```bash
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```
```{note}
Note that most of the commands in this tutorial are executed under the OpenMLDB CLI. In order to distinguish from the ordinary shell environment, the commands executed under the OpenMLDB CLI use a special prompt of >.
```

### 1.5 Preliminary Knowledge: Non-Blocking Task of Cluster Version
Some commands in the cluster version are non-blocking tasks, including `LOAD DATA` in online mode and `LOAD DATA`, `SELECT`, `SELECT INTO` commands in the offline mode. After submitting a task, you can use relevant commands such as `SHOW JOBS` and `SHOW JOB` to view the task progress. For details, see the offline task management document.

## 2. Machine Learning Process Based on OpenMLDB and OneFlow

### 2.1 Overview
Machine learning with OpenMLDB and OneFlow can be summarized into a few main steps. We will detail each step in the following sections. 

### 2.2 Offline feature extraction with OpenMLDB
#### 2.2.1 Creating Databases and Data Tables
The following commands are executed in the OpenMLDB CLI environment.
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
You can also use sql script to execute (`/root/project/create_tables.sql`) as shown below:

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/create_tables.sql
```

#### 2.2.2 Offline Data Preparation
First, you need to switch to offline execution mode. Next, import the sample data as offline data for offline feature calculation. 

The following commands are executed under the OpenMLDB CLI.

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
or use script to execute, and check the job status with the following commands:

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/load_data.sql

echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```


```{note}
Note that `LOAD DATA` is a non-blocking task. You can use the command `SHOW JOBS` to view the running status of the task. Please wait for the task to run successfully (`state` to `FINISHED` status) before proceeding to the next step.
```

#### 2.2.3 The Feature Extraction Script
Usually, users need to analyse the data according to the goal of machine learning before designing the features, and then design and investigate the features according to the analysis. Data analysis and feature research of the machine learning are not the scope of this demo, and we will not expand it. We assume that users already have the basic theoretical knowledge of machine learning, the ability to solve machine learning problems, the ability to understand SQL syntax, and the ability to use SQL syntax to construct features. For this case, we have designed several features after the analysis and research.

#### 2.2.4 Offline Feature Extraction
In the offline mode, the user extracts features and outputs the feature results to `'/root/project/out/1`(mapped to`$demodir/out/1`) that is saved in the data directory for subsequent model training. The `SELECT` command corresponds to the SQL feature extraction script generated based on the above table. The following commands are executed under the OpenMLDB CLI.
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
Since there is only one command, we can directly execute the sql script `sync_select_out.sql`:

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/sync_select_out.sql
```
```{note}
Note that the cluster version `SELECT INTO` is a non-blocking task. You can use the command `SHOW JOBS` to view the running status of the task. Please wait for the task to run successfully (`state` to `FINISHED` status) before proceeding to the next step.
```
### 2.3 Pre-process Dataset to Match DeepFM Model Requirements
```{note}
Note that following commands are executed outside the demo docker. They are executed in the environment as installed in section 1.1.
```
According to [DeepFM paper](https://arxiv.org/abs/1703.04247), we treat both categorical and continuous features as sparse features.

> χ may include categorical fields (e.g., gender, location) and continuous fields (e.g., age). Each categorical field is represented as a vector of one-hot encoding, and each continuous field is represented as the value itself, or a vector of one-hot encoding after discretization.

Change directory to demo directory and execute the following commands to process the data set.
```bash
cd $demodir/openmldb_process/
bash process_JD_out_full.sh $demodir/out/1
```
The generated dataset will be placed at `$demodir/openmldb_process/out`. After generating parquet dataset, dataset information will also be printed. It contains the information about the number of samples and table size array, which is needed when training.
```
train samples = 11073
val samples = 1351
test samples = 1492
table size array:
4,26,16,4,11,809,1,1,5,3,17,16,7,13916,13890,13916,10000,3674,9119,7,2,13916,5,4,4,33,2,2,7,2580,3,5,13916,10,47,13916,365,17,132,32,37
```

### 2.4 Launch OneFlow for Model Training
```{note}
Note that following commands are executed in the environment as installed in section 1.1.
```
#### 2.4.1 Update `train_deepfm.sh` Configuration File
The dataset information generated from the previous section need to be updated in the configuration file,including `num_train_samples`,`num_val_samples`,`num_test_samples` and `table_size_array`.
```bash
cd $demodir/oneflow_process/
```
```bash
#!/bin/bash
DEVICE_NUM_PER_NODE=1
demodir="$1"
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
--table_size_array "4,26,16,4,11,809,1,1,5,3,17,16,7,13916,13890,13916,10000,3674,9119,7,2,13916,5,4,4,33,2,2,7,2580,3,5,13916,10,47,13916,365,17,132,32,37" \
--store_type 'cached_host_mem' \
--cache_memory_budget_mb 1024 \
--batch_size 1000 \
--train_batches 75000 \
--loss_print_interval 100 \
--dnn "1000,1000,1000,1000,1000" \
--net_dropout 0.2 \
--learning_rate 0.001 \
--embedding_vec_size 16 \
--num_train_samples 11073 \
--num_val_samples 1351 \
--num_test_samples 1492 \
--model_save_dir $MODEL_SAVE_DIR \
--save_best_model \
--save_graph_for_serving \
--model_serving_path $MODEL_SERVING_PATH \
--save_model_after_each_eval
```
#### 2.4.2 Start Model Training
```bash
bash train_deepfm.sh $demodir
```
Trained model will be saved in `$demodir/oneflow_process/model_out`, saved model for serving will be saved in `$demodir/oneflow_process/model/embedding/1/model`.

## 3. Model Serving
### 3.1 Overview
Model serving with OpenMLDB+OneFlow can be summarized into a few main steps. We will detail each step in the following sections. 

### 3.2 Configure OpenMLDB for Online Feature Extraction 

#### 3.2.1 Online SQL Deployment
Assuming that the model produced by the features designed in Section 2.2.3 in the previous model training meets the expectation. The next step is to deploy the feature extraction SQL script online to provide real-time feature extraction.

1. Restart OpenMLDB CLI for SQL online deployment.
   ```bash
   docker exec -it demo bash
   /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
   ```
2. To execute online deployment, the following commands are executed in OpenMLDB CLI. 
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

Use the following command to check the deployment details:
```sql
show deployment demo;
```
#### 3.2.2 Online Data Import
We need to import the data for real-time feature extraction. First, you need to switch to **online** execution mode. Then, in the online mode, import the sample data as the online data source. The following commands are executed under the OpenMLDB CLI.
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
Note that the cluster version `LOAD  DATA` is a non-blocking task. You can use the command `SHOW JOBS` to view the running status of the task. Please wait for the task to run successfully (`state` to `FINISHED` status) before proceeding to the next step.
```

### 3.3 Configure OneFlow Model Serving

#### 3.3.1 Check Model Path (`$demodir/oneflow_process/model`)
Check if model files are correctly organized and saved as shown below:
```
$ tree  -L 5 model/
model/
└── embedding
    ├── 1
    │   └── model
    │       ├── model.mlir
    │       ├── module.dnn_layer.linear_layers.0.bias
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.0.weight
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.12.bias
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.12.weight
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.15.bias
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.15.weight
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.3.bias
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.3.weight
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.6.bias
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.6.weight
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.9.bias
    │       │   ├── meta
    │       │   └── out
    │       ├── module.dnn_layer.linear_layers.9.weight
    │       │   ├── meta
    │       │   └── out
    │       ├── module.embedding_layer.one_embedding.shadow
    │       │   ├── meta
    │       │   └── out
    │       └── one_embedding_options.json
    └── config.pbtxt
 ```   

#### 3.3.2 Check `config.pbtxt` configurations.
```
name: "embedding"
backend: "oneflow"
max_batch_size: 10000
input [
  {
    name: "INPUT_0"
    data_type: TYPE_INT64
    dims: [ 41 ]
  }
]
output [
  {
    name: "OUTPUT_0"
    data_type: TYPE_FP32
    dims: [ 1 ]
  }
]
instance_group [
  {
    count: 1
    kind: KIND_GPU
    gpus: [ 0 ]
  }
]
 ```
Field `name` in `config.pbtxt` should be consistent with the name of the folder.

#### 3.3.3 Change persistent path
change persistent table path in `one_embedding_options.json`. Change `embedding/kv_options/kv_store/persistent_table/path` to persistent table location in docker `/root/demo/persistent`.
```
{
    "embedding": [
        {
            "snapshot": "2022-09-29-03-27-44-953674",
            "kv_options": {
                "name": "sparse_embedding",
                "key_type_size": 8,
                "value_type_size": 4,
                "value_type": "oneflow.float32",
                "storage_dim": 51,
                "kv_store": {
                    "caches": [
                        {
                            "policy": "lru",
                            "cache_memory_budget_mb": 1024,
                            "value_memory_kind": "device"
                        },
                        {
                            "policy": "full",
                            "capacity": 110477,
                            "value_memory_kind": "host"
                        }
                    ],
                    "persistent_table": {
                        "path": "/root/demo/persistent",
                        "physical_block_size": 4096,
                        "capacity_hint": 110477
                    }
                },
                "parallel_num": 1
            }
        }
    ]
}
```

### 3.4 Start Serving
#### 3.4.1 Start OneFlow Model Serving
```{note}
Note that following commands are executed in the environment as installed in section 1.1.
```
Start OneFlow model serving with the following commands:
```bash
docker run --runtime=nvidia --rm --network=host \
  -v $demodir/oneflow_process/model:/models \
  -v $demodir/oneflow_process/persistent:/root/demo/persistent \
  oneflowinc/oneflow-serving:nightly \
  bash -c '/opt/tritonserver/bin/tritonserver --model-repository=/models'
  --model-repository=/models --backend-directory=/backends'
```
If sucessful, the output will look like the following:
'''
...
I0929 07:28:34.281655 1 grpc_server.cc:4117] Started GRPCInferenceService at 0.0.0.0:8001
I0929 07:28:34.282343 1 http_server.cc:2815] Started HTTPService at 0.0.0.0:8000
I0929 07:28:34.324662 1 http_server.cc:167] Started Metrics Service at 0.0.0.0:8002

'''

#### 3.4.2 Start OpenMLDB Serving
```{note}
Note that the following commands are executed in demo docker.
```
OpenMLDB online feature extraction has been deployed, and oneflow model serving is started. This demo connects both services. After receiving real-time requests, OpenMLDB service is firstly enaged for feature extraction. After which the extracted features are passed to oneflow model serving for inference. 
1. If you have not exited the OpenMLDB CLI, use the `quit` command to exit the OpenMLDB CLI.
2. Start the prediction service from the command line:
```bash
cd /root/project/serving/openmldb_serving
./start_predict_server.sh 0.0.0.0:9080
```

### 3.5 Send Real-Time Request
Requests can be executed outside the OpenMLDB docker. The details can be found in [IP Configuration](https://openmldb.ai/docs/en/main/reference/ip_tips.html).

Execute `predict.py` in command window. This script will send a line of request data to the prediction service. Results will be received and printed out.

```bash
python $demodir/serving/predict.py
```
Sample output:
```
----------------ins---------------
['200001_80005_2016-03-31 18:11:20' 1459419080000
 '200001_80005_2016-03-31 18:11:20' '200001_80005' '80005' '200001' 1 1.0
 1.0 1 1 5 1 '200001_80005_2016-03-31 18:11:20' None None None None None
 None None None None None None '200001_80005_2016-03-31 18:11:20'
 0.019200000911951065 0.0 0.0 2 2 '1,,NULL' '4,0,NULL'
 '200001_80005_2016-03-31 18:11:20' ',NULL,NULL' ',NULL,NULL' ',NULL,NULL'
 1 1 1 ',NULL,NULL' ',NULL,NULL']
---------------predict change of purchase -------------
[[b'0.006222:0']]
```
