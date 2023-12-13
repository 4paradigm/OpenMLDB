
#  OpenMLDB + OneFlow: Prediction of Purchase Intention for High Potential Customers

In this article, we will use [JD Prediction of purchase intention for high potential customers problem](https://jdata.jd.com/html/detail.html?id=1) as a demonstration，to show how we can use [OpenMLDB](https://github.com/4paradigm/OpenMLDB) and [OneFlow](https://github.com/Oneflow-Inc/oneflow) together to build a complete machine learning application. Full dataset [download here](https://openmldb.ai/download/jd-recommendation/JD_data.tgz).

## Background

Extracting patterns from historical data to predict future purchase intentions, to bring together the most suitable products and customers who need them most, is the key issue in the application of big data in precision marketing, and is also the key technology in digitalization for all e-commerce platforms. As the largest self-operated e-commerce company in China, JD.com has accumulated hundreds of millions of loyal customers and massive amounts of real-life data. 

This demonstration is based on real-life data, including real customers, product and behavior data (after desensitization) from Jingdong Mall, and utilizes data mining technology and machine learning algorithm to build a prediction model for user purchase intentions, and output matching results between high-potential customers and target products. This aims to provide high-quality target groups for precision marketing, mine the potential meaning behind the data, and provide e-commerce customers with a simpler, faster, and more worry-free shopping experience. 

In this demonstration, OpenMLDB is used for data mining, and the [DeepFM](https://github.com/Oneflow-Inc/models/tree/main/RecommenderSystems/deepfm) model in OneFlow is used for high-performance training and inference to provide accurate product recommendations.

```{note}
Note that this document uses the pre-compiled docker image. If you want to test it in the OpenMLDB environment compiled and built by yourself, please refer to relevant documents of [compilation](https://openmldb.ai/docs/en/main/deploy/compile.html) and the [installation and deployment documents](https://openmldb.ai/docs/en/main/deploy/install_deploy.html).
```

## Preparation and Preliminary Knowledge

### Download Demo Materials

Download demo data and scripts.
```
wget https://openmldb.ai/download/jd-recommendation/demo-0.8.1.tgz
tar xzf demo.tgz
ls jd-recommendation/
```
or you can checkout branch `demo/jd-recommendation`. The directory of this demo is set as `demodir`, which will be extensively used in the scripts. Therefore you need to set this environment variable:
```
export demodir=<your_path>/demo
```

We'll use the small dataset in demo.tgz. If you want to test on full dataset, please download [JD_data](http://openmldb.ai/download/jd-recommendation/JD_data.tgz).

### OneFlow Installation
OneFlow framework leverages on the great computational power from GPU. Therefore please ensure that the machines for deployment are equipped with NVidia GPUs, and ensure the driver version is >=460.X.X  [driver version support for CUDA 11.0](https://docs.nvidia.com/cuda/cuda-toolkit-release-notes/index.html#cuda-major-component-versions).
Install OneFlow with the following commands：
```bash
conda create -y -n oneflow python=3.9.2
conda activate oneflow
pip install numpy==1.23 nvidia-cudnn-cu11 # for oneflow
pip install -f https://release.oneflow.info oneflow==0.9.0+cu112
pip install psutil petastorm pandas sklearn xxhash "tritonclient[all]" geventhttpclient tornado
```

Pull Oneflow-serving docker image：
```bash
docker pull oneflowinc/oneflow-serving:nightly
```
```{note}
Note that we are installing Oneflow nightly versions here. The versions tested in this guide are as follows:
Oneflow：https://github.com/Oneflow-Inc/oneflow/tree/fcf205cf57989a5ecb7a756633a4be08444d8a28
Oneflow-serving：https://github.com/Oneflow-Inc/serving/tree/ce5d667468b6b3ba66d3be6986f41f965e52cf16
If this docker image is not available, you can use this [backup image](https://openmldb.ai/download/jd-recommendation/oneflow-image.tar.gz)，and then use `docker load < oneflow-image.tar.gz`.
```

### Pull and Start the OpenMLDB Docker Image
Pull the OpenMLDB docker image and run.
- Docker: >=18.03

Since the OpenMLDB cluster needs to communicate with other components, we will use the host network straightaway. In this example, we will use downloaded scripts in the docker, therefore we map the `demodir` directory into the docker container.
```bash
docker run -dit --name=openmldb --network=host -v $demodir:/work/oneflow_demo 4pdosc/openmldb:0.8.4 bash
docker exec -it openmldb bash
```

```{note}
Note that all the commands for OpenMLDB part below run in the docker container by default. All the commands for OneFlow are to run in the virtual environment `oneflow`.
```

### Start OpenMLDB cluster

In container:
```bash
/work/init.sh
```
We provide the init.sh script in the image that helps users to quickly initialize the environment including:
- Configure zookeeper
- Start cluster version OpenMLDB

### Start OpenMLDB CLI Client
```bash
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

### Preliminary
Some commands in the cluster version are non-blocking tasks, including `LOAD DATA` in online mode and `LOAD DATA`, `SELECT`, `SELECT INTO` commands in online/offline mode. After submitting a task, you can use relevant commands such as `SHOW JOBS` and `SHOW JOB` to view the task progress. For details, see the [offline task management document](../openmldb_sql/task_manage/SHOW_JOB.md).

## Machine Learning Process Based on OpenMLDB and OneFlow

### Overview
Machine learning with OpenMLDB and OneFlow can be summarized into a few main steps:
1. OpenMLDB offline feature design and extraction (SQL)
2. OneFlow model training
3. SQL and model serving
   
We will detail each step in the following sections. 

### Offline feature extraction with OpenMLDB
The following commands are all executed in OpenMLDB CLI.

#### Creating Databases and Data Tables
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
You can also use sql script to execute (`/work/oneflow_demo/sql_scripts/create_tables.sql`) as shown below:

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/create_tables.sql
```

#### Offline Data Preparation
First, you need to switch to offline execution mode. Next, import the sample data as offline data for offline feature calculation. If you are importing a larger dataset, you can consider using soft links to reduce import time. In this demo, only small amount of data is imported, thus we use hard copy. For multiple data imports, the asynchronous mode will be more time-efficient. But you need to make sure that all imports are done before going into the next step.

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
or use a script to execute, and check the job status with the following commands:

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/load_offline_data.sql
echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

```{note}
Note that `LOAD DATA` is a non-blocking task. You can use the command `SHOW JOBS` to view the running status of the task. Please wait for the task to run successfully (`state` to `FINISHED` status) before proceeding to the next step.
```

#### The Feature Extraction Script
Usually, users need to analyze the data according to the goal of machine learning before designing the features, and then design and investigate the features according to the analysis. Data analysis and feature research of machine learning are not in the scope of this demo, and we will not expand it. We assume that users already have the basic theoretical knowledge of machine learning, the ability to solve machine learning problems, the ability to understand SQL syntax, and the ability to use SQL syntax to construct features. For this case, we have designed several features after the analysis and research.

In the actual process of machine learning feature exploration, scientists repeatedly experiment with features, seeking the best feature set for model effectiveness. Therefore, they continuously repeat the process of "feature design -> offline feature extraction -> model training," constantly adjusting features to achieve the desired results.

#### Offline Feature Extraction
In the offline mode, the user extracts features and outputs the feature results to `'/work/oneflow_demo/out/1`(mapped to`$demodir/out/1`) which is saved in the data directory for subsequent model training. The `SELECT` command corresponds to the SQL feature extraction script generated based on the above table. The following commands are executed under the OpenMLDB CLI.
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
    top1_ratio(`pair_id`) over flattenRequest_user_id_eventTime_0_10_ as flattenRequest_pair_id_window_top1_ratio_28,
    top1_ratio(`pair_id`) over flattenRequest_user_id_eventTime_0s_14d_200 as flattenRequest_pair_id_window_top1_ratio_29,
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
    topn_frequency(`has_bad_comment`, 3) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_has_bad_comment_multi_top3frequency_30,
    topn_frequency(`comment_num`, 3) over bo_comment_sku_id_ingestionTime_0s_64d_100 as bo_comment_comment_num_multi_top3frequency_33
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
    topn_frequency(`br`, 3) over bo_action_pair_id_ingestionTime_0s_10h_100 as bo_action_br_multi_top3frequency_16,
    topn_frequency(`cate`, 3) over bo_action_pair_id_ingestionTime_0s_10h_100 as bo_action_cate_multi_top3frequency_17,
    topn_frequency(`model_id`, 3) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_model_id_multi_top3frequency_18,
    distinct_count(`model_id`) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_model_id_multi_unique_count_19,
    distinct_count(`model_id`) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_model_id_multi_unique_count_20,
    distinct_count(`type`) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_type_multi_unique_count_21,
    topn_frequency(`type`, 3) over bo_action_pair_id_ingestionTime_0s_7d_100 as bo_action_type_multi_top3frequency_40,
    topn_frequency(`type`, 3) over bo_action_pair_id_ingestionTime_0s_14d_100 as bo_action_type_multi_top3frequency_42
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
Note that the cluster version `SELECT INTO` is a non-blocking task. You can use the command `SHOW JOBS` to view the running status of the task. Please wait for the task to run successfully (`state` to `FINISHED` status) before proceeding to the next step. It takes around 1.5 minites.
```
Since there is only one command, we can directly execute the sql script `sync_select_out.sql`:

```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/sync_select_out.sql
```

### Pre-process Dataset to Match DeepFM Model Requirements
```{note}
Note that following commands are executed outside the demo docker. They are executed in the virtual environment for OneFlow.
```
According to [DeepFM paper](https://arxiv.org/abs/1703.04247), we treat both categorical and continuous features as sparse features.

> χ may include categorical fields (e.g., gender, location) and continuous fields (e.g., age). Each categorical field is represented as a vector of one-hot encoding, and each continuous field is represented as the value itself, or a vector of one-hot encoding after discretization.

Change directory to demo directory and execute the following commands to process the data set.
```bash
cd $demodir/feature_preprocess/
python preprocess.py $demodir/out/1
```

`$demodir/out/1` is the feature path generated by the last step. The generated dataset will be placed at `$demodir/feature_preprocess/out`, include 3 dataset, train,test and valid. And we'll save the number of rows in 3 datasets and `table_size_array` into `data_info.txt`(We can use the info file, avoid coping parameters manually). The output of preprocess is similar to the following:
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
And the tree of `out` path is：
```
out/
├── data_info.txt
├── test
│   └── test.parquet
├── train
│   └── train.parquet
└── valid
    └── valid.parquet

3 directories, 4 files
```

### Launch OneFlow for Model Training
```{note}
Note that the following commands are executed in the virtual environment for OneFlow.
```

```bash
cd $demodir/oneflow_process/
sh train_deepfm.sh -h
Usage: train_deepfm.sh DATA_DIR(abs)
        We'll read required args in $DATA_DIR/data_info.txt, and save results in path ./
```
The training in OneFlow is done with the script `train_deepfm.sh`, with usage shown above. Normally, no special configurations are required. The scripts will read the parameters from `$DATA_DIR/data_info.txt`, including `num_train_samples`, `num_val_samples`, `num_test_samples` and `table_size_array`. Please use the output directory as follows:

```bash
bash train_deepfm.sh $demodir/feature_preprocess/out
```

The trained model will be saved in `$demodir/oneflow_process/model_out`, saved model for serving will be saved in `$demodir/oneflow_process/model/embedding/1/model`.

## Model Serving
### Overview
Model serving with OpenMLDB+OneFlow can be summarized into a few main steps. 
1. OpenMLDB deploying: deploy SQL and prepare the online data
2. Oneflow serving: load model
3. Predict serving demo
   
We will detail each step in the following sections. 

### OpenMLDB Deploying

#### Online SQL Deployment
Assuming that the model produced by the features designed in the previous model training meets the expectation. The next step is to deploy the feature extraction SQL script online to provide real-time feature extraction. In OpenMLDB docker（if exited, enter with `docker exec -it openmldb bash`):

1. Restart OpenMLDB CLI for SQL online deployment.
   ```bash
   /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
   ```
2. Deploy the sql(see [Offline Feature Extracion](#offline-feature-extraction)) 
```sql
-- OpenMLDB CLI
USE JD_db;
DEPLOY demo OPTIONS(RANGE_BIAS='inf', ROWS_BIAS='inf') <SQL>;
```
Or you can deploy with script inside the docker:
```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/deploy.sql
```

Use the following command to check the deployment details:
```sql
show deployment demo;
```
After deployment, you can access the service through OpenMLDB ApiServer `127.0.0.1:9080`.

#### Online Data Import
We need to import the data for real-time feature extraction. For simplicity, we directly import and use the same dataset as offline. In production, typically the offline dataset comprises a large volume of cold data, while the online dataset consists of recent hot data. 

The following commands are executed under the OpenMLDB CLI.
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

You can run the script:
```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/load_online_data.sql
```
And check the import job status by:
```
  echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

```{note}
Note that the cluster version `LOAD  DATA` is a non-blocking task. You can use the command `SHOW JOBS` to view the running status of the task. Please wait for the task to run successfully (`state` to `FINISHED` status) before proceeding to the next step.
```

### Oneflow Serving

#### Check Config

Check if model files `$demodir/oneflow_process/model` are correctly organized and saved as shown below:
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
Field `name` in `config.pbtxt` should be consistent with the name of the folder(`embedding`). And `persistent_table.path` will be generated automatically in `model/embedding/1/model/one_embedding_options.json`, you can check if it's the absolute path of`$demodir/oneflow_process/persistent`.

3.3.2 Start OneFlow serving
Start OneFlow model serving with the following commands:
```
docker run --runtime=nvidia --rm -p 8001:8001 -p8000:8000 -p 8002:8002 \
  -v $demodir/oneflow_process/model:/models \
  -v $demodir/oneflow_process/persistent:/root/demo/persistent \
  oneflowinc/oneflow-serving:nightly \
  bash -c '/opt/tritonserver/bin/tritonserver --model-repository=/models'
```
If successful, the output will look like the following:
```
I0711 09:58:55.199227 1 server.cc:549]
+---------+---------------------------------------------------------+--------+
| Backend | Path                                                    | Config |
+---------+---------------------------------------------------------+--------+
| oneflow | /opt/tritonserver/backends/oneflow/libtriton_oneflow.so | {}     |
+---------+---------------------------------------------------------+--------+

I0711 09:58:55.199287 1 server.cc:592]
+-----------+---------+--------+
| Model     | Version | Status |
+-----------+---------+--------+
| embedding | 1       | READY  |
+-----------+---------+--------+
...
I0929 07:28:34.281655 1 grpc_server.cc:4117] Started GRPCInferenceService at 0.0.0.0:8001
I0929 07:28:34.282343 1 http_server.cc:2815] Started HTTPService at 0.0.0.0:8000
I0929 07:28:34.324662 1 http_server.cc:167] Started Metrics Service at 0.0.0.0:8002
```

We can request `http://127.0.0.1:8000` to do predict. You can check if the serving is working by:
```
curl -v localhost:8000/v2/health/ready
```
If the response is `Connection refused`, the serving failed to start.
Furthermore, check if model is successfully loaded:
```
curl -v localhost:8000/v2/models/stats
```
If successful, you will be able to see `embedding` information. Else, check the model path and the organization.
```{note}
If port 800x confict, you can change the host port. For example, use `-p 18000:8000`. If you change the host port mapping of 8000, you should change the oneflow request port in the predict server demo too.
```

### Predict Serving Demo

```{note}
Note that the following commands can be executed in any environment. Because of Python dependencies, we recommend using the virtual environment of OneFlow.
```

Upon receiving a request, the prediction service first obtains real-time features through OpenMLDB and the request the inference service with real-time features. The script uses `127.0.0.1:9080` to query OpenMLDB ApiServer, and `127.0.0.1:8000` to query OneFlow Triton serving.

```bash
sh $demodir/serving/start_predict_server.sh
```
You can check execution logs from `/tmp/p.log`.

### Send Real-Time Request to test
Requests can be executed outside the OpenMLDB docker. The details can be found in [IP Configuration](https://openmldb.ai/docs/en/main/reference/ip_tips.html).

`predict.py` will send a line of request data to the prediction service. Results will be received and printed out.

```bash
python $demodir/serving/predict.py
```
Sample output:
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
```{note}
If an error occurs, use client.py in the serving directory, or [download](https://github.com/4paradigm/OpenMLDB/blob/f2d985c986c5c4cbe538b01dabdbd1956588da40/demo/jd-recommendation/serving/client.py), to separately debug triton infer.
```
