For full instructions, please refer to [website page](http://openmldb.ai/docs/zh/main/use_case/JD_recommendation.html) or [github md](https://github.com/4paradigm/OpenMLDB/blob/main/docs/zh/use_case/JD_recommendation.md)

# Pre

1. env var `demodir`: the dir of demo source

1. Oneflow Env: use conda

# Training

## OpenMLDB Feature Extraction

```
docker run -dit --name=openmldb --network=host -v $demodir:/work/oneflow_demo 4pdosc/openmldb:0.6.9 bash
docker exec -it openmldb bash
```

in docker:

1. start cluster and create tables, load offline data
```
/work/init.sh

/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/create_tables.sql

/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/load_offline_data.sql
```

2. waiting for load jobs done
```
echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

3. feature extraction
```
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /work/oneflow_demo/sql_scripts/sync_select_out.sql
```
The feature data will be saved in `/work/oneflow_demo/out/1` in docker container file system.

## Oneflow Train

Use conda env:
```
conda create -y -n oneflow python=3.9.2
conda activate oneflow
pip install -f https://staging.oneflow.info/branch/master/cu112 --pre oneflow
pip install psutil petastorm pandas sklearn xxhash "tritonclient[all]" geventhttpclient tornado
```

1. Preprocess:
```
cd $demodir/feature_preprocess/
python preprocess.py $demodir/out/1
```
1. Train:
```
cd $demodir/oneflow_process/
bash train_deepfm.sh $demodir/feature_preprocess/out
```

## Model Serving

1. Configure openmldb for online feature extraction:
```
##in openmldb docker
docker exec -it demo bash
##deploy feature extraction
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/deploy.sql
##load online data
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/load_online_data.sql
```

2. Configure oneflow for model serving

check if config.pbtxt, model files, `model/embedding/1/model/one_embedding_options.json` persistent path are correcly set

3. Start oneflow serving 

```
docker run --runtime=nvidia --rm -p 8001:8001 -p8000:8000 -p 8002:8002 \
  -v $demodir/oneflow_process/model:/models \
  -v $demodir/oneflow_process/persistent:/root/demo/persistent \
  oneflowinc/oneflow-serving:nightly \
  bash -c '/opt/tritonserver/bin/tritonserver --model-repository=/models'
```

cd openmldb_serving/
## Test

1. start predict server(connect openmldb & oneflow service)
1. send a predict request
```
sh $demodir/serving/start_predict_server.sh
python $demodir/serving/predict.py
```
