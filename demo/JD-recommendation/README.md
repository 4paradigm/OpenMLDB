For full instructions, please refer to https://github.com/4paradigm/OpenMLDB/blob/main/docs/zh/use_case/JD_recommendation.md 

#Training:
1. Engage openmldb for feature extraction:
##in openmldb docker
docker exec -it demo bash
##launch openmldb CLI
./init.sh
##create data tables
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/create_tables.sql
##load offline data
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/load_data.sql
echo "show jobs;" | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
##select features
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/sync_select_out.sql

2. process openmldb output data:
##outside openmldb docker
conda activate oneflow

cd openmldb_process
##pass in directory of openmldb results
bash process_JD_out_full.sh $demodir/out/1
##output data in $demodir/openmldb_process/out
##note output information, table_size_array

3. Launch oneflow deepfm model training:
cd oneflow_process/
##modify directory, sample size, table_size_array information in train_deepfm.sh accordingly
bash train_deepfm.sh $demodir


#Model Serving
1. Configure openmldb for online feature extraction:
##in openmldb docker
docker exec -it demo bash
##deploy feature extraction
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/deploy.sql
##load online data
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < /root/project/load_online_data.sql

2. Configure oneflow for model serving
#check if config.pbtxt, model files, persistent path are correcly set

3. Start prediction server
cd openmldb_serving/
## start prediction server
./start_predict_server.sh 0.0.0.0:9080
## start oneflow serving
replace demodir with your demo folder path
docker run --runtime=nvidia --rm --network=host \
  -v $demodir/oneflow_process/model:/models \
  -v /home/gtest/work/oneflow_serving/serving/build/libtriton_oneflow.so:/backends/oneflow/libtriton_oneflow.so \
  -v /home/gtest/work/oneflow_serving/oneflow/build/liboneflow_cpp/lib/:/mylib \
  -v $demodir/oneflow_process/persistent:/root/demo/persistent \
  registry.cn-beijing.aliyuncs.com/oneflow/triton-devel \
  bash -c 'LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/mylib /opt/tritonserver/bin/tritonserver \
  --model-repository=/models --backend-directory=/backends'

Test:
## send data for prediciton
python predict.py
