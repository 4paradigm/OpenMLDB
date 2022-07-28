For full instructions, please refer to https://github.com/4paradigm/OpenMLDB/blob/main/docs/zh/use_case/JD_recommendation.md 


1. Engage openmldb for feature extraction:
##in openmldb docker
docker exec -it demo bash
##launch openmldb CLI
./init.sh
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client

##execute command as listed in openmldb_execution_steps.txt
##output data in "/root/project/out/1" ==> "$demodir/out/1"
##monitor job status until "FINISHED"
show jobs;
 

2. process openmldb output data:
##outside openmldb docker
conda activate oneflow

cd openmldb_process
##pass in directory of openmldb results
sh process_JD_out_full.sh $demodir/demo/out/1
##output data in $demodir/openmldb_process/out
##note output information, table_size_array

3. Launch oneflow deepfm model training:
cd oneflow_process/
##modify directory, sample size, table_size_array information in train_deepfm.sh accordingly
sh train_deepfm.sh

For serving, please refer to serving/README.md

