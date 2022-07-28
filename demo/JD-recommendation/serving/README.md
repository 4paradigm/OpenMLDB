##in openmldb docker
docker exec -it demo bash
##launch openmldb CLI
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client

##prepare online data and deployment script
##execute command as listed in openmldb_serving.txt

## exit openmldb CLI 
quit

cd openmldb_serving/
## start prediction server
./start_predict_server.sh 0.0.0.0:9080

## send data for prediciton
python3 predict.py
