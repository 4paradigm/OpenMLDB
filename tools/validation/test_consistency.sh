OPENMLDB_BIN_PATH=/work/openmldb/bin/openmldb
$OPENMLDB_BIN_PATH --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client  < /tmp/prepare_data.sql
python3 test.py $OPENMLDB_BIN_PATH