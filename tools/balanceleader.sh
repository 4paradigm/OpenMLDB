source ./env.sh
python rtidb_migrate.py --rtidb_bin_path=$RTIDB_BIN_PATH --zk_cluster=$ZK_CLUSTER --zk_root_path=$ZK_ROOT_PATH --cmd=balanceleader --showtable_path=$SHOWTABLE_PATH
