# openmldb mode: standalone / cluster
export OPENMLDB_MODE=standalone
# tablet port
export OPENMLDB_TABLET_PORT=10921
# nameserver port
export OPENMLDB_NAMESERVER_PORT=7527
# taskmanager port
export OPENMLDB_TASKMANAGER_PORT=9902
# apiserver port
export OPENMLDB_APISERVER_PORT=9080
# zookeeper cluster address
# if OPENMLDB_USE_EXISTING_ZK_CLUSTER is set, will use existing zk cluster
export OPENMLDB_USE_EXISTING_ZK_CLUSTER=false
# the zk cluster address
export OPENMLDB_ZK_CLUSTER=node-4:2181
# zookeeper root path
export OPENMLDB_ZK_ROOT_PATH=/openmldb
# openmldb root path
export OPENMLDB_HOME=
# openmldb work directory where the data and log are located
export OPENMLDB_WORK_DIR=
# the root path of openmldb spark release, default is $OPENMLDB_HOME/spark
# if not exists, download from online
export SPARK_HOME=
# the root path of zookeeper release, default is $OPENMLDB_HOME/zookeeper
# if not exists, download from online
export ZK_HOME=