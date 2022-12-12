#! /usr/bin/env bash
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

# if OPENMLDB_USE_EXISTING_ZK_CLUSTER is set, will use existing zk cluster
export OPENMLDB_USE_EXISTING_ZK_CLUSTER=false
# the root path of zookeeper release, default is $OPENMLDB_HOME/zookeeper
# if not exists, download from online
export OPENMLDB_ZK_HOME=
# the zookeeper cluster address, if not set, will get from conf/hosts
export OPENMLDB_ZK_CLUSTER=
# zookeeper root path
export OPENMLDB_ZK_ROOT_PATH=/openmldb
# zookeeper client port, clientPort=2181 in zoo.cfg
export OPENMLDB_ZK_CLUSTER_CLIENT_PORT=2181
# zookeeper peer port, which is the first port in this config server.1=zoo1:2888:3888 in zoo.cfg
export OPENMLDB_ZK_CLUSTER_PEER_PORT=2888
# zookeeper election port, which is the second port in this config server.1=zoo1:2888:3888 in zoo.cfg
export OPENMLDB_ZK_CLUSTER_ELECTION_PORT=3888

# openmldb root path
export OPENMLDB_HOME=
# the root path of openmldb spark release, default is $OPENMLDB_HOME/spark
# if not exists, download from online
export SPARK_HOME=