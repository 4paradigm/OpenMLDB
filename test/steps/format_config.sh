#! /usr/bin/env bash

#set DeployDir
rootPath=$1
jobName=$2
portFrom=$3
portTo=$4
Type=$5
Dependency=$6
version=$(echo $($rootPath/bin/openmldb --version) | awk '{print $3}')
curTime=$(date "+%m%d%H%M")
dirName=${jobName}-${version}-${curTime}

#set Deploy Host and Ports
Hosts=(node-1 node-3 node-4)

AvaNode1Ports=$(ssh ${Hosts[0]} "comm -23 <(seq $portFrom $portTo | sort) <(sudo ss -Htan | awk '{print $4}' | cut -d':' -f2 | sort -u) | shuf | head -n 8")
AvaNode2Ports=$(ssh ${Hosts[1]} "comm -23 <(seq $portFrom $portTo | sort) <(sudo ss -Htan | awk '{print $4}' | cut -d':' -f2 | sort -u) | shuf | head -n 1")
AvaNode3Ports=$(ssh ${Hosts[2]} "comm -23 <(seq $portFrom $portTo | sort) <(sudo ss -Htan | awk '{print $4}' | cut -d':' -f2 | sort -u) | shuf | head -n 1")

tablet1Port=$(echo $AvaNode1Ports | awk  '{print $1}')
tablet2Port=$(echo $AvaNode2Ports | awk  '{print $1}')
tablet3Port=$(echo $AvaNode3Ports | awk  '{print $1}')
ns1Port=$(echo $AvaNode1Ports | awk  '{print $2}')
#ns2Port=$(echo $AvaNode1Ports | awk  '{print $3}')
apiserverPort=$(echo $AvaNode1Ports | awk  '{print $4}')
taskmanagerPort=$(echo $AvaNode1Ports | awk  '{print $5}')
zookeeperPort1=$(echo $AvaNode1Ports | awk  '{print $6}')
zookeeperPort2=$(echo $AvaNode1Ports | awk  '{print $7}')
zookeeperPort3=$(echo $AvaNode1Ports | awk  '{print $8}')

# write addr to hosts
cat >$rootPath/conf/hosts<<EOF
[tablet]
${Hosts[0]}:$tablet1Port /tmp/$dirName/tablet
${Hosts[1]}:$tablet2Port /tmp/$dirName/tablet
${Hosts[2]}:$tablet3Port /tmp/$dirName/tablet
[nameserver]
${Hosts[0]}:$ns1Port /tmp/$dirName/ns
[apiserver]
${Hosts[0]}:$apiserverPort /tmp/$dirName/apiserver
[taskmanager]
${Hosts[0]}:$taskmanagerPort /tmp/$dirName/taskmanager
[zookeeper]
${Hosts[0]}:$zookeeperPort1:$zookeeperPort2:$zookeeperPort3 /tmp/$dirName/zk
EOF

#write openmldb.env.sh
cat >$rootPath/conf/openmldb-env.sh<<EOF
export OPENMLDB_VERSION=0.7.3
export OPENMLDB_MODE=\${OPENMLDB_MODE:=cluster}
export OPENMLDB_USE_EXISTING_ZK_CLUSTER=false
export OPENMLDB_ZK_HOME=
export OPENMLDB_ZK_CLUSTER=
export OPENMLDB_ZK_ROOT_PATH=/openmldb-$dirName
export OPENMLDB_HOME=
export SPARK_HOME=
export CLEAR_OPENMLDB_INSTALL_DIR=true
EOF

if [ "$Type"="java" ]; then
mkdir -p $rootPath/out
touch $rootPath/out/openmldb_info.yaml
cat >$rootPath/out/openmldb_info.yaml<<EOF
deployType: CLUSTER
zk_cluster: "${Hosts[0]}:$zookeeperPort1"
zk_root_path: "/openmldb-$dirName"
basePath: "$rootPath/tmp"
openMLDBPath: "/tmp/$dirName/tablet/bin/openmldb"
EOF
fi

if [ "$Dependency"="hadoop" ]; then
cat >$rootPath/conf/taskmanager.properties<<EOF
server.host=${Hosts[0]}
zookeeper.cluster=${Hosts[0]}:$zookeeperPort1
zookeeper.root_path=/openmldb-$dirName
server.port=$taskmanagerPort
job.log.path=./logs/
spark.home=
spark.master=yarn-client
offline.data.prefix=hdfs:///openmldb_integration_test/
spark.default.conf=spark.hadoop.yarn.timeline-service.enabled=false
hadoop.conf.dir=/4pd/home/liuqiyuan/hadoop
hadoop.user.name=root
external.function.dir=/tmp/
EOF
fi