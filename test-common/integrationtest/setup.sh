#!/bin/bash
set +x
rtidbver=$1
testpath=$(cd "$(dirname "$0")"; pwd)
testenvpath=${testpath}/env.conf
testconfpath=${testpath}/conf/test.conf
testlogpath=${testpath}/logs
projectpath=`echo ${testpath}|awk -F '/test-common' '{print $1}'`
rtidbpath=${projectpath}/build/bin/rtidb
confpath=${projectpath}/release/conf/rtidb.flags
leaderpath=${testpath}/leader
slave1path=${testpath}/slave1
slave2path=${testpath}/slave2
reportpath=${projectpath}/test-output/integrationtest/test-reports
<<<<<<< HEAD
leader="127.0.0.1:37770"
slave1="127.0.0.1:37771"
slave2="127.0.0.1:37772"
leader_scan="127.0.0.1:47770"
slave1_scan="127.0.0.1:47771"
slave2_scan="127.0.0.1:47772"
zk="127.0.0.1:22181"
zkpath=${testpath}/../../thirdsrc/zookeeper-3.4.10
=======
leader="0.0.0.0:37770"
slave1="0.0.0.0:37771"
slave2="0.0.0.0:37772"
leader_scan="0.0.0.0:47770"
slave1_scan="0.0.0.0:47771"
slave2_scan="0.0.0.0:47772"
>>>>>>> 0847407a87ce9732510bbf5c8dda820127cc7f7d

echo export rtidbver=${rtidbver} > ${testenvpath}
echo export testpath=${testpath} >> ${testenvpath}
echo export projectpath=${projectpath} >> ${testenvpath}
echo export testconfpath=${testconfpath} >> ${testenvpath}
echo export testlogpath=${testlogpath} >> ${testenvpath}
echo export rtidbpath=${rtidbpath} >> ${testenvpath}
echo export confpath=${confpath} >> ${testenvpath}
echo export leaderpath=${leaderpath} >> ${testenvpath}
echo export slave1path=${slave1path} >> ${testenvpath}
echo export slave2path=${slave2path} >> ${testenvpath}
echo export leader=${leader} >> ${testenvpath}
echo export slave1=${slave1} >> ${testenvpath}
echo export slave2=${slave2} >> ${testenvpath}
echo export leader_scan=${leader_scan} >> ${testenvpath}
echo export slave1_scan=${slave1_scan} >> ${testenvpath}
echo export slave2_scan=${slave2_scan} >> ${testenvpath}
echo export reportpath=${reportpath} >> ${testenvpath}
echo export zk=${zk} >> ${testenvpath}
echo export zkpath=${zkpath} >> ${testenvpath}

function setup() {
    cp -f ${rtidbpath} ${testpath}

    mkdir -p ${leaderpath}/conf
    cat ${confpath} | egrep -v "endpoint=|--gc_interval=" > ${leaderpath}/conf/rtidb.flags
    sed -i '1a --endpoint='${leader} ${leaderpath}/conf/rtidb.flags
    sed -i '1a --scan_endpoint='${leader_scan} ${leaderpath}/conf/rtidb.flags
    sed -i '1a --gc_interval=1' ${leaderpath}/conf/rtidb.flags
    sed -i '1a --zk_cluster='${zk} ${leaderpath}/conf/rtidb.flags
    echo '--zk_root_path=/onebox' >> ${leaderpath}/conf/rtidb.flags

    mkdir -p ${slave1path}/conf
    cat ${confpath} | egrep -v "endpoint=|--gc_interval=" > ${slave1path}/conf/rtidb.flags
    sed -i '1a --endpoint='${slave1} ${slave1path}/conf/rtidb.flags
    sed -i '1a --scan_endpoint='${slave1_scan} ${slave1path}/conf/rtidb.flags
    sed -i '1a --gc_interval=1' ${slave1path}/conf/rtidb.flags
    sed -i '1a --zk_cluster='${zk} ${slave1path}/conf/rtidb.flags
    echo '--zk_root_path=/onebox' >> ${slave1path}/conf/rtidb.flags

    mkdir -p ${slave2path}/conf
    cat ${confpath} | egrep -v "endpoint=|--gc_interval=" > ${slave2path}/conf/rtidb.flags
    sed -i '1a --endpoint='${slave2} ${slave2path}/conf/rtidb.flags
    sed -i '1a --scan_endpoint='${slave2_scan} ${slave2path}/conf/rtidb.flags
    sed -i '1a --gc_interval=1' ${slave2path}/conf/rtidb.flags
    sed -i '1a --zk_cluster='${zk} ${slave2path}/conf/rtidb.flags
    echo '--zk_root_path=/onebox' >> ${slave2path}/conf/rtidb.flags
}

function start_client() {
#    cd $1 && ../mon ./conf/boot.sh -d -l ./rtidb_mon.log
    cd $1 && ../rtidb --flagfile=$1/conf/rtidb.flags > $1/log.log 2>&1 &
}

function stop_client() {
    port=`echo $1|awk -F ':' '{print $2}'`
    echo ${port}
    lsof -i:${port}|grep LISTEN|awk '{print $2}'
    lsof -i:`echo $1|awk -F ':' '{print $2}'`|grep LISTEN|awk '{print $2}'|xargs -i kill {}
}

function stop_all_clients() {
    ps xf | grep rtidb_mon | grep -v grep | awk '{print $1}' |xargs -i kill {}
    for i in ${leader} ${slave1} ${slave2} ${leader_scan} ${slave1_scan} ${slave2_scan} ${ns1} ${ns2}; do
        lsof -i:`echo ${i}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs -i kill {}
    done
    rm -rf ${leaderpath}/db/* ${slave1path}/db/* ${slave2path}/db/*
}

setup

stop_all_clients
sleep 5

start_client ${leaderpath}
start_client ${slave1path}
start_client ${slave2path}

sleep 2