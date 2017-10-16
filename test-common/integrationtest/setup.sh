#!/bin/bash
set +x
rtidbver=$1
testpath=$(cd "$(dirname "$0")"; pwd)
testenvpath=${testpath}/env.conf
projectpath=`echo ${testpath}|awk -F '/test-common' '{print $1}'`
rtidbpath=${projectpath}/build/bin/rtidb
confpath=${projectpath}/release/conf/rtidb.flags
leaderpath=${testpath}/leader
slave1path=${testpath}/slave1
slave2path=${testpath}/slave2
leader="0.0.0.0:17770"
slave1="0.0.0.0:17771"
slave2="0.0.0.0:17772"
leader_scan="0.0.0.0:27770"
slave1_scan="0.0.0.0:27771"
slave2_scan="0.0.0.0:27772"

echo export rtidbver=${rtidbver} > ${testenvpath}
echo export testpath=${testpath} >> ${testenvpath}
echo export projectpath=${projectpath} >> ${testenvpath}
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

function setup() {
    cp -f ${rtidbpath} ${testpath}

    mkdir -p ${leaderpath}/conf
    cat ${confpath} | egrep -v "endpoint=|--gc_interval=" > ${leaderpath}/conf/rtidb.flags
    sed -i '1a --endpoint='${leader} ${leaderpath}/conf/rtidb.flags
    sed -i '1a --scan_endpoint='${leader_scan} ${leaderpath}/conf/rtidb.flags
    sed -i '1a --gc_interval=1' ${leaderpath}/conf/rtidb.flags
    cp -f ${testpath}/boot.sh ${leaderpath}/conf
    cp -f ${testpath}/start.sh ${leaderpath}/conf

    mkdir -p ${slave1path}/conf
    cat ${confpath} | egrep -v "endpoint=|--gc_interval=" > ${slave1path}/conf/rtidb.flags
    sed -i '1a --endpoint='${slave1} ${slave1path}/conf/rtidb.flags
    sed -i '1a --scan_endpoint='${slave1_scan} ${slave1path}/conf/rtidb.flags
    sed -i '1a --gc_interval=1' ${slave1path}/conf/rtidb.flags
    cp -f ${testpath}/boot.sh ${slave1path}/conf
    cp -f ${testpath}/start.sh ${slave1path}/conf

    mkdir -p ${slave2path}/conf
    cat ${confpath} | egrep -v "endpoint=|--gc_interval=" > ${slave2path}/conf/rtidb.flags
    sed -i '1a --endpoint='${slave2} ${slave2path}/conf/rtidb.flags
    sed -i '1a --scan_endpoint='${slave2_scan} ${slave2path}/conf/rtidb.flags
    sed -i '1a --gc_interval=1' ${slave2path}/conf/rtidb.flags
    cp -f ${testpath}/boot.sh ${slave2path}/conf
    cp -f ${testpath}/start.sh ${slave2path}/conf
}

function start_client() {
    cd $1 && ../mon ./conf/boot.sh -d -l ./rtidb_mon.log
}

function stop_client() {
    port=`echo $1|awk -F ':' '{print $2}'`
    echo $port
    lsof -i:${port}|grep LISTEN|awk '{print $2}'
	lsof -i:`echo $1|awk -F ':' '{print $2}'`|grep LISTEN|awk '{print $2}'|xargs kill -9
}

function stop_all_clients() {
	ps xf | grep rtidb_mon | grep -v grep | awk '{print $1}' |xargs kill -9
	lsof -i:`echo ${leader}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs kill -9
	lsof -i:`echo ${slave1}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs kill -9
	lsof -i:`echo ${slave2}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs kill -9
	lsof -i:`echo ${leader_scan}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs kill -9
	lsof -i:`echo ${slave1_scan}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs kill -9
	lsof -i:`echo ${slave2_scan}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs kill -9
	rm -rf ${leaderpath}/db/* ${slave1path}/db/* ${slave2path}/db/*
}

setup

stop_all_clients
sleep 2

start_client ${leaderpath}
start_client ${slave1path}
start_client ${slave2path}

cd ${testpath}
