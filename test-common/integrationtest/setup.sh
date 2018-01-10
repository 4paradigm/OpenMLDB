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
leader="127.0.0.1:37770"
slave1="127.0.0.1:37771"
slave2="127.0.0.1:37772"
ns1="127.0.0.1:37773"
ns2="127.0.0.1:37774"
zk="127.0.0.1:22181"
zkpath=${testpath}/../../thirdsrc/zookeeper-3.4.10

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
echo export reportpath=${reportpath} >> ${testenvpath}
echo export zk=${zk} >> ${testenvpath}
echo export zkpath=${zkpath} >> ${testenvpath}

function setup() {
    mkdir -p ${testlogpath}
    cp -f ${rtidbpath} ${testpath}

    mkdir -p ${leaderpath}/conf
    cat ${confpath} | egrep -v "endpoint=|--gc_interval=" > ${leaderpath}/conf/rtidb.flags
    sed -i '1a --endpoint='${leader} ${leaderpath}/conf/rtidb.flags
    sed -i '1a --gc_interval=1' ${leaderpath}/conf/rtidb.flags
    sed -i '1a --zk_cluster='${zk} ${leaderpath}/conf/rtidb.flags
    echo '--zk_root_path=/onebox' >> ${leaderpath}/conf/rtidb.flags

    mkdir -p ${slave1path}/conf
    cat ${confpath} | egrep -v "endpoint=|--gc_interval=" > ${slave1path}/conf/rtidb.flags
    sed -i '1a --endpoint='${slave1} ${slave1path}/conf/rtidb.flags
    sed -i '1a --gc_interval=1' ${slave1path}/conf/rtidb.flags
    sed -i '1a --zk_cluster='${zk} ${slave1path}/conf/rtidb.flags
    echo '--zk_root_path=/onebox' >> ${slave1path}/conf/rtidb.flags

    mkdir -p ${slave2path}/conf
    cat ${confpath} | egrep -v "endpoint=|--gc_interval=" > ${slave2path}/conf/rtidb.flags
    sed -i '1a --endpoint='${slave2} ${slave2path}/conf/rtidb.flags
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
    for i in ${leader} ${slave1} ${slave2} ${ns1} ${ns2}; do
        lsof -i:`echo ${i}|awk -F ':' '{print $2}'`|grep -v PID|awk '{print $2}'|xargs -i kill {}
    done
    rm -rf ${leaderpath}/db/* ${slave1path}/db/* ${slave2path}/db/*
}

rm -rf ${leaderpath}/db/* ${slave1path}/db/* ${slave2path}/db/*
setup

# setup xmlrunner
if [ -d "${testpath}/xmlrunner" ]
then
    echo "xmlrunner exist"
else
    echo "start install xmlrunner...."
    wget -O ${projectpath}/thirdsrc/xmlrunner.tar.gz http://pkg.4paradigm.com:81/rtidb/dev/xmlrunner.tar.gz >/dev/null
    tar -zxvf ${projectpath}/thirdsrc/xmlrunner.tar.gz -C ${testpath} >/dev/null
    echo "install xmlrunner done"
fi

# setup ddt
if [ -f "${testpath}/libs/ddt.pyc" ]
then
    echo "ddt exist"
else
    echo "start install ddt...."
    wget -O ${projectpath}/thirdsrc/ddt.tar.gz http://pkg.4paradigm.com:81/rtidb/dev/ddt.tar.gz >/dev/null
    tar -zxvf ${projectpath}/thirdsrc/ddt.tar.gz -C ${testpath}/libs >/dev/null
    echo "install ddt done"
fi

# setup zk
if [ -d "${projectpath}/thirdsrc/zookeeper-3.4.10" ]
then
    echo "zookeeper exist"
else
    echo "start install zookeeper...."
    wget -O ${projectpath}/thirdsrc/zookeeper-3.4.10.tar.gz http://pkg.4paradigm.com:81/rtidb/dev/zookeeper-3.4.10.tar.gz >/dev/null
    tar -zxvf ${projectpath}/thirdsrc/zookeeper-3.4.10.tar.gz -C ${projectpath}/thirdsrc >/dev/null
    echo "install zookeeper done"
fi

