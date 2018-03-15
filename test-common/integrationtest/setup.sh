#!/bin/bash
set +x
rtidbver=$1
testpath=$(cd "$(dirname "$0")"; pwd)
testenvpath=${testpath}/env.conf
testconfpath=${testpath}/conf/test.conf
testlogpath=${testpath}/logs
projectpath=`echo ${testpath}|awk -F '/test-common' '{print $1}'`
rtidbpath=${projectpath}/build/bin/rtidb
tbconfpath=${projectpath}/release/conf/rtidb.flags
nsconfpath=${projectpath}/release/conf/nameserver.flags
reportpath=${projectpath}/test-output/integrationtest/test-reports
zkpath=${testpath}/../../thirdsrc/zookeeper-3.4.10

echo export rtidbver=${rtidbver} > ${testenvpath}
echo export testpath=${testpath} >> ${testenvpath}
echo export projectpath=${projectpath} >> ${testenvpath}
echo export testconfpath=${testconfpath} >> ${testenvpath}
echo export testlogpath=${testlogpath} >> ${testenvpath}
echo export rtidbpath=${rtidbpath} >> ${testenvpath}
echo export tbconfpath=${tbconfpath} >> ${testenvpath}
echo export nsconfpath=${nsconfpath} >> ${testenvpath}
echo export reportpath=${reportpath} >> ${testenvpath}
echo export zkpath=${zkpath} >> ${testenvpath}

mkdir ${testlogpath}
cp -f ${rtidbpath} ${testpath}

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

