#! /bin/sh

set -x -e -u -E # this script will exit if any sub-command fails

WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty

if [ ! -d ${DEPS_PREFIX} ];then
    wget http://pkg.4paradigm.com/rtidb/dev/thirdparty.tar.gz 
    tar -zxvf thirdparty.tar.gz >/dev/null
fi

if [ ! -d ${DEPS_SOURCE} ];then
    mkdir -p ${DEPS_SOURCE}
    cd ${DEPS_SOURCE}
    wget http://pkg.4paradigm.com/rtidb/dev/zookeeper-3.4.10.tar.gz
    tar -zxvf zookeeper-3.4.10.tar.gz >/dev/null
fi
