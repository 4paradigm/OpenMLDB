#! /bin/sh

PLATFORM=$1
set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
# mac can't support source compile for the followings:
# 1. zookeeper_client_c
# 2. rocksdb
########################################
STAGE="DEBUG"
WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"
mkdir -p $DEPS_PREFIX/lib $DEPS_PREFIX/include
export CXXFLAGS="-O3 -fPIC"
export CFLAGS="-O3 -fPIC"
export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}
yum install -y bc
cd ${DEPS_SOURCE}

if [ -f "arrow_succ" ]
then
    echo "arrow installed"
else
    if [ -f "apache-arrow-0.15.1.tar.gz" ]
    then
        echo "apache-arrow-0.15.1.tar.gz   downloaded"
    else
        wget --no-check-certificate -O apache-arrow-0.15.1.tar.gz http://pkg.4paradigm.com/fesql/apache-arrow-0.15.1.tar.gz
    fi
    tar -zxf apache-arrow-0.15.1.tar.gz
    export ARROW_BUILD_TOOLCHAIN=${DEPS_PREFIX}
    export JEMALLOC_HOME=${DEPS_PREFIX}
    cd apache-arrow-0.15.1/cpp && mkdir -p build && cd build
    cmake  -DARROW_JEMALLOC=OFF -DARROW_MIMALLOC=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DARROW_CUDA=OFF -DARROW_FLIGHT=OFF -DARROW_GANDIVA=OFF \
    -DARROW_GANDIVA_JAVA=OFF -DARROW_BUILD_SHARED=OFF -DARROW_HDFS=ON -DARROW_HIVESERVER2=OFF \
    -DARROW_ORC=OFF -DARROW_PARQUET=ON -DARROW_PLASMA=OFF\
    -DARROW_PLASMA_JAVA_CLIENT=OFF -DARROW_PYTHON=OFF -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_UTILITIES=OFF ..
    make -j4 parquet && make install 
    cd ${DEPS_SOURCE}
    touch arrow_succ
fi

cd ${WORK_DIR} && rm -rf ${DEPS_SOURCE}
