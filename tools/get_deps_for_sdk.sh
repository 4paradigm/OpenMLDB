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
DEPS_SOURCE=`pwd`/sdk_thirdsrc
DEPS_PREFIX=`pwd`/sdk_thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"
mkdir -p $DEPS_PREFIX/lib $DEPS_PREFIX/include
export CXXFLAGS="-O3 -fPIC"
export CFLAGS="-O3 -fPIC"
export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}

cd ${DEPS_SOURCE}

if [ -f "gtest_succ" ]
then 
   echo "gtest exist"
else
   echo "install gtest ...."
   wget http://pkg.4paradigm.com/rtidb/dev/gtest-1.7.0.zip >/dev/null
   unzip gtest-1.7.0.zip 
   GTEST_DIR=$DEPS_SOURCE/googletest-release-1.7.0
   cd googletest-release-1.7.0
   cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC >/dev/null
   make -j2 
   cp -rf include/gtest ${DEPS_PREFIX}/include 
   cp libgtest.a libgtest_main.a ${DEPS_PREFIX}/lib
   cd $DEPS_SOURCE 
   touch gtest_succ
   echo "install gtest done"
fi

if [ -f "zlib_succ" ]
then
    echo "zlib exist"
else
    echo "start install zlib..."
    wget http://pkg.4paradigm.com/rtidb/dev/zlib-1.2.11.tar.gz
    tar zxf zlib-1.2.11.tar.gz 
    cd zlib-1.2.11
    ./configure --static --prefix=${DEPS_PREFIX} >/dev/null
    make -j2 >/dev/null
    make install
    cd -
    touch zlib_succ
    echo "install zlib done"
fi

if [ -f "protobuf_succ" ]
then
    echo "protobuf exist"
else
    echo "start install protobuf ..."
    # protobuf
    # wget --no-check-certificate https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz
    wget http://pkg.4paradigm.com/rtidb/dev/protobuf-2.6.1.tar.gz >/dev/null
    tar zxf protobuf-2.6.1.tar.gz >/dev/null
    cd protobuf-2.6.1
    export CPPFLAGS=-I${DEPS_PREFIX}/include
    export LDFLAGS=-L${DEPS_PREFIX}/lib
    ./configure ${DEPS_CONFIG} >/dev/null
    make -j2 >/dev/null
    make install
    cd -
    touch protobuf_succ
    echo "install protobuf done"
fi

if [ -f "snappy_succ" ]
then
    echo "snappy exist"
else
    echo "start install snappy ..."
    # snappy
    # wget --no-check-certificate https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz
    wget http://pkg.4paradigm.com/rtidb/dev/snappy-1.1.1.tar.gz
    tar zxf snappy-1.1.1.tar.gz >/dev/null
    cd snappy-1.1.1
    ./configure ${DEPS_CONFIG} >/dev/null
    make -j2 >/dev/null
    make install
    cd -
    touch snappy_succ
    echo "install snappy done"
fi

if [ -f "gflags_succ" ]
then
    echo "gflags-2.1.1.tar.gz exist"
else
    # gflags
    wget http://pkg.4paradigm.com/rtidb/dev/gflags-2.2.0.tar.gz 
    tar zxf gflags-2.2.0.tar.gz
    cd gflags-2.2.0
    cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC >/dev/null
    make -j2 >/dev/null
    make install
    cd -
    touch gflags_succ
fi



if [ -f "leveldb_succ" ]
then
    echo "leveldb exist"
else
    wget http://pkg.4paradigm.com/rtidb/dev/leveldb.tar.gz
    tar -zxvf leveldb.tar.gz
    cd leveldb
    make -j8
    cp -rf include/* ${DEPS_PREFIX}/include
    cp out-static/libleveldb.a ${DEPS_PREFIX}/lib
    cd -
    touch leveldb_succ
fi

if [ -f "openssl_succ" ]
then
    echo "openssl exist"
else
    wget -O OpenSSL_1_1_0.zip http://pkg.4paradigm.com/rtidb/dev/OpenSSL_1_1_0.zip > /dev/null
    unzip OpenSSL_1_1_0.zip
    cd openssl-OpenSSL_1_1_0
    ./config --prefix=${DEPS_PREFIX} --openssldir=${DEPS_PREFIX}
    make -j5
    make install
    rm -rf ${DEPS_PREFIX}/lib/libssl.so*
    rm -rf ${DEPS_PREFIX}/lib/libcrypto.so*
    cd -
    touch openssl_succ
    echo "openssl done"
fi

if [ -f "glog_succ" ]
then 
    echo "glog exist"
else
    wget --no-check-certificate -O glogs-v0.4.tar.gz https://github.com/google/glog/archive/v0.4.0.tar.gz
    tar zxf glogs-v0.4.tar.gz
    cd glog-0.4.0 && mkdir -p b2
    cd b2 && cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC ..  && make && make install
    cd ${DEPS_SOURCE}
    touch glog_succ
fi

if [ -f "zk_succ" ]
then
    echo "zk exist"
else
    wget https://mirrors.tuna.tsinghua.edu.cn/apache/zookeeper/zookeeper-3.5.7/apache-zookeeper-3.5.7.tar.gz
    tar -zxvf apache-zookeeper-3.5.7.tar.gz
    cd apache-zookeeper-3.5.7/zookeeper-client/zookeeper-client-c && mkdir -p build
    cd build && cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC ..  && make && make install
    cd ${DEPS_SOURCE}
    touch zk_succ
fi

if [ -f "brpc_succ" ]
then
    echo "brpc exist"
else
    if [ -d "incubator-brpc" ]
    then
        rm -rf incubator-brpc
    fi
    if [ -f "incubator-brpc.tar.gz" ]
    then
        echo "incubator-brpc.tar.gz exists"
    else
        wget http://pkg.4paradigm.com/fesql/incubator-brpc.0304.tar.gz
    fi
    tar -zxvf incubator-brpc.0304.tar.gz
    BRPC_DIR=$DEPS_SOURCE/incubator-brpc
    cd incubator-brpc && mkdir -p b2
    cd b2 && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DWITH_GLOG=ON .. && make brpc-static 
    cp -rf output/include/* ${DEPS_PREFIX}/include/
    cp -rf output/lib/* ${DEPS_PREFIX}/lib/
    cd ${DEPS_SOURCE}
    touch brpc_succ
    echo "brpc done"
fi

if [[ "$OSTYPE" == "linux-gnu" ]]; then
	cd ${DEPS_PREFIX}/lib && rm *.so
elif [[ "$OSTYPE" == "darwin"* ]]; then
	cd ${DEPS_PREFIX}/lib && rm *.dylib 
fi
