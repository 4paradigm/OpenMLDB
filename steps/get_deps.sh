#! /bin/sh

set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
########################################
STAGE="DEBUG"
WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"
mkdir -p $DEPS_PREFIX/lib $DEPS_PREFIX/include

export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}

cd ${DEPS_SOURCE}

# boost
if [ -f "boost_succ" ]
then
    echo "boost exist"
else
    echo "start install boost...."
    wget https://github.com/elasticlog/deps/files/621702/boost-header-only.tar.gz >/dev/null
    tar zxf boost-header-only.tar.gz >/dev/null
    mv boost ${DEPS_PREFIX}/include
    touch boost_succ
    echo "install boost done"
fi

if [ -f "gtest_succ" ]
then 
   echo "gtest exist"
else
   echo "install gtest ...."
   wget -O gtest-1.7.0.zip http://github.com/google/googletest/archive/release-1.7.0.zip >/dev/null
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

if [ -f "protobuf_succ" ]
then
    echo "protobuf exist"
else
    echo "start install protobuf ..."
    # protobuf
    # wget --no-check-certificate https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz
    git clone --depth=1 https://github.com/00k/protobuf >/dev/null
    mv protobuf/protobuf-2.6.1.tar.gz .
    tar zxf protobuf-2.6.1.tar.gz >/dev/null
    cd protobuf-2.6.1
    ./configure ${DEPS_CONFIG} >/dev/null
    make -j2 >/dev/null
    make install
    cd -
    touch protobuf_succ
    echo "install protobuf done"
fi

if [ -f "zlib_succ" ]
then
    echo "zlib exist"
else
    echo "start install zlib..."
    wget --no-check-certificate https://github.com/elasticlog/deps/files/877654/zlib-1.2.11.tar.gz
    tar zxf zlib-1.2.11.tar.gz 
    cd zlib-1.2.11
    sed -i '/CFLAGS="${CFLAGS--O3}"/c\  CFLAGS="${CFLAGS--O3} -fPIC"' configure
    ./configure --static --prefix=${DEPS_PREFIX} >/dev/null
    make -j2 >/dev/null
    make install
    cd -
    touch zlib_succ
    echo "install zlib done"
fi

if [ -f "snappy_succ" ]
then
    echo "snappy exist"
else
    echo "start install snappy ..."
    # snappy
    # wget --no-check-certificate https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz
    git clone --depth=1 https://github.com/00k/snappy
    mv snappy/snappy-1.1.1.tar.gz .
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
    wget --no-check-certificate -O gflags-2.2.0.tar.gz https://github.com/elasticlog/deps/files/789206/gflags-2.2.0.tar.gz
    tar zxf gflags-2.2.0.tar.gz
    cd gflags-2.2.0
    cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC >/dev/null
    make -j2 >/dev/null
    make install
    cd -
    touch gflags_succ
fi

if [ -f "common_succ" ]
then 
   echo "common exist"
else
  # common
  git clone https://github.com/baidu/common.git
  cd common
  sed -i 's/^INCLUDE_PATH=.*/INCLUDE_PATH=-Iinclude -I..\/..\/thirdparty\/include/' Makefile
  sed -i 's/LOG(/PDLOG(/g' include/logging.h
  make -j2 >/dev/null
  cp -rf include/* ${DEPS_PREFIX}/include
  cp -rf libcommon.a ${DEPS_PREFIX}/lib
  cd -
  touch common_succ
fi


if [ -f "unwind_succ" ] 
then
    echo "unwind_exist"
else
    wget --no-check-certificate -O libunwind-1.1.tar.gz https://github.com/libunwind/libunwind/archive/v1.1.tar.gz 
    tar -zxvf libunwind-1.1.tar.gz
    cd libunwind-1.1
    autoreconf -i
    ./configure --prefix=${DEPS_PREFIX}
    make -j4 && make install 
    cd -
    touch unwind_succ
fi

if [ -f "gperf_tool" ]
then
    echo "gperf_tool exist"
else
    wget --no-check-certificate -O gperftools-2.5.tar.gz https://github.com/gperftools/gperftools/releases/download/gperftools-2.5/gperftools-2.5.tar.gz 
    tar -zxvf gperftools-2.5.tar.gz 
    cd gperftools-2.5 
    ./configure --enable-cpu-profiler --enable-heap-checker --enable-heap-profiler --prefix=${DEPS_PREFIX} 
    make -j2 >/dev/null
    make install
    cd -
    touch gperf_tool
fi

if [ -f "rapjson_succ" ]
then 
    echo "rapjson exist"
else
    wget --no-check-certificate -O rapidjson.1.1.0.tar.gz https://github.com/miloyip/rapidjson/archive/v1.1.0.tar.gz
    tar -zxvf rapidjson.1.1.0.tar.gz
    cp -rf rapidjson-1.1.0/include/rapidjson ${DEPS_PREFIX}/include
    touch rapjson_succ
fi

if [ -f "leveldb_succ" ]
then
    echo "leveldb exist"
else
    git clone https://github.com/google/leveldb.git
    cd leveldb
    sed -i 's/^OPT ?= -O2 -DNDEBUG/OPT ?= -O2 -DNDEBUG -fPIC/' Makefile
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
    wget -O OpenSSL_1_1_0.zip https://github.com/openssl/openssl/archive/OpenSSL_1_1_0.zip > /dev/null
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


if [ -f "brpc_succ" ]
then
    echo "brpc exist"
else
    wget http://pkg.4paradigm.com/rtidb/dev/brpc-legacy-1.3.7.tar.gz
    tar -zxvf brpc-legacy-1.3.7.tar.gz
    BRPC_DIR=$DEPS_SOURCE/brpc-legacy
    cd brpc-legacy
    sh config_brpc.sh --headers=${DEPS_PREFIX}/include --libs=${DEPS_PREFIX}/lib
    make -j5 libbrpc.a
    make output/include
    cp -rf output/include/* ${DEPS_PREFIX}/include
    cp libbrpc.a ${DEPS_PREFIX}/lib
    cd -
    touch brpc_succ
    echo "brpc done"
fi

if [ -f "zk_succ" ]
then
    echo "zk exist"
else
    wget --no-check-certificate -O zookeeper-3.4.10.tar.gz http://www-eu.apache.org/dist/zookeeper/zookeeper-3.4.10/zookeeper-3.4.10.tar.gz
    tar -zxvf zookeeper-3.4.10.tar.gz
    cd zookeeper-3.4.10/src/c/
    ./configure --prefix=${DEPS_PREFIX} --enable-shared=no --enable-static=yes
    make -j4 >/dev/null 
    make install
    cd -
    touch zk_succ
fi

if [ -f "rocksdb_succ" ]
then
    echo "rocksdb exist"
else
    echo "start install rocksdb ..."
    # rocksdb
    wget -O rocksdb-5.18.3.tar.gz  https://github.com/facebook/rocksdb/archive/v5.18.3.tar.gz >/dev/null
    tar zxf rocksdb-5.18.3.tar.gz  >/dev/null
    cd rocksdb-5.18.3
    export CPPFLAGS=-I${DEPS_PREFIX}/include
    export LDFLAGS=-L${DEPS_PREFIX}/lib
    make static_lib -j2 >/dev/null
    cp -rf ./include/* ${DEPS_PREFIX}/include
    cp librocksdb.a ${DEPS_PREFIX}/lib
    cd -
    touch rocksdb_succ
    echo "install rocksdb done"
fi

