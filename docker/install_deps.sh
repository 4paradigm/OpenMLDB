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
yum install -y autoconf
yum install -y bc
cd ${DEPS_SOURCE}


if [ -f "zlib_succ" ]
then
    echo "zlib exist"
else
    echo "start install zlib..."
    wget http://pkg.4paradigm.com/rtidb/dev/zlib-1.2.11.tar.gz
    tar zxf zlib-1.2.11.tar.gz  >/dev/null
    cd zlib-1.2.11
    sed -i '/CFLAGS="${CFLAGS--O3}"/c\  CFLAGS="${CFLAGS--O3} -fPIC"' configure
    ./configure --static --prefix=${DEPS_PREFIX} >/dev/null
    make -j4 >/dev/null
    make install
    cd -
    touch zlib_succ
    echo "install zlib done"
fi

if [ -f "openssl_succ" ]
then
    echo "openssl exist"
else
    wget -O openssl-1.0.2u.tar.gz https://www.openssl.org/source/old/1.0.2/openssl-1.0.2u.tar.gz >/dev/null
    tar -zxf openssl-1.0.2u.tar.gz
    cd openssl-1.0.2u
    ./config --prefix=${DEPS_PREFIX} --openssldir=${DEPS_PREFIX}
    make -j8
    make install
    cd -
    touch openssl_succ
    echo "openssl done"
fi

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
   make -j4 
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
    wget http://pkg.4paradigm.com/rtidb/dev/protobuf-2.6.1.tar.gz >/dev/null
    tar zxf protobuf-2.6.1.tar.gz >/dev/null
    cd protobuf-2.6.1
    export CPPFLAGS=-I${DEPS_PREFIX}/include
    export LDFLAGS=-L${DEPS_PREFIX}/lib
    ./configure ${DEPS_CONFIG} >/dev/null
    make -j4 >/dev/null
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
    make -j4 >/dev/null
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
    tar zxf gflags-2.2.0.tar.gz >/dev/null
    cd gflags-2.2.0
    cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC >/dev/null
    make -j4 >/dev/null
    make install
    cd -
    touch gflags_succ
fi


if [ -f "unwind_succ" ] 
then
    echo "unwind_exist"
else
    wget http://pkg.4paradigm.com/rtidb/dev/libunwind-1.1.tar.gz  
    tar zxf libunwind-1.1.tar.gz 
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
    wget http://pkg.4paradigm.com/rtidb/dev/gperftools-2.5.tar.gz
    tar -zxf gperftools-2.5.tar.gz
    cd gperftools-2.5
    ./configure --enable-cpu-profiler --enable-heap-checker --enable-heap-profiler  --enable-static --prefix=${DEPS_PREFIX}
    make -j4 >/dev/null
    make install
    cd -
    touch gperf_tool
fi

if [ -f "rapjson_succ" ]
then 
    echo "rapjson exist"
else
    wget http://pkg.4paradigm.com/rtidb/dev/rapidjson.1.1.0.tar.gz
    tar -zxf rapidjson.1.1.0.tar.gz
    cp -rf rapidjson-1.1.0/include/rapidjson ${DEPS_PREFIX}/include
    touch rapjson_succ
fi

if [ -f "leveldb_succ" ]
then
    echo "leveldb exist"
else
    wget http://pkg.4paradigm.com/rtidb/dev/leveldb.tar.gz
    tar -zxf leveldb.tar.gz
    cd leveldb
    sed -i 's/^OPT ?= -O2 -DNDEBUG/OPT ?= -O2 -DNDEBUG -fPIC/' Makefile
    make -j8
    cp -rf include/* ${DEPS_PREFIX}/include
    cp out-static/libleveldb.a ${DEPS_PREFIX}/lib
    cd -
    touch leveldb_succ
fi


if [ -f "zk_succ" ]
then
    echo "zk exist"
else
    wget https://mirrors.tuna.tsinghua.edu.cn/apache/zookeeper/zookeeper-3.5.5/apache-zookeeper-3.5.5.tar.gz
    tar -zxf apache-zookeeper-3.5.5.tar.gz
    cd apache-zookeeper-3.5.5/zookeeper-client/zookeeper-client-c 
    autoconf && ./configure --prefix=${DEPS_PREFIX} --enable-shared=no --enable-static=yes && make -j4 && make install
    cd ${DEPS_SOURCE}
    touch zk_succ
fi

if [ -f "abseil_succ" ]
then
    echo "abseil exist"
else
    wget --no-check-certificate -O 20190808.tar.gz https://github.com/abseil/abseil-cpp/archive/20190808.tar.gz
    tar zxf 20190808.tar.gz
    cd abseil-cpp-20190808 && mkdir build 
    cd build && cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC .. >/dev/null
    make -j4 && make install
    cd ${DEPS_SOURCE}
    touch abseil_succ
fi

if [ -f "bison_succ" ]
then
    echo "bison exist"
else
    wget --no-check-certificate -O bison-3.4.tar.gz http://ftp.gnu.org/gnu/bison/bison-3.4.tar.gz
    tar zxf bison-3.4.tar.gz
    cd bison-3.4
    ./configure --prefix=${DEPS_PREFIX} && make install
    cd -
    touch bison_succ
fi

if [ -f "flex_succ" ]
then
    echo "flex exist"
else
    wget --no-check-certificate -O flex-2.6.4.tar.gz http://pkg.4paradigm.com/fesql/flex-2.6.4.tar.gz
    tar zxf flex-2.6.4.tar.gz
    cd flex-2.6.4
    ./autogen.sh && ./configure --prefix=${DEPS_PREFIX} && make install
    cd -
    touch flex_succ
fi

if [ -f "benchmark_succ" ]
then
    echo "benchmark exist"
else
    wget --no-check-certificate -O v1.5.0.tar.gz http://pkg.4paradigm.com/fesql/benchmark-1.5.0.tar.gz
    tar zxf v1.5.0.tar.gz
    cd benchmark-1.5.0 && mkdir -p build
    cd build && cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC -DBENCHMARK_ENABLE_GTEST_TESTS=OFF .. >/dev/null
    make -j4 && make install
    cd ${DEPS_SOURCE}
    touch benchmark_succ
fi

if [ -f "xz_succ" ] 
then
    echo "zx exist"
else
    wget --no-check-certificate -O xz-5.2.4.tar.gz http://pkg.4paradigm.com/fesql/xz-5.2.4.tar.gz
    tar -zxf xz-5.2.4.tar.gz
    cd xz-5.2.4 && ./configure --prefix=${DEPS_PREFIX} && make -j4 && make install
    cd -
    touch xz_succ
fi

if [ -f "boost_succ" ] 
then
    echo "boost exist"
else
    if [ -f "boost_1_69_0.tar.gz" ]
    then
        echo "boost exist"
    else
        wget --no-check-certificate -O boost_1_69_0.tar.gz http://pkg.4paradigm.com/fesql/boost_1_69_0.tar.gz
    fi
    tar -zxf boost_1_69_0.tar.gz
    cd boost_1_69_0 && ./bootstrap.sh && ./b2 -j12 && ./b2 install --prefix=${DEPS_PREFIX}
    cd -
    touch boost_succ
fi

if [ -f "brotli_succ" ] 
then
    echo "brotli exist"
else
    if [ -f "v1.0.7.tar.gz" ]
    then
        echo "brotli exist"
    else
        wget --no-check-certificate -O v1.0.7.tar.gz http://pkg.4paradigm.com/fesql/brotli-1.0.7.tar.gz
    fi
    tar -zxf v1.0.7.tar.gz
    cd brotli-1.0.7  && ./bootstrap && ./configure --prefix=${DEPS_PREFIX} && make -j4 && make install
    cd -
    touch brotli_succ
fi

if [ -f "double-conversion_succ" ]
then 
    echo "double-conversion exist"
else
    if [ -f "v3.1.5.tar.gz" ]
    then
        echo "double-conversion pkg exist"
    else
        wget --no-check-certificate -O v3.1.5.tar.gz http://pkg.4paradigm.com/fesql/double-conversion-3.1.5.tar.gz
    fi
    tar -zxf v3.1.5.tar.gz
    cd double-conversion-3.1.5 && mkdir -p build
    cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC .. >/dev/null
    make -j4 && make install
    cd ${DEPS_SOURCE}
    touch double-conversion_succ
fi

if [ -f "lz4_succ" ]
then
    echo " lz4 exist"
else
    if [ -f "lz4-1.7.5.tar.gz" ]
    then
        echo "lz4 tar exist"
    else
        wget --no-check-certificate -O lz4-1.7.5.tar.gz http://pkg.4paradigm.com/fesql/lz4-1.7.5.tar.gz
    fi
    tar -zxf lz4-1.7.5.tar.gz 
    cd lz4-1.7.5 && make -j4 && make install PREFIX=${DEPS_PREFIX}
    cd ${DEPS_SOURCE}
    touch lz4_succ
fi

if [ -f "bzip2_succ" ]
then
    echo "bzip2 installed"
else
    if [ -f "bzip2-1.0.8.tar.gz" ]
    then
        echo "bzip2-1.0.8.tar.gz  downloaded"
    else
        wget --no-check-certificate -O bzip2-1.0.8.tar.gz http://pkg.4paradigm.com/fesql/bzip2-1.0.8.tar.gz
    fi
    tar -zxf bzip2-1.0.8.tar.gz 
    cd bzip2-1.0.8 && make -j4 && make install PREFIX=${DEPS_PREFIX}
    cd -
    touch bzip2_succ
fi

if [ -f "jemalloc_succ" ]
then
    echo "jemalloc installed"
else
    if [ -f "jemalloc-5.2.1.tar.gz" ]
    then
        echo "jemalloc-5.2.1.tar.gz downloaded"
    else
        wget --no-check-certificate -O jemalloc-5.2.1.tar.gz http://pkg.4paradigm.com/fesql/jemalloc-5.2.1.tar.gz
    fi
    tar -zxf jemalloc-5.2.1.tar.gz
    cd jemalloc-5.2.1 && ./autogen.sh && ./configure --prefix=${DEPS_PREFIX} && make -j4 && make install
    cd - 
    touch jemalloc_succ
fi

if [ -f "flatbuffer_succ" ]
then
    echo "flatbuffer installed"
else
    if [ -f "flatbuffers-1.11.0.tar.gz" ]
    then
        echo "flatbuffers-1.11.0.tar.gz downloaded"
    else
        wget --no-check-certificate -O flatbuffers-1.11.0.tar.gz http://pkg.4paradigm.com/fesql/flatbuffers-1.11.0.tar.gz
    fi
    tar -zxf flatbuffers-1.11.0.tar.gz
    cd flatbuffers-1.11.0 && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC ..
    make -j4 && make install
    cd ${DEPS_SOURCE}
    touch flatbuffer_succ
fi

if [ -f "zstd_succ" ]
then
    echo "zstd installed"
else
    if [ -f "zstd-1.4.4.tar.gz" ]
    then
        echo "zstd-1.4.4.tar.gz  downloaded"
    else
        wget --no-check-certificate -O zstd-1.4.4.tar.gz http://pkg.4paradigm.com/fesql/zstd-1.4.4.tar.gz
    fi
    tar -zxf zstd-1.4.4.tar.gz
    cd zstd-1.4.4 && make -j4 && make install PREFIX=${DEPS_PREFIX}
    cd ${DEPS_SOURCE}
    touch zstd_succ
fi

if [ -f "thrift_succ" ]
then
    echo "thrift installed"
else
    if [ -f "thrift-0.12.0.tar.gz" ]
    then
        echo "thrift-0.12.0.tar.gz  downloaded"
    else
        wget --no-check-certificate -O thrift-0.12.0.tar.gz  http://pkg.4paradigm.com/fesql/thrift-0.12.0.tar.gz
    fi
    tar -zxf thrift-0.12.0.tar.gz
    cd thrift-0.12.0 && ./configure --enable-shared=no --enable-tests=no --with-python=no --with-nodejs=no --prefix=${DEPS_PREFIX} && make -j4 && make install
    cd ${DEPS_SOURCE}
    touch thrift_succ
fi


if [ -f "glog_succ" ]
then 
    echo "glog exist"
else
    wget --no-check-certificate -O glogs-v0.4.tar.gz https://github.com/google/glog/archive/v0.4.0.tar.gz
    tar zxf glogs-v0.4.tar.gz
    cd glog-0.4.0
    ./autogen.sh && CXXFLAGS=-fPIC ./configure --prefix=${DEPS_PREFIX} && make install
    cd -
    touch glog_succ
fi

if [ -f "brpc_succ" ]
then
    echo "brpc exist"
else
    #wget http://pkg.4paradigm.com/rtidb/dev/brpc-legacy-1.3.7.tar.gz
    #tar -zxvf brpc-legacy-1.3.7.tar.gz
    if [ -d "incubator-brpc" ]
    then
        rm -rf incubator-brpc
    fi
    wget http://pkg.4paradigm.com/fesql/incubator-brpc.tar.gz
    tar -zxf incubator-brpc.tar.gz
    BRPC_DIR=$DEPS_SOURCE/incubator-brpc
    cd incubator-brpc
    sh config_brpc.sh --with-glog --headers=${DEPS_PREFIX}/include --libs=${DEPS_PREFIX}/lib
    make -j5 libbrpc.a
    make output/include
    cp -rf output/include/* ${DEPS_PREFIX}/include
    cp libbrpc.a ${DEPS_PREFIX}/lib
    cd -
    touch brpc_succ
    echo "brpc done"
fi

cd ${WORK_DIR} && rm -rf ${DEPS_SOURCE}
