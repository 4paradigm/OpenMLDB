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
    if [ ""$PLATFORM == "mac" ]
    then
        gsed -i '/CFLAGS="${CFLAGS--O3}"/c\  CFLAGS="${CFLAGS--O3} -fPIC"' configure
    else
        sed -i '/CFLAGS="${CFLAGS--O3}"/c\  CFLAGS="${CFLAGS--O3} -fPIC"' configure
    fi
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


if [ -f "unwind_succ" ] 
then
    echo "unwind_exist"
else
    wget http://pkg.4paradigm.com/rtidb/dev/libunwind-1.1.tar.gz  
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
    #wget http://pkg.4paradigm.com/rtidb/dev/gperftools-2.5.tar.gz
    #tar -zxvf gperftools-2.5.tar.gz
    #cd gperftools-2.5
    #wget https://github.com/gperftools/gperftools/releases/download/gperftools-2.7/gperftools-2.7.tar.gz
    tar xaf gperftools-2.7.tar.gz
    cd gperftools-2.7
    #./configure --enable-cpu-profiler --enable-heap-checker --enable-heap-profiler  --enable-static --prefix=${DEPS_PREFIX}
    ./configure --prefix=${DEPS_PREFIX}
    make -j2 >/dev/null
    make install
    cd -
    touch gperf_tool
fi

if [ -f "rapjson_succ" ]
then 
    echo "rapjson exist"
else
    wget http://pkg.4paradigm.com/rtidb/dev/rapidjson.1.1.0.tar.gz
    tar -zxvf rapidjson.1.1.0.tar.gz
    cp -rf rapidjson-1.1.0/include/rapidjson ${DEPS_PREFIX}/include
    touch rapjson_succ
fi

if [ -f "leveldb_succ" ]
then
    echo "leveldb exist"
else
    wget http://pkg.4paradigm.com/rtidb/dev/leveldb.tar.gz
    tar -zxvf leveldb.tar.gz
    cd leveldb
    if [ ""$PLATFORM == "mac" ]
    then
        gsed -i 's/^OPT ?= -O2 -DNDEBUG/OPT ?= -O2 -DNDEBUG -fPIC/' Makefile
    else
        sed -i 's/^OPT ?= -O2 -DNDEBUG/OPT ?= -O2 -DNDEBUG -fPIC/' Makefile
    fi
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
    cd glog-0.4.0
    ./autogen.sh && ./configure --prefix=${DEPS_PREFIX} && make install
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
    git clone https://github.com/apache/incubator-brpc.git

    BRPC_DIR=$DEPS_SOURCE/incubator-brpc
    cd incubator-brpc
    if [ ""$PLATFORM != "mac" ]
    then
        sh config_brpc.sh --with-glog --headers=${DEPS_PREFIX}/include --libs=${DEPS_PREFIX}/lib
    else
        sh config_brpc.sh --with-glog --headers=${DEPS_PREFIX}/include --libs=${DEPS_PREFIX}/lib --cc=clang --cxx=clang++
    fi
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
    wget https://mirrors.tuna.tsinghua.edu.cn/apache/zookeeper/zookeeper-3.5.5/apache-zookeeper-3.5.5.tar.gz
    tar -zxvf apache-zookeeper-3.5.5.tar.gz
    cd apache-zookeeper-3.5.5/zookeeper-client/zookeeper-client-c && mkdir -p build
    cd build && cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC ..  && make && make install
    cd ${DEPS_SOURCE}
    touch zk_succ
fi

if [ -f "rocksdb_succ" ]
then
    echo "rocksdb exist"
else
    echo "start install rocksdb ..."
    # rocksdb
    wget http://pkg.4paradigm.com/rtidb/dev/rocksdb-5.18.3.tar.gz >/dev/null
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


if [ -f "jit_succ" ]
then 
    echo "jit exist"
else
    wget --no-check-certificate -O libjit-0.1.4.tar.gz http://git.savannah.gnu.org/cgit/libjit.git/snapshot/libjit-0.1.4.tar.gz
    tar zxf libjit-0.1.4.tar.gz
    cd libjit-0.1.4
    ./bootstrap && ./configure --prefix=${DEPS_PREFIX} && make install
    cd -
    touch jit_succ
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
    wget --no-check-certificate -O flex-2.6.4.tar.gz https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz
    tar zxf flex-2.6.4.tar.gz
    cd flex-2.6.4
    ./autogen.sh && ./configure --prefix=${DEPS_PREFIX} && make install
    cd -
    touch flex_succ
fi

if [ -f "flatc_succ" ]
then
    echo "flatc exist"
else
    wget --no-check-certificate -O 1.11.0.tar.gz https://github.com/google/flatbuffers/archive/1.11.0.tar.gz
    tar zxf 1.11.0.tar.gz
    cd flatbuffers-1.11.0 &&  mkdir cmake-build
    if [ ""$PLATFORM != "mac" ]
    then
        cd cmake-build && cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC .. >/dev/null
    else
        cd cmake-build && cmake -G"Unix Makefiles" -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC .. >/dev/null
    fi
    make -j4 && make install
    cd ${DEPS_SOURCE}
    touch flatc_succ
fi

if [ -f "benchmark_succ" ]
then
    echo "benchmark exist"
else
    wget --no-check-certificate -O v1.5.0.tar.gz https://github.com/google/benchmark/archive/v1.5.0.tar.gz
    tar zxf v1.5.0.tar.gz
    cd benchmark-1.5.0 && mkdir -p build
    cd build && cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC -DBENCHMARK_ENABLE_GTEST_TESTS=OFF .. >/dev/null
    make -j4 && make install
    cd ${DEPS_SOURCE}
    touch benchmark_succ
fi

if [ -f "llvm_succ" ]
then 
    echo "llvm_exist"
else
    wget --no-check-certificate -O llvm-9.0.0.src.tar.xz http://releases.llvm.org/9.0.0/llvm-9.0.0.src.tar.xz
    tar xf llvm-9.0.0.src.tar.xz
    cd llvm-9.0.0.src && mkdir -p build
    cd build && cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC .. >/dev/null
    make -j12 && make install
    cd ${DEPS_SOURCE}
    touch llvm_succ
fi


if [ -d "xz-5.2.4" ] 
then
    echo "zx exist"
else
    wget --no-check-certificate -O xz-5.2.4.tar.gz https://tukaani.org/xz/xz-5.2.4.tar.gz
    tar -zxvf xz-5.2.4.tar.gz
    cd xz-5.2.4 && ./configure --prefix=${DEPS_PREFIX} && make -j4 && make install
    cd -
    touch xz_succ
fi

