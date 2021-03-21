# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#! /bin/sh

PLATFORM=$1
set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
# mac can't support source compile for the followings:
# 1. install cppunit: brew install cppunit
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

cd ${DEPS_SOURCE}

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

if [ -f "llvm_succ" ]
then 
    echo "llvm_exist"
else
    if [ ! -d "llvm-9.0.0.src" ]
    then
        wget --no-check-certificate -O llvm-9.0.0.src.tar.xz http://releases.llvm.org/9.0.0/llvm-9.0.0.src.tar.xz
        tar xf llvm-9.0.0.src.tar.xz
    fi
    cd llvm-9.0.0.src && mkdir -p build
    cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC .. >/dev/null
    make -j12 && make install
    cd ${DEPS_SOURCE}
    touch llvm_succ
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
    if [ -f "apache-zookeeper-3.5.7.tar.gz" ]
        then
            echo "apache-zookeeper-3.5.7.tar.gz exists"
    else
        wget https://mirrors.tuna.tsinghua.edu.cn/apache/zookeeper/zookeeper-3.5.7/apache-zookeeper-3.5.7.tar.gz
    fi
    tar -zxvf apache-zookeeper-3.5.7.tar.gz
    # mac os should
    # brew install cppunit
    # cd apache-zookeeper-3.5.7 && ant compile_jute
    # cd apache-zookeeper-3.5.7/zookeeper-client/zookeeper-client-c
    # autoreconf -if
    # ./configure --prefix=${DEPS_PREFIX}
    # make && make install
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
        echo "incubator-brpc exists"
    else
        if [ -f "incubator-brpc.0304.tar.gz" ]
        then
            echo "incubator-brpc.tar.gz exists"
            tar -zxvf incubator-brpc.0304.tar.gz
        else
            wget http://pkg.4paradigm.com/hybridse/incubator-brpc.0304.tar.gz
        fi
    fi

    BRPC_DIR=$DEPS_SOURCE/incubator-brpc
    cd incubator-brpc && mkdir -p b2
    cd b2 && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DWITH_GLOG=ON .. && make brpc-static 
    cp -rf output/include/* ${DEPS_PREFIX}/include/
    cp -rf output/lib/* ${DEPS_PREFIX}/lib/
    cd ${DEPS_SOURCE}
    touch brpc_succ
    echo "brpc done"
fi

if [ -f "boost_succ" ]
then
    echo "boost exist"
else
    if [ -f "boost_1_69_0.tar.gz" ]
    then
        echo "boost exist"
    else
        wget --no-check-certificate -O boost_1_69_0.tar.gz http://pkg.4paradigm.com/hybridse/boost_1_69_0.tar.gz
    fi
    tar -zxvf boost_1_69_0.tar.gz
    cd boost_1_69_0 && ./bootstrap.sh --with-toolset=clang  && ./b2 install --prefix=${DEPS_PREFIX}
    #####
    # mac os should install to os for thrif
    # ./b2 install
    ###################
    cd -
    touch boost_succ
fi

if [ -f "benchmark_succ" ]
then
    echo "benchmark exist"
else
    if [ -f "v1.5.0.tar.gz" ]
    then
        echo "benchmark  exist"
    else
        wget --no-check-certificate -O v1.5.0.tar.gz https://github.com/google/benchmark/archive/v1.5.0.tar.gz
    fi

    tar zxf v1.5.0.tar.gz
    cd benchmark-1.5.0 && mkdir -p build
    cd build && cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC -DBENCHMARK_ENABLE_GTEST_TESTS=OFF .. >/dev/null
    make -j4 && make install
    cd ${DEPS_SOURCE}
    touch benchmark_succ
fi


if [ -f "gperf_tool" ]
then
    echo "gperf_tool exist"
else
    wget http://pkg.4paradigm.com/rtidb/dev/gperftools-2.5.tar.gz
    tar -zxvf gperftools-2.5.tar.gz
    cd gperftools-2.5
    ./configure --enable-cpu-profiler --enable-heap-checker --enable-heap-profiler  --enable-static --prefix=${DEPS_PREFIX}
    make -j2 >/dev/null
    make install
    cd -
    touch gperf_tool
fi

if [ -f "lz4_succ" ]
then
    echo " lz4 exist"
else
    if [ -f "lz4-1.7.5.tar.gz" ]
    then
        echo "lz4 tar exist"
    else
        wget --no-check-certificate -O lz4-1.7.5.tar.gz http://pkg.4paradigm.com/hybridse/lz4-1.7.5.tar.gz
    fi
    tar -zxvf lz4-1.7.5.tar.gz
    cd lz4-1.7.5 && make -j4 && make install PREFIX=${DEPS_PREFIX}
    cd ${DEPS_SOURCE}
    touch lz4_succ
fi
if [ -f "zstd_succ" ]
then
    echo "zstd installed"
else
    if [ -f "zstd-1.4.4.tar.gz" ]
    then
        echo "zstd-1.4.4.tar.gz  downloaded"
    else
        wget --no-check-certificate -O zstd-1.4.4.tar.gz http://pkg.4paradigm.com/hybridse/zstd-1.4.4.tar.gz
    fi
    tar -zxvf zstd-1.4.4.tar.gz
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
        wget --no-check-certificate -O thrift-0.12.0.tar.gz  http://pkg.4paradigm.com/hybridse/thrift-0.12.0.tar.gz
    fi
    tar -zxvf thrift-0.12.0.tar.gz
    cd thrift-0.12.0 && ./configure --with-python=no --with-nodejs=no --prefix=${DEPS_PREFIX} --with-boost=${DEPS_PREFIX} && make -j10 && make install
    cd ${DEPS_SOURCE}
    touch thrift_succ
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


if [ -f "flatbuffer_succ" ]
then
    echo "flatbuffer installed"
else
    if [ -f "flatbuffers-1.11.0.tar.gz" ]
    then
        echo "flatbuffers-1.11.0.tar.gz downloaded"
    else
        wget --no-check-certificate -O flatbuffers-1.11.0.tar.gz http://pkg.4paradigm.com/hybridse/flatbuffers-1.11.0.tar.gz
    fi
    tar -zxvf flatbuffers-1.11.0.tar.gz
    cd flatbuffers-1.11.0 && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC ..
    make -j4 && make install
    cd ${DEPS_SOURCE}
    touch flatbuffer_succ
fi
if [ -f "brotli_succ" ]
then
    echo "brotli exist"
else
    if [ -f "v1.0.7.tar.gz" ]
    then
        echo "brotli exist"
    else
        wget --no-check-certificate -O v1.0.7.tar.gz https://github.com/google/brotli/archive/v1.0.7.tar.gz
    fi
    tar -zxvf v1.0.7.tar.gz
    ####
    # mac install
    # mkdir out && cd out
    # cmake -DCMAKE_INSTALL_PREFIX=./installed ..
    # cmake --build . -target install
    ####
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
        wget --no-check-certificate -O v3.1.5.tar.gz https://github.com/google/double-conversion/archive/v3.1.5.tar.gz
    fi
    tar -zxvf v3.1.5.tar.gz
    cd double-conversion-3.1.5 && mkdir -p build2 && cd build2 && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DCMAKE_CXX_FLAGS=-fPIC .. >/dev/null
    make -j4 && make install
    cd ${DEPS_SOURCE}
    touch double-conversion_succ
fi

if [ -f "arrow_succ" ]
then
    echo "arrow installed"
else
    if [ -f "apache-arrow-0.15.1.tar.gz" ]
    then
        echo "apache-arrow-0.15.1.tar.gz   downloaded"
    else
        wget --no-check-certificate -O apache-arrow-0.15.1.tar.gz http://pkg.4paradigm.com/hybridse/apache-arrow-0.15.1.tar.gz
    fi
    tar -zxvf apache-arrow-0.15.1.tar.gz
    ##########
    # mac should install before:
    # 1. brew install thrift
    ##########
    export ARROW_BUILD_TOOLCHAIN=${DEPS_PREFIX}
    export JEMALLOC_HOME=${DEPS_PREFIX}
    cd apache-arrow-0.15.1/cpp && mkdir -p build && cd build
    cmake  -DARROW_JEMALLOC=OFF -DARROW_MIMALLOC=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DARROW_CUDA=OFF -DARROW_FLIGHT=OFF -DARROW_GANDIVA=OFF \
    -DARROW_GANDIVA_JAVA=OFF -DARROW_HDFS=ON -DARROW_HIVESERVER2=OFF \
    -DARROW_ORC=OFF -DARROW_PARQUET=ON -DARROW_PLASMA=OFF\
    -DARROW_PLASMA_JAVA_CLIENT=OFF -DARROW_PYTHON=OFF -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_UTILITIES=OFF ..
    make -j10 parquet_static && make install
    cd ${DEPS_SOURCE}
    touch arrow_succ
fi

if [ -f "yaml_succ" ]
then
    echo "yaml-cpp installed"
else
    if [ -f "yaml-cpp-0.6.3.tar.gz" ]
    then
        echo "yaml-cpp-0.6.3.tar.gz downloaded"
    else
        wget --no-check-certificate -O yaml-cpp-0.6.3.tar.gz https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-0.6.3.tar.gz
    fi
    tar -zxvf yaml-cpp-0.6.3.tar.gz
    cd yaml-cpp-yaml-cpp-0.6.3 && mkdir -p build && cd build
    cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} ..
    make && make install
    touch yaml_succ
fi
