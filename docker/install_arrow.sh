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

cd ${DEPS_SOURCE}

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
    tar -zxvf boost_1_69_0.tar.gz
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
    tar -zxvf v1.0.7.tar.gz
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
    tar -zxvf v3.1.5.tar.gz
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
    tar -zxvf lz4-1.7.5.tar.gz 
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
    tar -zxvf bzip2-1.0.8.tar.gz 
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
    tar -zxvf jemalloc-5.2.1.tar.gz
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
    tar -zxvf flatbuffers-1.11.0.tar.gz
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
        wget --no-check-certificate -O thrift-0.12.0.tar.gz  http://pkg.4paradigm.com/fesql/thrift-0.12.0.tar.gz
    fi
    tar -zxvf thrift-0.12.0.tar.gz
    cd thrift-0.12.0 && ./configure --with-python=no --with-nodejs=no --prefix=${DEPS_PREFIX} && make -j4 && make install
    cd ${DEPS_SOURCE}
    touch thrift_succ
fi

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
    tar -zxvf apache-arrow-0.15.1.tar.gz
    export ARROW_BUILD_TOOLCHAIN=${DEPS_PREFIX}
    export JEMALLOC_HOME=${DEPS_PREFIX}
    cd apache-arrow-0.15.1/cpp && mkdir -p build && cd build
    cmake  -DARROW_JEMALLOC=OFF -DARROW_MIMALLOC=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DARROW_CUDA=OFF -DARROW_FLIGHT=OFF -DARROW_GANDIVA=OFF \
    -DARROW_GANDIVA_JAVA=OFF -DARROW_HDFS=ON -DARROW_HIVESERVER2=OFF \
    -DARROW_ORC=OFF -DARROW_PARQUET=ON -DARROW_PLASMA=OFF\
    -DARROW_PLASMA_JAVA_CLIENT=OFF -DARROW_PYTHON=OFF -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_UTILITIES=OFF ..
    make -j4 parquet && make install 
    cd ${DEPS_SOURCE}
    touch arrow_succ
fi
