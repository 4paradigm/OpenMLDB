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
    wget http://pkg.4paradigm.com:81/rtidb/dev/boost-header-only.tar.gz >/dev/null
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
   wget http://pkg.4paradigm.com:81/rtidb/dev/gtest-1.7.0.zip >/dev/null
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
    wget http://pkg.4paradigm.com:81/rtidb/dev/protobuf-2.6.1.tar.gz >/dev/null
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
    wget http://pkg.4paradigm.com:81/rtidb/dev/zlib-1.2.11.tar.gz
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
    wget http://pkg.4paradigm.com:81/rtidb/dev/snappy-1.1.1.tar.gz
    tar zxf snappy-1.1.1.tar.gz >/dev/null
    cd snappy-1.1.1
    ./configure ${DEPS_CONFIG} >/dev/null
    make -j2 >/dev/null
    make install
    cd -
    touch snappy_succ
    echo "install snappy done"
fi

if [ -f "sofa_succ" ]
then
    echo "sofa exist"
else
    # sofa-pbrpc
    wget http://pkg.4paradigm.com:81/rtidb/dev/sofa-pbrpc.tar.gz
    tar zxf sofa-pbrpc.tar.gz
    cd sofa-pbrpc
    echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/include" >> depends.mk
    echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "PREFIX=${DEPS_PREFIX}" >> depends.mk
    cd -
    cd sofa-pbrpc/src
    sh compile_proto.sh ${DEPS_PREFIX}/include
    cd -
    cd sofa-pbrpc
    make -j2 >/dev/null
    make install
    cd -
    touch sofa_succ
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
  wget http://pkg.4paradigm.com:81/rtidb/dev/common.tar.gz
  tar -zxvf common.tar.gz
  cd common
  sed -i 's/^INCLUDE_PATH=.*/INCLUDE_PATH=-Iinclude -I..\/..\/thirdparty\/include/' Makefile
  make -j2 >/dev/null
  cp -rf include/* ${DEPS_PREFIX}/include
  cp -rf libcommon.a ${DEPS_PREFIX}/lib
  cd -
  touch common_succ
fi

if [ -f "gperf_tool" ]
then
    echo "gperf_tool exist"
else
    wget http://pkg.4paradigm.com:81/rtidb/dev/gperftools-2.5.tar.gz 
    tar -zxvf gperftools-2.5.tar.gz 
    cd gperftools-2.5 
    if [ "$STAGE" = 'DEBUG' ]
    then
        echo "debug stage"
        ./configure --enable-cpu-profiler --enable-heap-checker --enable-heap-profiler --prefix=${DEPS_PREFIX} 
    else
        echo "prod stage"
        ./configure --disable-cpu-profiler --enable-minimal --disable-heap-checker --disable-heap-profiler --enable-shared=no --prefix=${DEPS_PREFIX} 
    fi
    make -j2 >/dev/null
    make install
    cd -
    touch gperf_tool
fi

if [ -f "rapjson_succ" ]
then 
    echo "rapjson exist"
else
    wget http://pkg.4paradigm.com:81/rtidb/dev/rapidjson.1.1.0.tar.gz
    tar -zxvf rapidjson.1.1.0.tar.gz
    cp -rf rapidjson-1.1.0/include/rapidjson ${DEPS_PREFIX}/include
    touch rapjson_succ
fi

if [ -f "leveldb_succ" ]
then
    echo "leveldb exist"
else
    wget http://pkg.4paradigm.com:81/rtidb/dev/leveldb.tar.gz
    tar -zxvf leveldb.tar.gz
    cd leveldb
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
    wget http://pkg.4paradigm.com:81/rtidb/dev/zookeeper-3.4.10.tar.gz
    tar -zxvf zookeeper-3.4.10.tar.gz
    cd zookeeper-3.4.10/src/c/
    ./configure --prefix=${DEPS_PREFIX} --enable-shared=no --enable-static=yes
    make -j4 >/dev/null 
    make install
    cd -
    touch zk_succ
fi

