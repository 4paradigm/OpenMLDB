#!/bin/bash

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

# install_deps.sh: build and install dependencies into to specific directory.
# Installation directory is /depends/thirdparty, while source file located in
# /depends/thirdsrc

set -eE
set -x

if [ -d '/opt/rh/devtoolset-7' ] ; then
    # shellcheck disable=SC1091
    source /opt/rh/devtoolset-7/enable
fi

if [ -d '/opt/rh/sclo-git218' ] ; then
    # shellcheck disable=SC1091
    source /opt/rh/sclo-git218/enable
fi

if [ -d '/opt/rh/rh-python38' ] ; then
    # shellcheck disable=SC1091
    source /opt/rh/rh-python38/enable
fi

DEPS_SOURCE=/depends/thirdsrc
DEPS_PREFIX=/depends/thirdparty
DEPS_CONFIG="--prefix=$DEPS_PREFIX --disable-shared --with-pic"

export CXXFLAGS=" -O3 -fPIC"
export CFLAGS=" -O3 -fPIC"

mkdir -p "$DEPS_PREFIX/lib" "$DEPS_PREFIX/include" "$DEPS_SOURCE"
export PATH=${DEPS_PREFIX}/bin:$PATH

if ! command -v nproc; then
	alias nproc='sysctl -n hw.logicalcpu'
fi

pushd "$DEPS_SOURCE"

# install cmake
tar xzf cmake-3.19.*
pushd cmake-3.19.7-Linux-x86_64
find . -type f -exec install -D -m 755 {} /usr/local/{} \; > /dev/null
popd

if [ ! -f gtest_succ ]; then
	echo "installing gtest ...."
	tar xzf googletest-release-1.10.0.tar.gz

	pushd googletest-release-1.10.0
	cmake -H. -Bbuild -DCMAKE_INSTALL_PREFIX="$DEPS_PREFIX" -DCMAKE_CXX_FLAGS=-fPIC
	cmake --build build -- "-j$(nproc)"
    cmake --build build --target install
	popd

	touch gtest_succ
	echo "install gtest done"
fi

if [ -f "zlib_succ" ]; then
	echo "zlib exist"
else
	echo "installing zlib..."
	tar xzf zlib-1.2.11.tar.gz
	pushd zlib-1.2.11
	sed -i '/CFLAGS="${CFLAGS--O3}"/c\  CFLAGS="${CFLAGS--O3} -fPIC"' configure
	./configure --static --prefix="$DEPS_PREFIX"
	make -j"$(nproc)"
	make install
	popd
	touch zlib_succ
	echo "install zlib done"
fi

if [ -f "protobuf_succ" ]; then
	echo "protobuf exist"
else
	echo "start install protobuf ..."
	tar zxf protobuf-2.6.1.tar.gz

	pushd protobuf-2.6.1
	./configure $DEPS_CONFIG CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
	make -j"$(nproc)"
	make install
	popd

	touch protobuf_succ
	echo "install protobuf done"
fi

if [ -f "snappy_succ" ]; then
	echo "snappy exist"
else
	echo "start install snappy ..."
	tar zxf snappy-1.1.1.tar.gz
	pushd snappy-1.1.1
	./configure $DEPS_CONFIG
	make "-j$(nproc)"
	make install
	popd

	touch snappy_succ
	echo "install snappy done"
fi

if [ -f "gflags_succ" ]; then
	echo "gflags-2.2.0.tar.gz exist"
else
	tar zxf gflags-2.2.0.tar.gz
	pushd gflags-2.2.0
	cmake -H. -Bbuild -DCMAKE_INSTALL_PREFIX="$DEPS_PREFIX" -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC
	cmake --build build -- "-j$(nproc)"
    cmake --build build --target install
	popd

	touch gflags_succ
	echo "install gflags done"
fi

if [ -f "unwind_succ" ]; then
	echo "unwind_exist"
else
	tar zxf libunwind-1.1.tar.gz
	pushd libunwind-1.1
	autoreconf -i
	./configure --prefix="$DEPS_PREFIX"
    make -j"$(nproc)"
    make install
	popd

	touch unwind_succ
fi

if [ -f "gperf_tool" ]; then
	echo "gperf_tool exist"
else
	tar zxf gperftools-2.5.tar.gz
	pushd gperftools-2.5
	./configure --enable-cpu-profiler --enable-heap-checker --enable-heap-profiler --enable-static --prefix="$DEPS_PREFIX"
	make "-j$(nproc)"
	make install
	popd
	touch gperf_tool
fi

if [ -f "leveldb_succ" ]; then
	echo "leveldb exist"
else
    # TODO fix compile on leveldb 1.23
	tar zxf leveldb-1.20.tar.gz
	pushd leveldb-1.20
	sed -i 's/^OPT ?= -O2 -DNDEBUG/OPT ?= -O2 -DNDEBUG -fPIC/' Makefile
	make "-j$(nproc)"
	cp -rf include/* "$DEPS_PREFIX/include"
	cp out-static/libleveldb.a "$DEPS_PREFIX/lib"
	popd
	touch leveldb_succ
fi

if [ -f "openssl_succ" ]; then
	echo "openssl exist"
else
	unzip OpenSSL_1_1_0.zip
	pushd openssl-OpenSSL_1_1_0
	./config --prefix="$DEPS_PREFIX" --openssldir="$DEPS_PREFIX"
	make "-j$(nproc)"
	make install
	rm -rf "$DEPS_PREFIX"/lib/libssl.so*
	rm -rf "$DEPS_PREFIX"/lib/libcrypto.so*
	popd
	touch openssl_succ
	echo "openssl done"
fi

if [ -f "glog_succ" ]; then
	echo "glog exist"
else
	echo "installing glog ..."
	tar xzf glog-0.4.0.tar.gz
	pushd glog-0.4.0
	./autogen.sh && CXXFLAGS=-fPIC ./configure --prefix="$DEPS_PREFIX"
	make -j"$(nproc)" install
	popd
	touch glog_succ
	echo "installed glog"
fi

if [ -f "brpc_succ" ]; then
	echo "brpc exist"
else
	unzip incubator-brpc.zip
	pushd incubator-brpc-*
	sh config_brpc.sh --with-glog --headers="$DEPS_PREFIX/include" --libs="$DEPS_PREFIX/lib"
	make "-j$(nproc)" libbrpc.a output/include
	cp -rf output/include/* "$DEPS_PREFIX/include/"
	cp libbrpc.a "$DEPS_PREFIX/lib"
	popd

	touch brpc_succ
	echo "brpc done"
fi

if [ -f "zk_succ" ]
then
    echo "zk exist"
else
    tar -zxf apache-zookeeper-3.4.14.tar.gz
    pushd zookeeper-3.4.14/zookeeper-client/zookeeper-client-c
    autoreconf -if
    ./configure --prefix="$DEPS_PREFIX"
    make -j"$(nproc)"
    make install
    popd
    touch zk_succ
    echo "installed zookeeper c"
fi

if [ -f "bison_succ" ]; then
	echo "bison exist"
else
	tar zxf bison-3.4.tar.gz
	pushd bison-3.4
	./configure --prefix="$DEPS_PREFIX"
	make install
	popd
	touch bison_succ
fi

if [ -f "benchmark_succ" ]; then
	echo "benchmark exist"
else
	tar zxf v1.5.0.tar.gz
	pushd benchmark-1.5.0
	mkdir -p build
	cd build
	cmake -DCMAKE_INSTALL_PREFIX="$DEPS_PREFIX" -DCMAKE_CXX_FLAGS=-fPIC -DBENCHMARK_ENABLE_GTEST_TESTS=OFF ..
	make -j"$(nproc)"
	make install
	popd
	touch benchmark_succ
fi

if [ -f "swig_succ" ]; then
	echo "swig exist"
else
	tar -zxf swig-4.0.1.tar.gz
	pushd swig-4.0.1
	./autogen.sh
	./configure --without-pcre --prefix="$DEPS_PREFIX"
	make -j"$(nproc)"
	make install
	popd
	touch swig_succ
fi

if [ -f "yaml_succ" ]; then
	echo "yaml-cpp installed"
else
	tar -zxf yaml-cpp-0.6.3.tar.gz
	pushd yaml-cpp-yaml-cpp-0.6.3
	mkdir -p build
	cd build
	cmake -DCMAKE_INSTALL_PREFIX="$DEPS_PREFIX" ..
	make -j"$(nproc)"
	make install
	popd
	touch yaml_succ
fi

if [ -f "sqlite_succ" ]; then
	echo "sqlite installed"
else
	unzip sqlite-*.zip
	pushd sqlite-version-3.32.3
	mkdir -p build
	cd build
	../configure --prefix="$DEPS_PREFIX"
	make -j"$(nproc)" && make install
	popd
	touch sqlite_succ
fi

if [ -f "llvm_succ" ]; then
	echo "llvm_exist"
else
	tar xf llvm-9.0.0.src.tar.xz
	pushd llvm-9.0.0.src
	mkdir -p build && cd build
	cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$DEPS_PREFIX" -DLLVM_TARGETS_TO_BUILD=X86  -DCMAKE_CXX_FLAGS=-fPIC ..
	make "-j$(nproc)"
	make install
	popd
	touch llvm_succ
fi

if [ -f "boost_succ" ]; then
	echo "boost exist"
else
	tar -zxf boost_1_69_0.tar.gz
	pushd boost_1_69_0
	./bootstrap.sh
	./b2 link=static cxxflags=-fPIC cflags=-fPIC release install --prefix="$DEPS_PREFIX"
	popd
	touch boost_succ
fi

# echo "installing python3"
# tar xzf Python-3.8.8.tgz
# pushd Python-3.8.8
# ./configure --prefix=$DEPS_PREFIX --with-openssl=$DEPS_PREFIX
# make -j"$(nproc)"
# make install
# popd
# echo "python3 installed"

echo "installing common"
tar xzf common-1.0.0.tar.gz
pushd common-1.0.0
make -j"$(nproc)" INCLUDE_PATH="-Iinclude -I$DEPS_PREFIX/include" PREFIX=/depends/thirdparty  install
popd
echo "common installed"

echo "installing doxygen ..."
tar xzf doxygen-1.8.19.src.tar.gz
pushd doxygen-Release_1_8_19

sed -i '/pedantic/d' ./cmake/CompilerWarnings.cmake
sed -i '/double-promotion/d' ./cmake/CompilerWarnings.cmake
mkdir build
cd build
cmake -G "Unix Makefiles" ..
make -j"$(nproc)"
make install

echo "installed doxygen"
popd

# install maven
tar xzf apache-maven-3.6.3-bin.tar.gz
mv apache-maven-3.6.3 "/opt/maven"

# Remove dynamic library files for static link
find /depends/thirdparty/lib/ -name "lib*so*" | grep -v "libRemarks" | grep -v "libLTO" | xargs rm
find /depends/thirdparty/lib64/ -name "lib*so*" | grep -v "libRemarks" | grep -v "libLTO" | xargs rm

popd
