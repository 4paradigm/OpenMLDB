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

# fetch_resource.sh: fetch all source/binary files from network,
# downloaded files were saved into $DOWNLOAD_DIR

set -eE

GREEN='\033[0;32m'
NC='\033[0m'
DOWNLOAD_DIR=${1:-/depends/thirdsrc}

fetch()
{
    if [ $# -ne 2 ]; then
        echo "usage: fetch url output_file"
        exit 1
    fi
    local url=$1
    local file_name=$2
    if [ ! -e  "$file_name" ]; then
        echo -e "${GREEN}downloading $url ...${NC}"
        curl -SL -o "$file_name" "$url"
        echo -e "${GREEN}download $url${NC}"
    fi
}

mkdir -p "$DOWNLOAD_DIR"
pushd "$DOWNLOAD_DIR"

echo -e "${GREEN}downloading resource into $DOWNLOAD_DIR${NC}"

# cmake
fetch https://github.com/Kitware/CMake/releases/download/v3.19.7/cmake-3.19.7-Linux-x86_64.tar.gz cmake-3.19.7-Linux-x86_64.tar.gz

# google test
fetch https://github.com/google/googletest/archive/refs/tags/release-1.10.0.tar.gz googletest-release-1.10.0.tar.gz

# zlib
fetch https://github.com/madler/zlib/archive/v1.2.11.tar.gz zlib-1.2.11.tar.gz

# snappy
fetch https://src.fedoraproject.org/lookaside/pkgs/snappy/snappy-1.1.1.tar.gz/8887e3b7253b22a31f5486bca3cbc1c2/snappy-1.1.1.tar.gz snappy-1.1.1.tar.gz

# gflags
fetch https://github.com/gflags/gflags/archive/refs/tags/v2.2.0.tar.gz gflags-2.2.0.tar.gz
# libunwind
fetch https://github.com/libunwind/libunwind/archive/refs/tags/v1.1.tar.gz libunwind-1.1.tar.gz
# gperftools
fetch https://github.com/gperftools/gperftools/releases/download/gperftools-2.5/gperftools-2.5.tar.gz gperftools-2.5.tar.gz
# leveldb
fetch https://github.com/google/leveldb/archive/refs/tags/v1.20.tar.gz leveldb-1.20.tar.gz
# openssl
fetch https://github.com/openssl/openssl/archive/OpenSSL_1_1_0.zip OpenSSL_1_1_0.zip
# glog
fetch https://github.com/google/glog/archive/refs/tags/v0.4.0.tar.gz glog-0.4.0.tar.gz
# bison
fetch https://ftp.gnu.org/gnu/bison/bison-3.4.tar.gz bison-3.4.tar.gz

# swig
fetch https://github.com/swig/swig/archive/v4.0.1.tar.gz swig-4.0.1.tar.gz

# yaml-cpp
fetch https://github.com/jbeder/yaml-cpp/archive/refs/tags/yaml-cpp-0.6.3.tar.gz yaml-cpp-0.6.3.tar.gz

# sqlite
fetch https://github.com/sqlite/sqlite/archive/version-3.32.3.zip sqlite-3.32.3.zip

# protobuf
fetch https://github.com/protocolbuffers/protobuf/releases/download/v2.6.1/protobuf-2.6.1.tar.gz protobuf-2.6.1.tar.gz

# llvm
fetch https://releases.llvm.org/9.0.0/llvm-9.0.0.src.tar.xz llvm-9.0.0.src.tar.xz

# boost
fetch https://boostorg.jfrog.io/artifactory/main/release/1.69.0/source/boost_1_69_0.tar.gz boost_1_69_0.tar.gz

# google benchmark
fetch https://github.com/google/benchmark/archive/v1.5.0.tar.gz v1.5.0.tar.gz

# incubator brpc
fetch https://github.com/4paradigm/incubator-brpc/archive/4f69bc0c04abc0734962722ba43aecb4dd7a5dea.zip incubator-brpc.zip

# maven
fetch https://mirrors.ocf.berkeley.edu/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz apache-maven-3.6.3-bin.tar.gz

# scala
fetch https://downloads.lightbend.com/scala/2.12.8/scala-2.12.8.rpm scala-2.12.8.rpm

# doxygen
fetch https://github.com/doxygen/doxygen/archive/Release_1_8_19.tar.gz doxygen-1.8.19.src.tar.gz

# zookeeper
fetch https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz apache-zookeeper-3.4.14.tar.gz

# baidu common
fetch https://github.com/4paradigm/common/archive/refs/tags/v1.0.0.tar.gz common-1.0.0.tar.gz

popd
