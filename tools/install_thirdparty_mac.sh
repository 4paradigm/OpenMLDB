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
set -e

echo "Install thirdparty for MacOS"
echo "CICD_RUNNER_THIRDPARTY_PATH: ${CICD_RUNNER_THIRDPARTY_PATH}"
echo "CICD_RUNNER_THIRDSRC_PATH: ${CICD_RUNNER_THIRDSRC_PATH}"

mkdir "${CICD_RUNNER_THIRDPARTY_PATH}"
mkdir "${CICD_RUNNER_THIRDSRC_PATH}"
pushd "${CICD_RUNNER_THIRDSRC_PATH}"

# download thirdparty-mac
wget -nv --show-progress https://github.com/jingchen2222/hybridsql-asserts/releases/download/v0.3.1/thirdparty-2021-05-27-drawin-x86_64.tar.gz
tar xzf thirdparty-2021-05-27-drawin-x86_64.tar.gz -C  "${CICD_RUNNER_THIRDPARTY_PATH}" --strip-components 1
# download and install libzetasql
wget -nv --show-progress -O libzetasql.tar.gz https://github.com/jingchen2222/zetasql/releases/download/v0.2.0-beta7/libzetasql-0.2.0-beta7-darwin-x86_64.tar.gz
tar xzf libzetasql.tar.gz -C  "${CICD_RUNNER_THIRDPARTY_PATH}" --strip-components 1
echo "list files under ${CICD_RUNNER_THIRDPARTY_PATH}"

if [ -f "bison_succ" ]; then
  echo "bison exist"
else
  wget --no-check-certificate -O bison-3.4.tar.gz http://ftp.gnu.org/gnu/bison/bison-3.4.tar.gz
  tar zxf bison-3.4.tar.gz
  cd bison-3.4
  ./configure --prefix="${CICD_RUNNER_THIRDPARTY_PATH}" && make install
  cd -
  touch bison_succ
fi

if [ -f "gflags_succ" ]; then
  echo "gflags exist"
else
  wget --no-check-certificate -O gflags-2.2.2.tar.gz https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz
  tar zxf gflags-2.2.2.tar.gz
  cd gflags-2.2.2
  mkdir -p b2
  cd b2
  cmake -DCMAKE_INSTALL_PREFIX="${CICD_RUNNER_THIRDPARTY_PATH}" -DCMAKE_CXX_FLAGS=-fPIC -DBUILD_SHARED_LIBS=ON -DBUILD_gflags_nothreads_LIB=ON ..
  make install -j8
  cd -
  touch gflags_succ
fi

if [ -f "glog_succ" ]; then
  echo "glog exist"
else
  wget --no-check-certificate -O glog-0.4.0.tar.gz https://github.com/google/glog/archive/refs/tags/v0.4.0.tar.gz
  tar zxf glog-0.4.0.tar.gz
  cd glog-0.4.0
  mkdir -p b2
  cd b2
  cmake -DCMAKE_INSTALL_PREFIX="${CICD_RUNNER_THIRDPARTY_PATH}" -DCMAKE_CXX_FLAGS=-fPIC ..
  make install -j8
  cd -
  touch glog_succ
fi
popd

pushd /usr/local/opt
ln -sf ${CICD_RUNNER_THIRDPARTY_PATH} gflags
ln -sf ${CICD_RUNNER_THIRDPARTY_PATH} glog
popd