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

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

echo "Install thirdparty for MacOS"

# on local machine, one can tweak thirdparty path by passing extra argument
THIRDPARTY_PATH=${1:-"$ROOT/thirdparty"}
THIRDSRC_PATH="$ROOT/thirdsrc"

echo "CICD_RUNNER_THIRDPARTY_PATH: ${THIRDPARTY_PATH}"
echo "CICD_RUNNER_THIRDSRC_PATH: ${THIRDSRC_PATH}"

mkdir -p "$THIRDPARTY_PATH"
mkdir -p "$THIRDSRC_PATH"

pushd "${THIRDSRC_PATH}"

# download thirdparty-mac
curl -SLO https://github.com/jingchen2222/hybridsql-asserts/releases/download/v0.3.1/thirdparty-2021-05-27-drawin-x86_64.tar.gz
tar xzf thirdparty-2021-05-27-drawin-x86_64.tar.gz -C "${THIRDPARTY_PATH}" --strip-components 1
# download and install libzetasql
curl -SLo libzetasql.tar.gz https://github.com/jingchen2222/zetasql/releases/download/v0.2.0/libzetasql-0.2.0-darwin-x86_64.tar.gz
tar xzf libzetasql.tar.gz -C "${THIRDPARTY_PATH}" --strip-components 1
echo "list files under ${THIRDPARTY_PATH}"

# TODO: use new mac thirdparty assert from https://github.com/jingchen2222/hybridsql-asserts/pull/7
# v0.3.1 thirdparty assert have dylib for glog and gflags only
rm -vf "$THIRDPARTY_PATH/lib/"libgflags*.dylib
rm -vf "$THIRDPARTY_PATH/lib/"libglog*.dylib

curl -SLo gflags-2.2.2.tar.gz https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz
tar zxf gflags-2.2.2.tar.gz

cmake -Hgflags-2.2.2 -Bb2 -DCMAKE_INSTALL_PREFIX="${THIRDPARTY_PATH}" -DCMAKE_CXX_FLAGS=-fPIC -DBUILD_STATIC_LIBS=ON
cmake --build b2 --target install

curl -SLo glog-0.4.0.tar.gz https://github.com/google/glog/archive/refs/tags/v0.4.0.tar.gz
tar zxf glog-0.4.0.tar.gz
pushd glog-0.4.0
./autogen.sh && CXXFLAGS=-fPIC ./configure --prefix="$THIRDPARTY_PATH" --enable-shared=no
make install
popd

popd
