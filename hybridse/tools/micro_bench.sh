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

#
# micro_bench.sh

set -eE
# goto toplevel directory
pushd "$(dirname "$0")/../.."
OPENMLDB_DIR=$(pwd)
popd

# goto hybridse directory
cd "$(dirname "$0")/.."
# install thirdparty hybrise
HYBRIDSE_THIRDPARTY="$(pwd)/thirdparty"
../steps/setup_thirdparty.sh ${HYBRIDSE_THIRDPARTY}

if uname -a | grep -q Darwin; then
  # in case coreutils not install on mac
  alias nproc='sysctl -n hw.logicalcpu'
fi

rm -rf build
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTESTING_ENABLE=OFF -DBENCHMARK_ENABLE=ON -DEXAMPLES_ENABLE=ON -DJAVASDK_ENABLE=OFF
make -j"$(nproc)" hybridse_bm toydb_bm
if [ -f src/benchmark/udf_bm ]; then
  echo "udf benchmark:"
  src/benchmark/udf_bm 2>/dev/null
else
  echo "udf_bm not exist, aborting"
  exit
fi

if [ -f examples/toydb/src/bm/storage_bm ]; then
  echo "toydb storage benchmark:"
  examples/toydb/src/bm/storage_bm 2>/dev/null
else
  echo "examples/toydb/src/bm/storage_bm not exist, aborting"
  exit
fi
if [ -f examples/toydb/src/bm/engine_bm ]; then
  echo "toydb engine benchmark:"
  SQL_CASE_BASE_DIR=${OPENMLDB_DIR} examples/toydb/src/bm/engine_bm 2>/dev/null
else
  echo "examples/toydb/src/bm/engine_bm not exist, aborting"
  exit
fi

if [ -f examples/toydb/src/bm/client_batch_run_bm ]; then
  echo "toydb client batch run benchmark:"
  SQL_CASE_BASE_DIR=${OPENMLDB_DIR} examples/toydb/src/bm/client_batch_run_bm 2>/dev/null
else
  echo "examples/toydb/src/bm/client_batch_run_bm not exist, aborting"
  exit
fi

if [ -f examples/toydb/src/bm/batch_request_bm ]; then
  echo "toydb batch request benchmark:"
  SQL_CASE_BASE_DIR=${OPENMLDB_DIR} examples/toydb/src/bm/batch_request_bm 2>/dev/null
else
  echo "examples/toydb/src/bm/batch_request_bm not exist, aborting"
  exit
fi
