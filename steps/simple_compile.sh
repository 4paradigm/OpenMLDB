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
#
# compile.sh
CMAKE_TYPE=$1
if [[ "${CMAKE_TYPE}" != "Debug" ]]; then
        CMAKE_TYPE="RelWithDebInfo"
fi
WORK_DIR=`pwd`

mkdir -p $WORK_DIR/build
cd $WORK_DIR/build && cmake -DCMAKE_BUILD_TYPE=${CMAKE_TYPE} .. && make -j5 fedb sql_sdk_test sql_cluster_test sql_pysdk_package sql_jsdk parse_log sqlalchemy_fedb
code=$?
cd $WORK_DIR
exit $code
