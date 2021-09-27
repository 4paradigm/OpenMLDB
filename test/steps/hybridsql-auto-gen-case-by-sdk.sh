#!/usr/bin/env bash

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


while getopts ":b:c:d:l:" opt
do
   case $opt in
        d)
        echo "参数d的值:$OPTARG"
        DEPLOY_MODE=$OPTARG
        ;;
        ?) echo "未知参数"
           exit 1
        ;;
   esac
done
if [[ "${DEPLOY_MODE}" == "" ]]; then
    DEPLOY_MODE="cluster"
fi

echo "DEPLOY_MODE:${DEPLOY_MODE}"

ROOT_DIR=$(pwd)
ulimit -c unlimited

#udf_defs.yaml 一般改动很小 为了节省性能 我们不用每次生成
#sh steps/gen_code.sh
#export JAVA_HOME=${RTIDB_DEV_JAVA_HOME:-/depends/thirdparty/jdk1.8.0_141}
#export RTIDB_THIRDPARTY=${RTIDB_DEV_THIRDPARTY:-/depends/thirdparty}
#cd fesql && ln -sf ${RTIDB_THIRDPARTY} thirdparty && mkdir -p build
#cd build && cmake .. && make fesql_proto && make fesql_parser && make -j5
#cd ${ROOT_DIR}
#./fesql/build/src/export_udf_info --output_file=fesql/tools/autotest/udf_defs.yaml
python3 -m ensurepip
python3 -m pip install numpy
python3 -m pip install PyYaml

cd python/hybridsql_gen_case || exit
sh steps/gen_auto_case.sh

cd "${ROOT_DIR}" || exit
sh steps/fedb-sdk-test-java.sh -b PKG -d "${DEPLOY_MODE}" -c test_auto_gen_case.xml -l "0"
