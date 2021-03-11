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

ROOT_DIR=`pwd`
ulimit -c unlimited

export JAVA_HOME=${FEDB_DEV_JAVA_HOME:-/depends/thirdparty/jdk1.8.0_141}
export FEDB_THIRDPARTY=${FEDB_DEV_THIRDPARTY:-/depends/thirdparty}
cd fesql && ln -sf ${FEDB_THIRDPARTY} thirdparty && mkdir -p build
cd build && cmake .. && make fesql_proto && make fesql_parser && make -j5
cd ${ROOT_DIR}
./fesql/build/src/export_udf_info --output_file=./udf_defs.yaml
python3 -m pip install numpy -i https://pypi.tuna.tsinghua.edu.cn/simple
python3 -m pip install PyYaml -i https://pypi.tuna.tsinghua.edu.cn/simple
python3 fesql/tools/autotest/auto_cases.py  \
    --bin_path=fesql/build  \
    --udf_path=udf_defs.yaml  \
    --expr_num=2  \
    --expr_depth=3  \
    --max_cases=1 \
    --workers=4

ls -l logs
failed_num=`ls logs | wc -l`
echo "failed_num=$failed_num"
echo "CI_COMMIT_SHA:$CI_COMMIT_SHA"
tar czvf $CI_COMMIT_SHA logs
if [ $failed_num -gt 0 ];then
  sh -x steps/upload_to_pkg.sh http://pkg.4paradigm.com:81/rtidb/test/fesql-log/ $CI_COMMIT_SHA
  exit 1
else
  exit 0
fi
#
#cd ${ROOT_DIR}
#echo "CCC"
#ls -al fesql/build
#echo "DDDD"
#ls -al fesql/build/src/
#
#cd ${ROOT_DIR}/src/sdk/java/
#mvn install -Dmaven.test.skip=true
#cd ${ROOT_DIR}/src/sdk/java/fesql-auto-test-java
#mvn test -DsuiteXmlFile=test_suite/${case_xml}
