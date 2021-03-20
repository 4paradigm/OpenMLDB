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
# ut.sh
#


CASE_LEVEL=$1
CASE_NAME=$2
if [[ "${CASE_LEVEL}" == "" ]]; then
        CASE_LEVEL="0"
fi
if [[ "${CASE_NAME}" == "" ]]; then
        CASE_NAME="sql_sdk_test\|sql_cluster_test"
fi
echo "sql c++ sdk test : case_level ${CASE_LEVEL}, case_file ${CASE_NAME}"

ROOT_DIR=`pwd`
echo "WORK_DIR: ${ROOT_DIR}"
ulimit -c unlimited
test -d reports && rm -rf reports
mkdir -p reports
cd ${ROOT_DIR}
cp ${ROOT_DIR}/steps/zoo.cfg ${ROOT_DIR}/thirdsrc/zookeeper-3.4.14/conf/zoo.cfg
cd thirdsrc/zookeeper-3.4.14 && ./bin/zkServer.sh start && cd ${ROOT_DIR}
sleep 5
TMPFILE="code.tmp"
echo 0 > $TMPFILE
ls  build/bin/ | grep test | grep ${CASE_NAME} | grep -v grep | while read line
do
    GLOG_minloglevel=2 HYBRIDSE_LEVEL=${CASE_LEVEL} ./build/bin/$line --gtest_output=xml:./reports/$line.xml
    RET=$?
    echo "$line result code is: $RET"
    if [ $RET -ne 0 ];then
        echo $RET > $TMPFILE
    fi
done
code=`cat $TMPFILE`
echo "code result: $code"
rm $TMPFILE
cd $ROOT_DIR/thirdsrc/zookeeper-3.4.14 && ./bin/zkServer.sh stop
cd -
exit $code
