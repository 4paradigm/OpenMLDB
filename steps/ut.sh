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
WORK_DIR=`pwd`
ulimit -c unlimited
test -d reports && rm -rf reports
mkdir -p reports
cp steps/zoo.cfg thirdsrc/zookeeper-3.4.14/conf
cd thirdsrc/zookeeper-3.4.14 && ./bin/zkServer.sh start && cd $WORK_DIR
sleep 5
TMPFILE="code.tmp"
echo 0 > $TMPFILE
ls  build/bin/ | grep test | grep -v "sql_sdk_test\|sql_cluster_test"| grep -v grep | while read line
do 
    ./build/bin/$line --gtest_output=xml:./reports/$line.xml 2>/tmp/${line}.${USER}.log 1>&2
    RET=$?
    echo "$line result code is: $RET"
    if [ $RET -ne 0 ];then 
        cat /tmp/${line}.${USER}.log
        echo $RET > $TMPFILE
    else
        rm -f /tmp/${line}.${USER}.log
    fi 
done
code=`cat $TMPFILE`
echo "code result: $code"
rm $TMPFILE
cd thirdsrc/zookeeper-3.4.14 && ./bin/zkServer.sh stop
cd -
exit $code
