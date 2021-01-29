#! /bin/sh
#
# ut.sh
#


CASE_LEVEL=$1
if [[ "${CASE_LEVEL}" == "" ]]; then
        CASE_LEVEL="0"
fi
echo "fesql c++ sdk test : case_level ${CASE_LEVEL}"

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
ls  build/bin/ | grep test | grep "sql_sdk_test\|sql_cluster_test\|tablet_engine_test" | grep -v grep | while read line
do
    FESQL_LEVEL=${CASE_LEVEL} ./build/bin/$line --gtest_output=xml:./reports/$line.xml
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
