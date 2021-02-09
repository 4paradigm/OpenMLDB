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
ls  build/bin/ | grep test | grep -v "sql_sdk_test\|sql_cluster_test\|tablet_engine_test"| grep -v grep | while read line
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
