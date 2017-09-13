#! /bin/sh
#
# ut.sh
#
WORK_DIR=`pwd`
cp steps/zoo.cfg thirdsrc/zookeeper-3.4.10/conf
cd thirdsrc/zookeeper-3.4.10/bin && ./zkServer.sh start && cd $WORK_DIR
ls  build/bin/ | grep test | grep -v grep | while read line; do ./build/bin/$line; done
cd thirdsrc/zookeeper-3.4.10/bin && ./zkServer.sh stop


