#! /bin/sh
#
# compile2.sh

WORK_DIR=`pwd`
if [ "$1" = "DEBUG" ]
then
    sed -i '/set(CMAKE_BUILD_TYPE/c\set(CMAKE_BUILD_TYPE Debug)' CMakeLists.txt 
else
    sed -i '/set(CMAKE_BUILD_TYPE/c\set(CMAKE_BUILD_TYPE RelWithDebInfo)' CMakeLists.txt 
fi
THIRDPARY=$WORK_DIR/thirdparty
export PATH=${THIRDPARY}/bin:$PATH
export JAVA_HOME=${THIRDPARY}/jdk1.8.0_141
export PATH=$JAVA_HOME/bin:$PATH
export MAVEN_HOME=${THIRDPARY}/apache-maven-3.6.3
export PATH=$MAVEN_HOME/bin:$PATH
sh steps/gen_code.sh
mkdir -p $WORK_DIR/build 
cd $WORK_DIR/build && cmake .. && make rtidb
code=$?
cd $WORK_DIR
exit $code
