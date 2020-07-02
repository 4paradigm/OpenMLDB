#! /bin/sh

ROOT_DIR=`pwd`

PROTO_BIN=$ROOT_DIR/thirdparty/bin/protoc
ulimit -c unlimited
sed -i "/protocExecutable/c\<protocExecutable>${PROTO_BIN}<\/protocExecutable>" java/pom.xml
mkdir -p java/src/main/proto/
cp -rf src/proto/tablet.proto java/src/main/proto/
cp -rf src/proto/name_server.proto java/src/main/proto/
cp -rf src/proto/common.proto java/src/main/proto/

ls -al build/bin
rtidb_path=$ROOT_DIR/build/bin/rtidb
echo "rtidb_path:$rtidb_path"
source steps/read_properties.sh

echo "java_client_version:${java_client_version}"
echo "test_case_xml:${test_case_xml}"
echo "server_env:${server_env}"
echo "upgrade_version:${upgrade_version}"
echo "rtidb_auto_test_branch:${rtidb_auto_test_branch}"

if [ ! -z ${java_client_version} ] ; then
	rtidb_version=${java_client_version}
else
    cd $ROOT_DIR/java
    rtidb_version=`cat pom.xml| grep version | head -n 1 | sed -n 's/<version>\(.*\)<\/version>/\1/p'`
    mvn clean install -Dmaven.test.skip=true
fi
echo "rtidb_version:$rtidb_version"

cd $ROOT_DIR
rm -rf auto-test-rtidb
echo "AAAAAA"
ls -al
git submodule add https://gitlab.4pd.io/FeatureEngineering/rtidb-auto-test-java.git auto-test-rtidb
cd auto-test-rtidb
git checkout ${rtidb_auto_test_branch}
git pull

#bash run-compatibility.sh -c test_1500.xml -j 1.5.0.0-RELEASE -s 1500 -u 1510 -r /home/rtidb/rtidb
#-c 执行的suite_xml,决定了跑哪些case 默认为test_1500.xml
#-j java_client的版本号 默认为1.5.0.0-RELEASE
#-r 编译后的rtidb路径，无默认值
#-s 服务端的环境，默认为1500，为1.5.0.0版本
#-u 升级到的版本，无默认值，进行升级测试时必须传此参数

parameters="-c ${test_case_xml} -j $rtidb_version -s ${server_env}"

if [ ! -z ${override_rtidb} ] ; then
    parameters=$parameters" -r $rtidb_path"
fi

if [ ! -z ${upgrade_version} ] ; then
	parameters=$parameters" -u ${upgrade_version}"
fi
echo "parameters:$parameters"

sh run-compatibility.sh $parameters

code=$?
exit $code
