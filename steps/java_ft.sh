ROOT_DIR=`pwd`
PROTO_BIN=$ROOT_DIR/thirdparty/bin/protoc
sed -i "/protocExecutable/c\<protocExecutable>${PROTO_BIN}<\/protocExecutable>" java/pom.xml
testpath=$(cd "$(dirname "$0")"; pwd)
projectpath=${testpath}/..
sh ${projectpath}/test-common/integrationtest/setup.sh
source ${projectpath}/test-common/integrationtest/env.conf
python ${projectpath}/test-common/integrationtest/setup.py
cd ${projectpath}/java && mvn test -Dtest=com._4paradigm.rtidb.client.functiontest.cases.*Test
python ${testpath}/setup.py -T=1 -C=1 
