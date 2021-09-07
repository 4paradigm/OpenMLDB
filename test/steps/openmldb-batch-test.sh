
ROOT_DIR=`pwd`

source steps/read_properties.sh
sh steps/download-case.sh ${CASE_BRANCH}
cd ${ROOT_DIR}/java/hybridsql-test/
mvn clean install -Dmaven.test.skip=true
cd ${ROOT_DIR}/java/hybridsql-test/sparkfe_test/
mvn clean test