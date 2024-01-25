#!/bin/bash
while getopts ":c:d:l:s:j:m:" opt
do
   case $opt in
        c)
        echo "参数c的值:$OPTARG"
        CASE_XML=$OPTARG
        ;;
        d)
        echo "参数d的值:$OPTARG"
        DEPLOY_MODE=$OPTARG
        ;;
        l) echo "参数l的值:$OPTARG"
        CASE_LEVEL=$OPTARG
        ;;
        s) echo "参数s的值:$OPTARG"
        TABLE_STORAGE_MODE=$OPTARG
        ;;
        j) echo "参数j的值:$OPTARG"
        JAR_VERSION=$OPTARG
        ;;
        m) echo "参数m的值:$OPTARG"
        EXECUTE_MODE=$OPTARG
        ;;
        ?) echo "未知参数"
           exit 1
        ;;
   esac
done
if [[ "${CASE_XML}" == "" ]]; then
    CASE_XML="test_all.xml"
fi
if [[ "${DEPLOY_MODE}" == "" ]]; then
    DEPLOY_MODE="cluster"
fi
if [[ "${CASE_LEVEL}" == "" ]]; then
    CASE_LEVEL="0"
fi
if [[ "${EXECUTE_MODE}" == "" ]]; then
    EXECUTE_MODE="javasdk"
fi

JAVA_SDK_VERSION=$(more java/pom.xml | grep "<version>.*</version>" | head -1 | sed 's#.*<version>\(.*\)</version>.*#\1#')
sh test/steps/modify_java_sdk_config.sh "${CASE_XML}" "${DEPLOY_MODE}" "${JAR_VERSION}" "" "${JAR_VERSION}" "${JAR_VERSION}" "${TABLE_STORAGE_MODE}"
mkdir -p ../mvnrepo
export MAVEN_OPTS="-Dmaven.repo.local=$(pwd)/../mvnrepo"
mvn install:install-file -Dfile=openmldb-batch.jar -DartifactId=openmldb-batch -DgroupId=com.4paradigm.openmldb -Dversion="${JAR_VERSION}" -Dpackaging=jar
mvn install:install-file -Dfile=openmldb-jdbc.jar -DartifactId=openmldb-jdbc -DgroupId=com.4paradigm.openmldb -Dversion="${JAR_VERSION}" -Dpackaging=jar
mvn install:install-file -Dfile=openmldb-native.jar -DartifactId=openmldb-native -DgroupId=com.4paradigm.openmldb -Dversion="${JAR_VERSION}" -Dpackaging=jar
mvn install:install-file -Dfile=openmldb-spark-connector.jar -DartifactId=openmldb-spark-connector -DgroupId=com.4paradigm.openmldb -Dversion="${JAR_VERSION}" -Dpackaging=jar

mvn clean install -B -Dmaven.test.skip=true -f test/test-tool/command-tool/pom.xml
mvn clean install -B -Dmaven.test.skip=true -f test/integration-test/openmldb-test-java/pom.xml -Dopenmldb.native.version="${JAR_VERSION}" -Dopenmldb.jdbc.version="${JAR_VERSION}" -Dopenmldb.batch.version="${JAR_VERSION}"
if [[ "${EXECUTE_MODE}" == "javasdk" ]]; then
  mvn clean test -B -e -U -DsuiteXmlFile=test_suite/"${CASE_XML}" -f test/integration-test/openmldb-test-java/openmldb-sdk-test/pom.xml -DcaseLevel="${CASE_LEVEL}" -Dopenmldb.native.version="${JAR_VERSION}" -Dopenmldb.jdbc.version="${JAR_VERSION}" -Dopenmldb.batch.version="${JAR_VERSION}"
elif [[ "${EXECUTE_MODE}" == "apiserver" ]]; then
  mvn clean test -B -e -U -DsuiteXmlFile=test_suite/"${CASE_XML}" -f test/integration-test/openmldb-test-java/openmldb-http-test/pom.xml -DcaseLevel="${CASE_LEVEL}" -Dopenmldb.native.version="${JAR_VERSION}" -Dopenmldb.jdbc.version="${JAR_VERSION}" -Dopenmldb.batch.version="${JAR_VERSION}"
fi
