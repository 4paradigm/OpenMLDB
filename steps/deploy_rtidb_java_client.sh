#! /bin/sh
#
# deploy_rtidb_java_client.sh
#

set -x
VERSION=$1
sh steps/release.sh ${VERSION}
sed -i "6c \ \ \ \ \ \ \ <version>$VERSION-RELEASE</version>" java/pom.xml
PROTOC=`which protoc`
#sed -i "<protocExecutable>${PROTOC}</protocExecutable>" java/pom.xml
sed -i "s#<protocExecutable>.*</protocExecutable>#<protocExecutable>${PROTOC}</protocExecutable>#" java/pom.xml
cd java
mvn deploy -Dmaven.test.skip=true
cd -
