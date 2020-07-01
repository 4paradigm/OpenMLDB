#! /bin/sh
#
# deploy_package.sh
#
checkFile() {
    fileSize=`ls -l $2  | awk '{print $5}'`
    result=`curl -I $1$2`
    code=`echo $result | awk '{print $2}'`
    if [ $code -ne 200 ]; then
        return 1
    fi
    size=`echo $result | awk -F '\r' '{print $5}' | awk -F ':' '{print $2}' | awk '$1=$1'`
    if [ $fileSize -eq $size ]; then
        return 0
    else
        return 1
    fi
}

checkFileExist() {
    result=`curl -I $1$2`
    code=`echo $result | awk '{print $2}'`
    if [ $code -eq 404 ]; then
        return 0
    else
        return 1
    fi
}

VERSION=`git describe --always --tag`
VERSION=${VERSION:1}
if [[ ! ($VERSION =~ ^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}$) ]]; then
    echo "$VERSION is not release version"
    exit 0
fi

sh -x steps/package.sh $VERSION || exit 1
sh -x steps/package_whl.sh

URL="http://pkg.4paradigm.com:81/rtidb/"
CHECKURL="http://pkg.4paradigm.com/rtidb/"
FILE=rtidb-cluster-$VERSION.tar.gz
checkFileExist $CHECKURL $FILE
if [ $? -ne 0 ]; then
    echo "package has already exist"
    exit 1
fi
sh -x steps/upload_to_pkg.sh $URL $FILE
checkFile $CHECKURL $FILE
if [ $? -ne 0 ]; then
    echo "upload package failed"
    exit 1
fi

# release java client
sed -i "6c \ \ \ \ \ \ \ <version>$VERSION-RELEASE</version>" java/pom.xml
cd java
#mvn deploy -Dmaven.test.skip=true
cd -
