#! /bin/sh
#
# get_deps.sh
if [ $# -eq 0 ];
then
    echo "please input openmldb version. ex: sh get_deps.sh 0.2.0"
    exit
fi
OPENMLDB_V=$1

wget https://github.com/4paradigm/OpenMLDB/releases/download/v${OPENMLDB_V}/openmldb-${OPENMLDB_V}-linux.tar.gz

wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz

wget -c --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.tar.gz


sed -i "s/openmldb-.*-linux/openmldb-$OPENMLDB_V-linux/g" Dockerfile
sed -i "s/spark-.*-bin/spark-$OPENMLDB_V-bin/g" Dockerfile

