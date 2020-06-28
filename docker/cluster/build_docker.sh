#! /bin/sh

wget http://pkg.4paradigm.com/rtidb/dev/zookeeper-3.4.14.tar.gz
tar -zxvf zookeeper-3.4.14.tar.gz

cp zookeeper-3.4.14/conf/zoo_sample.cfg zookeeper-3.4.14/conf/zoo.cfg

wget http://pkg.4paradigm.com/rtidb/dev/jdk-8u121-linux-x64.tar.gz
tar -zxvf jdk-8u121-linux-x64.tar.gz

cp -r ../../release/conf ./
cp -r ../../release/bin ./
cp ../../build/bin/rtidb bin/

docker build -t develop-registry.4pd.io/fedb:0.0.0 .
docker push develop-registry.4pd.io/fedb:0.0.0
