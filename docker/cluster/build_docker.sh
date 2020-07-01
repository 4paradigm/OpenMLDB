#! /bin/sh

wget http://pkg.4paradigm.com/rtidb/dev/zookeeper-3.4.14.tar.gz
tar -zxvf zookeeper-3.4.14.tar.gz

cp zookeeper-3.4.14/conf/zoo_sample.cfg zookeeper-3.4.14/conf/zoo.cfg

wget http://pkg.4paradigm.com/rtidb/dev/jdk-8u121-linux-x64.tar.gz
tar -zxvf jdk-8u121-linux-x64.tar.gz

wget http://pkg.4paradigm.com/fedb/spark/spark-2.3.4-bin-hadoop2.7.tar.gz
tar -zxvf spark-2.3.4-bin-hadoop2.7.tar.gz

cp -r ../../release/conf ./
cp -r ../../release/bin ./
cp ../../build/bin/rtidb bin/fedb
cp -r ../../build/sql_pysdk sql_pysdk
cp -r ../../fesql/build/python fe_python
sed -i 's/rtidb/fedb/g' bin/boot.sh
sed -i 's/rtidb/fedb/g' bin/boot_ns.sh

docker build -t develop-registry.4pd.io/fedb:2.0.0 .

