#! /bin/sh
#
# package.sh
#

VERSION=`date +"%Y-%m-%d"`-`git rev-parse --short HEAD`
WORKDIR=`pwd`
set -e
package=fedb-cluster-fpga-$VERSION
rm -rf ${package} || :
mkdir ${package} || :
cp -r release/conf ${package}/conf
cp -r release/bin ${package}/bin
sed -i 's/rtidb/fedb/g' ${package}/bin/boot.sh
sed -i 's/rtidb/fedb/g' ${package}/bin/start.sh
sed -i 's/rtidb/fedb/g' ${package}/bin/start_ns.sh
sed -i 's/rtidb/fedb/g' ${package}/bin/boot_ns.sh
IP=127.0.0.1
sed -i "s/--endpoint=.*/--endpoint=${IP}:6527/g" ${package}/conf/nameserver.flags
sed -i "s/--zk_cluster=.*/--zk_cluster=${IP}:2181/g" ${package}/conf/nameserver.flags
sed -i "s/--zk_root_path=.*/--zk_root_path=\/fedb/g" ${package}/conf/nameserver.flags

sed -i "s/--endpoint=.*/--endpoint=${IP}:9527/g" ${package}/conf/tablet.flags
sed -i "s/#--zk_cluster=.*/--zk_cluster=${IP}:2181/g" ${package}/conf/tablet.flags
sed -i "s/#--zk_root_path=.*/--zk_root_path=\/fedb/g" ${package}/conf/tablet.flags

cp -r tools ${package}/tools
rm -rf ${package}/tools/dataImporter || :
rm -rf ${package}/tools/rtidbCmdUtil || :
ls -l build/bin/
cp -r build/bin/rtidb_fpga ${package}/bin/fedb
cd ${package}/bin
wget http://pkg.4paradigm.com/rtidb/dev/node_exporter
wget http://pkg.4paradigm.com/rtidb/metricbeat
wget http://pkg.4paradigm.com/rtidb/filebeat
wget http://pkg.4paradigm.com/rtidb/dev/prometheus_client-0.6.0.tar.gz
chmod a+x node_exporter
chmod a+x metricbeat
chmod a+x filebeat
tar -xvzf prometheus_client-0.6.0.tar.gz
rm prometheus_client-0.6.0.tar.gz
cd ../..
tar -cvzf ${package}.tar.gz ${package}
echo "package at ./${package}"
URL="http://pkg.4paradigm.com:81/fedb/dailybuild/"
CHECKURL="http://pkg.4paradigm.com/fedb/dailybuild/"
FILE=${package}.tar.gz
sh -x steps/upload_to_pkg.sh $URL $FILE

