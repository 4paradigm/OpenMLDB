#! /bin/sh
#
# package.sh
#

WORKDIR=`pwd`
VERSION=`git describe --always --tag`
VERSION=${VERSION:1}
if [[ ! ($VERSION =~ ^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}$) ]]; then
    echo "$VERSION is not release version"
    exit 0
fi
set -e
package=rtdb-cluster-rdma-$VERSION
rm -rf ${package} || :
mkdir ${package} || :
cp -r release/conf ${package}/conf
cp -r release/bin ${package}/bin

cp -r tools ${package}/tools
rm -rf ${package}/tools/dataImporter || :
rm -rf ${package}/tools/rtidbCmdUtil || :
ls -l build/bin/
cp -r build/bin/rtidb_rdma ${package}/bin/rtidb
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
URL="http://pkg.4paradigm.com:81/rtidb/"
CHECKURL="http://pkg.4paradigm.com/rtidb/"
FILE=${package}.tar.gz
sh -x steps/upload_to_pkg.sh $URL $FILE

