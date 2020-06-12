#! /bin/sh
#
# package.sh
#
if [ $# != 1 ] || [[ ! ($1 =~ ^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,2}$) ]]; then
    echo "format error e.g. sh steps/package.sh 1.4.2.2"
    exit 1;
fi
sh ./steps/release.sh $1
ln -sf /usr/workdir/thirdparty thirdparty 
ln -sf /usr/workdir/thirdsrc thirdsrc
sed -i /[:blank:]*version/s/1.0/$1/ python/setup.py
sh ./steps/compile.sh
package=rtidb-cluster-$1
rm -rf ${package}
mkdir ${package}
cp -r release/conf ${package}/conf
cp -r release/bin ${package}/bin
cp -r tools ${package}/tools
rm -rf ${package}/tools/dataImporter
rm -rf ${package}/tools/rtidbCmdUtil
cp -r build/bin/rtidb ${package}/bin/rtidb
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
