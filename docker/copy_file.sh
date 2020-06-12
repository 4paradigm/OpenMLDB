#! /bin/sh
set -e 
cp ./build/bin/rtidb ./docker/tablet/bin/
cp ./build/bin/rtidb ./docker/nameserver/bin/
cp ./build/bin/rtidb ./docker/blob_proxy/bin/
cp ./release/conf/tablet.flags ./docker/tablet/
cp ./release/conf/nameserver.flags ./docker/nameserver/
mkdir -p ./docker/tablet/lib
mkdir -p ./docker/nameserver/lib
mkdir -p ./docker/blob_proxy/lib
wget http://pkg.4paradigm.com:81/rtidb/libstdc++.so.6 >/dev/null
cp libstdc++.so.6 ./docker/tablet/lib/ 
cp libstdc++.so.6 ./docker/nameserver/lib/ 
cp libstdc++.so.6 ./docker/blob_proxy/lib/ 
