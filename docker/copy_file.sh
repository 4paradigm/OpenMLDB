#! /bin/sh
set -e 
mkdir -p ./docker/{bin,lib} || :
cp ./build/bin/rtidb ./docker/bin/
wget http://pkg.4paradigm.com:81/rtidb/libstdc++.so.6 >/dev/null
cp libstdc++.so.6 ./docker/lib/ 
