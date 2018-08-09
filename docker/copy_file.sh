#! /bin/sh
cp ./build/bin/rtidb ./docker/tablet/bin/
cp ./build/bin/rtidb ./docker/nameserver/bin/
cp ./release/conf/tablet.flags ./docker/tablet/
cp ./release/conf/nameserver.flags ./docker/nameserver/
