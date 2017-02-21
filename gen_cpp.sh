#! /bin/sh
PJ_ROOT=`pwd`
cd src/proto
$PJ_ROOT/thirdparty/bin/protoc -I. --cpp_out . rtidb_tablet_server.proto


