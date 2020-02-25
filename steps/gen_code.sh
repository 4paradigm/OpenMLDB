#! /bin/sh
PJ_ROOT=`pwd`
cd src/proto
$PJ_ROOT/thirdparty/bin/protoc --cpp_out . type.proto
$PJ_ROOT/thirdparty/bin/protoc --cpp_out . common.proto
$PJ_ROOT/thirdparty/bin/protoc --cpp_out . tablet.proto
$PJ_ROOT/thirdparty/bin/protoc --cpp_out . name_server.proto
$PJ_ROOT/thirdparty/bin/protoc --cpp_out . client.proto


