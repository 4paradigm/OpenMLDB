#! /bin/sh
PJ_ROOT=`pwd`
cd src/proto
$PJ_ROOT/thirdparty/bin/protoc -I$PJ_ROOT/thirdsrc/protobuf-2.6.1/src/ -I. --cpp_out . tablet.proto


