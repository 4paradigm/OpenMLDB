#! /bin/sh
#
# compile_in_docker.sh
rm thirdparty
ln -sf /depends/thirdparty thirdparty
source /opt/rh/devtoolset-8/enable && bash tools/compile_and_test.sh

