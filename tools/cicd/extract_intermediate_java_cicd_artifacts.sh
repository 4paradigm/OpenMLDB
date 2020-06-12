#!/bin/bash
ROOT_DIR=$(cd $(dirname $0); pwd)/../..
SUFFIX=$1
TAR_NAME="java_intermediate_cicd_artifact_${SUFFIX}.tar.gz"

# extract into java/fesql-proto java/fesql-native
cd ${ROOT_DIR}; tar xf ${TAR_NAME}