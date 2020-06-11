#!/bin/bash
# Create java artifact tar including:
# fesql-proto, fesql-native
ROOT_DIR=$(cd $(dirname $0); pwd)/../..
SUFFIX=$1
cd ${ROOT_DIR}; tar cfz java_intermediate_cicd_artifact_${SUFFIX}.tar.gz \
	./java/fesql-native ./java/fesql-proto
