#! /bin/sh
#
# build.sh

docker build -f Dockerfile_llvm -t develop-registry.4pd.io/fesql_build_base_llvm:1.0 .
