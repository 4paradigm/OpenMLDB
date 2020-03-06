#! /bin/sh
#
# build.sh

docker build -f Dockerfile_arrow -t develop-registry.4pd.io/fesql_build_base:1.0 .
