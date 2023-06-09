#!/bin/bash
set +x
# docker run --network=host --name build_docker_os6 -it -v`pwd`:/root/mnt ghcr.io/4paradigm/centos6_gcc7_hybridsql bash

# self build or workflow
IN_WORKFLOW=${IN_WORKFLOW:-"false"}
# if download from openmldb.ai
OPENMLDB_SOURCE=${OPENMLDB_SOURCE:-"false"}

function tool_install() {
    echo "tools install"
    yum install -y bison bison-devel byacc cppunit-devel patch devtoolset-8-gcc devtoolset-8-gcc-c++
    echo "ID=centos" > /etc/os-release
    if [ "$OPENMLDB_SOURCE" == "true" ]; then
        echo "download bazel from openmldb.ai"
        curl -SLo bazel https://openmldb.ai/download/legacy/bazel-1.0.0
    else
        echo "download bazel from github sub-mod"
        curl -SLo bazel https://github.com/sub-mod/bazel-builds/releases/download/1.0.0/bazel-1.0.0
    fi
    chmod +x bazel
}

if [ "$IN_WORKFLOW" == "true" ]; then
    echo "in workflow"
else
    echo "in self build"
fi

tool_install

echo "set envs, if IN_WORKFLOW, you should set envs in workflow"
export PATH=$PATH:`pwd`
source /opt/rh/devtoolset-8/enable

echo "add patch in fetch cmake"
# skip -lrt in rocksdb
sed -i'' '34s/$/ -DWITH_CORE_TOOLS=OFF/' third-party/cmake/FetchRocksDB.cmake

# If BUILD_BUNDLED=OFF will download pre-built thirdparty, not good. So we use cmake to build zetasql only
echo  "modify in .deps needs a make first, download&build zetasql first(build will fail)"
# sed -i'' '31s/${BUILD_BUNDLED}/ON/' third-party/CMakeLists.txt
cmake -S third-party -B `pwd`/.deps -DSRC_INSTALL_DIR=`pwd`/thirdsrc -DDEPS_INSTALL_DIR=`pwd`/.deps/usr -DBUILD_BUNDLED=ON
cmake --build `pwd`/.deps --target zetasql
echo "add patch in .deps zetasql"
sed -i'' "26s/lm'/lm:-lrt'/" .deps/build/src/zetasql/build_zetasql_parser.sh
# skip more target to avoid adding -lrt
sed -i'' '42s/^/#/' .deps/build/src/zetasql/build_zetasql_parser.sh
sed -i'' '6a function realpath () { \n[[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"\n}' .deps/build/src/zetasql/pack_zetasql.sh
if [ "OPENMLDB_SOURCE" = "true" ]; then
    echo "add patch, use openmldb.ai download icu4c required by zetasql"
    sed -i'' '911s#],#,"https://openmldb.ai/download/legacy/icu4c-65_1-src.tgz"],#' .deps/build/src/zetasql/bazel/zetasql_deps_step_2.bzl
fi

# python wheel will be installed in sdk make

if [ "$IN_WORKFLOW" == "false" ]; then
    echo "build and install, you can set env, e.g. SQL_JAVASDK_ENABLE=ON SQL_PYSDK_ENABLE=ON NRPOC=8"
    make BUILD_BUNDLED=ON 
    make install
fi
