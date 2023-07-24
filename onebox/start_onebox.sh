#!/bin/bash

# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eE
set -x

cd "$(dirname "$0")/../"
BASE=$(pwd)

# allow producing core files
ulimit -c unlimited

OPENMLDB_BIN=${OPENMLDB_BIN:-build/bin/openmldb}
if [ ! -r "$OPENMLDB_BIN" ]; then
    echo "openmldb binary: $OPENMLDB_BIN not exist"
    exit 1
fi

# enable dynamic loading searches the 'udf' directory
mkdir -p "$(dirname "$OPENMLDB_BIN")/../udf"

# the subdirectory can be set through the environment variable ONEBOX_WORKDIR
WORKSPACE=${ONEBOX_WORKDIR:-onebox/workspace}
mkdir -p "$WORKSPACE"
WORKSPACE=$(cd "$WORKSPACE" && pwd)

IP=127.0.0.1

# Function: cluster_start_component
#   start a openmldb component for cluster
# @param $1 role, (tablet/nameserver)
# @param $2 endpoint
# @param $3 log directory
# @param $4 zk root endpoint
# @param $5 zk root path
# @param $6 binlog path (tablet only)
# @param $7 recycle bin path (tablet only)
cluster_start_component() {
    if [ $# -lt 5 ]; then
        echo -e "at least 5 param needed for $0
\tusage: $0 \$role \$endpoint \$log_dir \$zk_root \$zk_path [ \$binlog_dir ] [ \$recycle_bin_dir ]"
        return 2
    fi

    local role=$1
    local endpoint=$2
    local log_dir="$3"
    local zk_end=$4
    local zk_path=$5

    local binlog_dir="$6"
    local recycle_bin_dir="$7"

    mkdir -p "$log_dir"

    local extra_opts=(--enable_status_service=true)
    if [[ $role = 'tablet' ]]; then
        [ -d "$binlog_dir" ] && rm -r "$binlog_dir"
        mkdir -p "$binlog_dir"

        [ -d "$recycle_bin_dir" ] && rm -r "$recycle_bin_dir"
        mkdir -p "$recycle_bin_dir"

        extra_opts+=(
            --binlog_notify_on_put=true
            --zk_keep_alive_check_interval=60000
            --db_root_path="$binlog_dir"
            --recycle_bin_root_path="$recycle_bin_dir"
        )
    elif [[ $role = 'nameserver' ]]; then
        extra_opts+=(
            --tablet_offline_check_interval=1
            --tablet_heartbeat_timeout=1
        )
    else
        echo "unsupported role: $role"
        return 3
    fi

    # just need extra_opts to split
    LD_DEBUG=libs "$OPENMLDB_BIN" \
        --role="$role" \
        --endpoint="$endpoint" \
        --openmldb_log_dir="$log_dir" \
        --zk_cluster="$zk_end" \
        --zk_root_path="$zk_path" \
        "${extra_opts[@]}"
}

ZK_CLUSTER=$IP:6181

NS0=$IP:9622
NS1=$IP:9623
NS2=$IP:9624

TABLET0=$IP:9520
TABLET1=$IP:9521
TABLET2=$IP:9522

start_cluster() {
    # first start zookeeper

    cluster_start_component tablet "$TABLET0" "$WORKSPACE/logs/tablet0" "$ZK_CLUSTER" "/onebox" "$WORKSPACE/tablet0-binlogs" "$WORKSPACE/recycle_bin0" >"$WORKSPACE/tablet0.log" 2>&1 &
    sleep 2

    cluster_start_component tablet "$TABLET1" "$WORKSPACE/logs/tablet1" "$ZK_CLUSTER" "/onebox" "$WORKSPACE/tablet1-binlogs" "$WORKSPACE/recycle_bin1" >"$WORKSPACE/tablet1.log" 2>&1 &
    sleep 2

    cluster_start_component tablet "$TABLET2" "$WORKSPACE/logs/tablet2" "$ZK_CLUSTER" "/onebox" "$WORKSPACE/tablet2-binlogs" "$WORKSPACE/recycle_bin2" >"$WORKSPACE/tablet1.log" 2>&1 &
    sleep 2

    cluster_start_component nameserver "$NS0" "$WORKSPACE/logs/ns0" "$ZK_CLUSTER" "/onebox" >"$WORKSPACE/ns0.log" 2>&1 &
    sleep 2

    cluster_start_component nameserver "$NS1" "$WORKSPACE/logs/ns1" "$ZK_CLUSTER" "/onebox" >"$WORKSPACE/ns1.log" 2>&1 &
    sleep 2

    cluster_start_component nameserver "$NS2" "$WORKSPACE/logs/ns2" "$ZK_CLUSTER" "/onebox" >"$WORKSPACE/ns2.log" 2>&1 &
    sleep 2

    echo "cluster start ok"
}

start_taskmanager() {
    rm -rf onebox/taskmanager
    cp -r java/openmldb-taskmanager/target/openmldb-taskmanager-binary/ onebox/taskmanager
    pushd onebox/taskmanager/
    chmod +x bin/*.sh
    # NOTE: taskmanager find shared libraraies in "<cwd>/udf", where starts taskmanager matters
    mkdir -p udf/
    cp -v "$BASE/build/udf/"*.so udf/
    cp -v "$BASE/onebox/taskmanager.properties" conf/

    LD_DEBUG=libs ./bin/taskmanager.sh > "$WORKSPACE/logs/taskmanager.log" 2>&1 &
    popd
}

SA_NS=$IP:6527
SA_TABLET=$IP:9921
SA_BINLOG="$WORKSPACE/standalone/binlog"
SA_RECYCLE="$WORKSPACE/standalone/recycle_bin"

start_standalone() {

    # start tablet
    [ -d "$SA_BINLOG" ] && rm -r "$SA_BINLOG"
    [ -d "$SA_RECYCLE" ] && rm -r "$SA_RECYCLE"
    mkdir -p "$SA_BINLOG"
    mkdir -p "$SA_RECYCLE"
    mkdir -p "$WORKSPACE/logs/standalone-tb"
    mkdir -p "$WORKSPACE/logs/standalone-ns"

    LD_DEBUG=libs ./build/bin/openmldb --db_root_path="$SA_BINLOG" \
        --recycle_bin_root_path="$SA_RECYCLE" \
        --openmldb_log_dir="$WORKSPACE/logs/standalone-tb" \
        --endpoint="$SA_TABLET" --role=tablet \
        --binlog_notify_on_put=true >"$WORKSPACE/sa-tablet.log" 2>&1 &
    sleep 2

    # start ns
    LD_DEBUG=libs ./build/bin/openmldb --endpoint="$SA_NS" --role=nameserver \
        --tablet="$SA_TABLET" \
        --openmldb_log_dir="$WORKSPACE/logs/standalone-ns" \
        --tablet_offline_check_interval=1 --tablet_heartbeat_timeout=1 >"$WORKSPACE/sa-ns.log" 2>&1 &
    sleep 2
    echo "standalone start ok"
}

help() {
    echo -e "\tUsage: $0 [ cluster | standalone ]"
}

# with no param, default is cluster mode
if [ $# -eq 0 ]; then
    start_cluster
    exit 0
fi

if [ $# -ne 1 ]; then
    echo "parameter must be standalone|cluster"
    exit 1
fi

OP=$1
case $OP in

cluster)
    start_cluster
    ;;
standalone)
    start_standalone
    ;;
taskmanager)
    start_taskmanager
    ;;

-h | help)
    help
    ;;
*)
    echo -e "Invalid argument: $*"
    help
    exit 1
    ;;
esac
