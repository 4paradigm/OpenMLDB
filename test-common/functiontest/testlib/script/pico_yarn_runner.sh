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

if [ "$1" = "--enable-hdfs-proxy" ]; then

shift

echo "starting hdfs-proxy server" >&2

export PATH="${HADOOP_HOME}/bin:$PATH"

HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS} -Xmx4096m" hadoop jar hdfs-proxy.jar com._4paradigm.hdfsproxy.HdfsProxyServer &

hdfsproxy_pid=$!

trap "kill -15 ${hdfsproxy_pid}" INT TERM EXIT

while [ ! -f ./hdfs_proxy_server.rc ]; do
    sleep 1
done
sleep 1

eval `cat ./hdfs_proxy_server.rc | awk -F"=" '{
    if ($1 == "WEBSERVICE_URI_HOST") {
        print "export WEBHDFS_HOST="$2;
    } else if ($1 == "WEBSERVICE_URI_PORT") {
        print "export WEBHDFS_PORT="$2;
    }
}'`

if [ "${WEBHDFS_HOST}" = "" -o "${WEBHDFS_PORT}" = "" ]; then
    echo -e "get HdfsProxy server info failed" >&2;
    exit -1
fi

echo "hdfs-proxy server is running at ${WEBHDFS_HOST}:${WEBHDFS_PORT}" >&2

fi

./yarn_wrapper "$1" --config_file="$2"

exit $?
