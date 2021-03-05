#!/usr/bin/env bash
# Usage: ./benchspark spark-submit [ARGS]
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


CURDIR=$(cd $(dirname $0); pwd)
echo "Spark submit process: $@"

# use pipe to read spark-submit output
tmp="/tmp/.fd_$(date +%Y%m%d%H%M%S)"
mkfifo ${tmp}
exec 9<>${tmp}

# show some environments
SPARK_HOME=${SPARK_HOME:-}
HADOOP_BIN_PATH=${HADOOP_BIN_PATH:-}
HADOOP_USER_NAME=${HADOOP_USER_NAME:-work}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-}
YARN_CONF_DIR=${YARN_CONF_DIR:-}

echo "Specified hadoop bin path: ${HADOOP_BIN_PATH}"
echo "Will find yarn command from: ${HADOOP_BIN_PATH}"
echo "Will use hadoop user: ${HADOOP_USER_NAME:-${USER}}"
echo "Specified hadoop conf: ${HADOOP_CONF_DIR}"
echo "Specified yarn conf: ${YARN_CONF_DIR}"
echo "Specified spark home: ${SPARK_HOME}"
echo "Will use spark-submit from: $(which spark-submit)"

# show current spark-submit or exit
spark-submit --version || exit 1
yarn version || exit 1

# fast path for local spark
echo "$@" | grep -q "\-\-master local"
if [[ "$?" -eq 0 ]]; then
    time $@; exit $?
fi

# launch spark-submit
$@ 2>&1 | tee ${tmp} &
sleep 3
spark_child=$(ps -ef | grep $$ | grep SparkSubmit | grep -v grep | awk '{print $2}')


# kill spark-submit if current script killed
_handler() {
  echo "Kill spark-submit process: ${spark_child}"
  kill -TERM ${spark_child}
  rm -f ${tmp}
  exit 1
}
trap _handler SIGTERM
trap _handler SIGINT
trap _handler SIGHUP


APP_ID=""
while read line; do
{
    if [[ "${APP_ID}" == "" ]]; then
        APP_ID=$(echo ${line} | sed -n "s/.* Application report for \([a-zA-Z0-9_]*\).*/\1/p")
        if [[ "${APP_ID}" != "" ]]; then
            echo "Get application id: ${APP_ID}"
        fi
    fi

    ps -ef | awk '{print $2}' | grep -v grep | grep -q ${spark_child}
    if [[ "$?" -ne 0 ]]; then
        echo "Spark application exits"
        rm -f ${tmp}

        if [[ "${APP_ID}" == "" ]]; then
            echo "Fail to get application id"
            exit 1
        fi

        is_success="no"
        for ((i=0; i<3; ++i)); do
            # try multiple times to get status
            if [[ "$is_success" != "0" ]]; then
                status=$(${HADOOP_BIN_PATH}/yarn application -status ${APP_ID})
                echo -ne ${status}
                is_success=$(echo ${status} | grep -q "Final-State : SUCCEEDED"; echo $?)
            fi
            sleep 10
        done

        start_time=$(echo ${status} | sed -n "s/.* Start-Time : \([0-9_]*\).*/\1/p")
        end_time=$(echo ${status} | sed -n "s/.* Finish-Time : \([0-9_]*\).*/\1/p")
        total_resource=$(echo ${status} | \
            sed -n "s/.* Aggregate Resource Allocation : \([a-zA-Z0-9_, -]* vcore-seconds\).*/\1/p")

        echo -e "\n"
        echo "----------------------------------------------------"
        if [[ "$is_success" == "0" ]]; then
            echo "YARN Application start at: ${start_time}"
            echo "YARN Application time: $(expr $(expr ${end_time} - ${start_time}) / 1000) seconds"
            echo "YARN Application resource: $total_resource"
            exit 0
        else
            echo "YARN Application not success: $APP_ID"
            exit 1
        fi
    fi
}
done <&9
wait ${spark_child}
rm -f ${tmp}

