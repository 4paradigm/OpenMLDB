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

set -e

ulimit -c unlimited
ulimit -n 655360
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:$(pwd)/udf"
export LD_LIBRARY_PATH

export COMPONENTS="tablet tablet2 nameserver apiserver taskmanager standalone_tablet standalone_nameserver standalone_apiserver"

if [ $# -lt 2 ]; then
  echo "Usage: start.sh start/stop/restart <component>"
  echo "component: $COMPONENTS"
  exit 1
fi

CURDIR=$(pwd)
cd "$(dirname "$0")"/../ || exit 1
RED='\E[1;31m'
RES='\E[0m'

OP=$1
COMPONENT=$2
HAS_COMPONENT="false"
for ITEM in $COMPONENTS;
do
  if [ "$COMPONENT" = "$ITEM" ]; then
    HAS_COMPONENT="true"
  fi
done

if [ "$HAS_COMPONENT" = "false" ]; then
    echo "No component named $COMPONENT in [$COMPONENTS]";
    exit 1;
fi

OPENMLDB_PID_FILE="./bin/$COMPONENT.pid"
mkdir -p "$(dirname "$OPENMLDB_PID_FILE")"

if [ "$COMPONENT" != "taskmanager" ]; then
    LOG_DIR=$(grep log_dir ./conf/"$COMPONENT".flags | awk -F '=' '{print $2}')
else
    LOG_DIR=$(grep job.log.path ./conf/"$COMPONENT".properties | awk -F '=' '{print $2}')
fi

[ -n "$LOG_DIR" ] || { echo "Invalid log dir"; exit 1; }
mkdir -p "$LOG_DIR"
case $OP in
    start)
        echo "Starting $COMPONENT ... "
        if [ -f "$OPENMLDB_PID_FILE" ]; then
            PID=$(tr -d '\0' < "$OPENMLDB_PID_FILE")
            if kill -0 "$PID" > /dev/null 2>&1; then
                echo -e "${RED}$COMPONENT already running as process $PID ${RES}"
                exit 0
            fi
        fi

        if [ "$COMPONENT" != "taskmanager" ]; then
            ./bin/openmldb --flagfile=./conf/"$COMPONENT".flags --enable_status_service=true >>  "$LOG_DIR"."$COMPONENT".log 2>&1 &
            PID=$!
            sleep 3
            ENDPOINT=$(grep '\--endpoint' ./conf/"$COMPONENT".flags | awk -F '=' '{print $2}')
            COUNT=1
            while [ $COUNT -lt 12 ]
            do
                if ! curl "http://$ENDPOINT/status" > /dev/null 2>&1; then
                    sleep 1
                    (( COUNT+=1 ))
                elif kill -0 "$PID" > /dev/null 2>&1; then
                    echo $PID > "$OPENMLDB_PID_FILE"
                    echo "Start ${COMPONENT} success"
                    exit 0
                else
                    break
                fi
            done
        else
            if [ -f "./conf/taskmanager.properties" ]; then
                cp ./conf/taskmanager.properties ./taskmanager/conf/taskmanager.properties
            fi
            pushd ./taskmanager/bin/ > /dev/null
            mkdir -p logs
            sh ./taskmanager.sh > logs/taskmanager.out 2>&1 &
            PID=$!
            popd > /dev/null 
            sleep 10
            if kill -0 $PID > /dev/null 2>&1; then
                /bin/echo $PID > "$OPENMLDB_PID_FILE"
                echo "Start ${COMPONENT} success"
                exit 0
            fi
        fi
        echo -e "${RED}Start ${COMPONENT} failed!${RES}"
        ;;
    stop)
        echo "Stopping $COMPONENT ... "
        if [ ! -f "$OPENMLDB_PID_FILE" ]
        then
             echo "no $COMPONENT to stop (could not find file $OPENMLDB_PID_FILE)"
        else
            PID="$(tr -d '\0' < "$OPENMLDB_PID_FILE")"
            if kill -0 "$PID" > /dev/null 2>&1; then
                kill "$PID"
            fi
            rm "$OPENMLDB_PID_FILE"
            echo "Stop ${COMPONENT} success"
        fi
        ;;
    restart)
        shift
        cd "$CURDIR" || exit 1
        sh "$0" stop "${@}"
        sleep 15
        sh "$0" start "${@}"
        ;;
    *)
        echo "Only support {start|stop|restart}" >&2
esac
