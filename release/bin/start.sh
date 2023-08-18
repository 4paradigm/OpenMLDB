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

export COMPONENTS="tablet tablet2 nameserver apiserver taskmanager standalone_tablet standalone_nameserver standalone_apiserver data_collector synctool"

if [ $# -lt 2 ]; then
    echo "Usage: start.sh start/stop/restart <component> [mon]"
    echo "component: $COMPONENTS"
    echo "start flag 'mon': if use mon flag, we'll use mon to start the component"
    echo "    stop will kill both the mon and component automatically, don't need to use mon"
    exit 1
fi

# cd rootdir
cd "$(dirname "$0")"/../ || exit 1
ROOTDIR=$(pwd)
BINDIR=$(pwd)/bin # the script dir
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
    echo "No component named $COMPONENT in [$COMPONENTS]"
    exit 1
fi

DAEMON_MODE="false"
if [ $# -gt 2 ] && [ "$3" = "mon" ]; then
    DAEMON_MODE="true"
fi

if [[ $OSTYPE == 'darwin'* ]]; then
  MON_BINARY='./bin/mon_mac'
else
  MON_BINARY='./bin/mon'
fi

OPENMLDB_PID_FILE="./bin/$COMPONENT.pid"
mkdir -p "$(dirname "$OPENMLDB_PID_FILE")"

if [ "$COMPONENT" == "taskmanager" ]; then
    LOG_DIR=$(grep job.log.path ./conf/"$COMPONENT".properties | awk -F '=' '{print $2}')
elif [ "$COMPONENT" == "synctool" ] || [ "$COMPONENT" == "data_collector" ]; then
    # use bootstrap dir
    LOG_DIR="${ROOTDIR}/logs"
else
    LOG_DIR=$(grep log_dir ./conf/"$COMPONENT".flags | awk -F '=' '{print $2}')
fi

[ -n "$LOG_DIR" ] || { echo "Invalid log dir"; exit 1; }
mkdir -p "$LOG_DIR"
case $OP in
    start)
        echo "Starting $COMPONENT ... "
        if [ -f "$OPENMLDB_PID_FILE" ]; then
            # kill the mon process, the component process will be killed automatically
            PID=$(tr -d '\0' < "$OPENMLDB_PID_FILE")
            if kill -0 "$PID" > /dev/null 2>&1; then
                echo -e "${RED}$COMPONENT already running as process $PID ${RES}"
                exit 0
            fi
        fi

        # java style
        if [ "$COMPONENT" == "taskmanager" ] || [ "$COMPONENT" == "synctool" ]; then
            # unique setup
            if [ "$COMPONENT" == "taskmanager" ]; then
                echo "SPARK_HOME: ${SPARK_HOME}"
            fi
            if [ -f "./conf/${COMPONENT}.properties" ]; then
                echo "Rewrite properties by ./conf/${COMPONENT}.properties"
                cp ./conf/"${COMPONENT}".properties ./"${COMPONENT}"/conf/"${COMPONENT}".properties
            fi
            pushd ./"$COMPONENT"/bin/ > /dev/null
            mkdir -p "$LOG_DIR" # no matter the path is abs or relat
            if [ "$DAEMON_MODE" = "true" ]; then
                "${ROOTDIR}"/"${MON_BINARY}" "./${COMPONENT}.sh" -d -s 10 -l "$LOG_DIR"/"$COMPONENT"_mon.log -m "${ROOTDIR}/${OPENMLDB_PID_FILE}" -p "${ROOTDIR}/${OPENMLDB_PID_FILE}.child"
                sleep 3
                MON_PID=$(tr -d '\0' < "${ROOTDIR}/${OPENMLDB_PID_FILE}")
                PID=$(tr -d '\0' < "${ROOTDIR}/${OPENMLDB_PID_FILE}.child")
                echo "mon pid is $MON_PID, process pid is $PID, check $PID status"
            else
                sh ./"${COMPONENT}".sh > "$LOG_DIR"/"$COMPONENT".log 2>&1 &
                PID=$!
                echo "process pid is $PID"
            fi

            popd > /dev/null 
            sleep 10
            if kill -0 "$PID" > /dev/null 2>&1; then
                if [ "$DAEMON_MODE" != "true" ]; then
                    echo "$PID" > "$OPENMLDB_PID_FILE"
                fi
                echo "Start ${COMPONENT} success"
                exit 0
            fi
            echo -e "${RED}Start ${COMPONENT} failed! Please check log in ${LOG_DIR}/${COMPONENT}[_mon].log and ./${COMPONENT}/bin/logs/${COMPONENT}.log ${RES}"
        else
            # cxx style
            BIN_NAME="openmldb"
            if [ "$COMPONENT" = "data_collector" ]; then
                BIN_NAME="data_collector"
            fi
            if [ "$DAEMON_MODE" = "true" ]; then # nohup? test it
                START_CMD="./bin/${BIN_NAME} --flagfile=./conf/${COMPONENT}.flags --enable_status_service=true"
                # save the mon process pid, but check the component pid
                ${MON_BINARY} "${START_CMD}" -d -s 10 -l "$LOG_DIR"/"$COMPONENT"_mon.log -m "${OPENMLDB_PID_FILE}" -p "${OPENMLDB_PID_FILE}.child"
                # sleep for pid files
                sleep 3
                MON_PID=$(tr -d '\0' < "$OPENMLDB_PID_FILE")
                PID=$(tr -d '\0' < "$OPENMLDB_PID_FILE.child")
                echo "mon pid is $MON_PID, process pid is $PID, check $PID status"
            else
                # DO NOT put the whole command in variable
                ./bin/${BIN_NAME} --flagfile=./conf/"${COMPONENT}".flags --enable_status_service=true >> "$LOG_DIR"/"$COMPONENT".log 2>&1 &
                PID=$!
                echo "process pid is $PID"
            fi
            if [ -x "$(command -v curl)" ]; then
                sleep 3
                ENDPOINT=$(grep '^\--endpoint' ./conf/"$COMPONENT".flags | grep -v '#' | awk -F '=' '{print $2}')
                COUNT=1
                while [ $COUNT -lt 12 ]
                do
                    if ! curl --show-error --silent -o /dev/null "http://$ENDPOINT/status"; then
                        echo "curl server status failed, retry later"
                        sleep 1
                        (( COUNT+=1 ))
                    elif kill -0 "$PID" > /dev/null 2>&1; then
                        if [ "$DAEMON_MODE" != "true" ]; then
                            echo $PID > "$OPENMLDB_PID_FILE"
                        fi
                        echo "Start ${COMPONENT} success"
                        exit 0
                    else
                        break
                    fi
                done
            else
                echo "no curl, sleep 10s and then check the process running status"
                sleep 10
                if kill -0 "$PID" > /dev/null 2>&1; then
                    if [ "$DAEMON_MODE" != "true" ]; then
                        echo $PID > "$OPENMLDB_PID_FILE"
                    fi
                    echo "Start ${COMPONENT} success"
                    exit 0
                fi
            fi
            echo -e "${RED}Start ${COMPONENT} failed! Please check log in ${LOG_DIR}/${COMPONENT}[_mon].log and ${LOG_DIR}/${COMPONENT}.INFO ${RES}"
        fi
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
                sleep 3
            fi
            rm "$OPENMLDB_PID_FILE"
        fi
        if [ ! -f "$OPENMLDB_PID_FILE.child" ]
        then
            # normal mode
            echo "Stop ${COMPONENT} success"
        else
            # check the child (component process)
            PID="$(tr -d '\0' < "$OPENMLDB_PID_FILE.child")"
            if kill -0 "$PID" > /dev/null 2>&1; then
                echo "After mon stopped, component is still alive, kill it"
                kill "$PID"
            fi
            rm "$OPENMLDB_PID_FILE.child"
            echo "Stop ${COMPONENT} success(mon mode)"
        fi 
        ;;
    restart)
        shift
        # $0 may be 'bin/start.sh', 'start.sh', etc. We use the real script name in bin/.
        cd "$BINDIR" || exit 1
        sh start.sh stop "${@}"
        sleep 15
        sh start.sh start "${@}"
        ;;
    *)
        echo "Only support {start|stop|restart}" >&2
esac
