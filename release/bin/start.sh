#! /bin/bash

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

if [[ $OSTYPE == 'darwin'* ]]; then
  MON_BINARY='./bin/mon_mac'
else
  MON_BINARY='./bin/mon'
fi

export COMPONENTS="tablet tablet2 nameserver apiserver taskmanager standalone_tablet standalone_nameserver standalone_apiserver"

if [ $# -lt 2 ]; then
  echo "Usage: start.sh start/stop/restart <component>"
  echo "component: $COMPONENTS"
  exit 1
fi

CURDIR=$(pwd)
cd "$(dirname "$0")"/../ || exit 1

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
            if tr -d '\0' < "$OPENMLDB_PID_FILE" | xargs kill -0 > /dev/null 2>&1; then
                echo tablet already running as process "$(tr -d '\0' < "$OPENMLDB_PID_FILE")".
                exit 0
            fi
        fi

        # Ref https://github.com/tj/mon
        if [ "$COMPONENT" != "taskmanager" ]; then
            $MON_BINARY "./bin/boot.sh $COMPONENT" -d -s 10 -l "$LOG_DIR"/"$COMPONENT"_mon.log -m "$OPENMLDB_PID_FILE";
            sleep 1
        else
            $MON_BINARY "./bin/boot_taskmanager.sh" -d -s 10 -l "$LOG_DIR"/"$COMPONENT"_mon.log -m "$OPENMLDB_PID_FILE";
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
            fi
            rm "$OPENMLDB_PID_FILE"
            echo STOPPED
        fi
        ;;
    restart)
        shift
        cd "$CURDIR" || exit 1
        sh "$0" stop "${@}"
        sleep 5
        sh "$0" start "${@}"
        ;;
    *)
        echo "Only support {start|stop|restart}" >&2
esac
