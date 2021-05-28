#! /bin/sh
# start.sh
#
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

export COMPONENTS="tablet nameserver apiserver"

if [ $# -lt 2 ]; then
  echo "Usage: start.sh start/stop/restart <component>"
  echo "component: $COMPONENTS"
  exit 1
fi

CURDIR=$(pwd)
cd "$(dirname "$0")"/../ || exit 1

OP=$1
COMPONENT=$2
HAS_COMPONENT=false
for ITEM in $COMPONENTS;
do
  if [ "$COMPONENT" = "$ITEM" ]; then
    HAS_COMPONENT=true
  fi
done

"$HAS_COMPONENT" == false || { echo "No component named $COMPONENT in [$COMPONENTS]"; exit 1; }

FEDBPIDFILE="./bin/$COMPONENT.pid"
mkdir -p "$(dirname "$FEDBPIDFILE")"
LOGDIR=$(grep log_dir ./conf/"$COMPONENT".flags | awk -F '=' '{print $2}')
[ -n "$LOGDIR" ] || { echo "Invalid log dir"; exit 1; }
mkdir -p "$LOGDIR"
case $OP in
    start)
        echo "Starting $COMPONENT ... "
        if [ -f "$FEDBPIDFILE" ]; then
            if kill -0 "$(cat "$FEDBPIDFILE")" > /dev/null 2>&1; then
                echo tablet already running as process "$(cat "$FEDBPIDFILE")".
                exit 0
            fi
        fi

        # Ref https://github.com/tj/mon
        if ./bin/mon "./bin/boot.sh $COMPONENT" -d -s 10 -l "$LOGDIR"/fedb_mon.log -m "$FEDBPIDFILE";
        then
            sleep 1
            echo STARTED
        else
            echo SERVER DID NOT START
            exit 1
        fi
        ;;
    stop)
        echo "Stopping $COMPONENT ... "
        if [ ! -f "$FEDBPIDFILE" ]
        then
             echo "no tablet to stop (could not find file $FEDBPIDFILE)"
        else
            kill "$(cat "$FEDBPIDFILE")"
            rm "$FEDBPIDFILE"
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
