#! /bin/sh
# start-all.sh
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

arr=("tablet" "nameserver")
for COMPONENT in ${arr[@]};do
    PID_FILE="./bin/$COMPONENT.pid"
    mkdir -p "$(dirname "$PID_FILE")"
    LOG_DIR=$(grep log_dir ./conf/"$COMPONENT".flags | awk -F '=' '{print $2}')
    [ -n "$LOG_DIR" ] || { echo "Invalid log dir"; exit 1; }
    mkdir -p "$LOG_DIR"
    if [ -f "$PID_FILE" ]; then
        if kill -0 "$(cat "$PID_FILE")" > /dev/null 2>&1; then
            echo $COMPONENT already running as process "$(cat "$PID_FILE")".
            exit 0
        fi
    fi
    if ./bin/mon "./bin/boot.sh $COMPONENT" -d -s 10 -l "$LOG_DIR"/"$COMPONENT"_mon.log -m "$PID_FILE";
    then
        sleep 1
    else
        echo $COMPONENT start failed
        exit 1
    fi
done    
echo "OpenMLDB start success"
