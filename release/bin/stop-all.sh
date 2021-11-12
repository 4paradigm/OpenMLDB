#! /bin/sh
# stop-all.sh
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
BASEDIR="$(dirname "$( cd "$( dirname "$0"  )" && pwd )")"
for COMPONENT in $COMPONENTS; do
    PID_FILE="$BASEDIR/bin/$COMPONENT.pid"
    if [ ! -f "$PID_FILE" ]
    then
         echo "no $COMPONENT to stop (could not find file $PID_FILE)"
    else
        kill "$(cat "$PID_FILE")"
        rm "$PID_FILE"
    fi
done
echo "OpenMLDB stopped"
