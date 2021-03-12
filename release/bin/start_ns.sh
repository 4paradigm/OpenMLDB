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

#! /bin/sh
#
# start_ns.sh
CURDIR=`pwd`
cd "$(dirname "$0")"/../
FEDBPIDFILE="./bin/ns.pid"
mkdir -p "$(dirname "$FEDBPIDFILE")"
LOGDIR=`grep log_dir ./conf/nameserver.flags | awk -F '=' '{print $2}'`
mkdir -p $LOGDIR
case $1 in
    start)
        echo -n "Starting nameserver ... "
        if [ -f "$FEDBPIDFILE" ]; then
            if kill -0 `cat "$FEDBPIDFILE"` > /dev/null 2>&1; then
                echo nameserver already running as process `cat "$FEDBPIDFILE"`.
                exit 0
            fi
        fi
        ./bin/mon ./bin/boot_ns.sh -d -s 10 -l $LOGDIR/fedb_ns_mon.log -m $FEDBPIDFILE
        if [ $? -eq 0 ]
        then
            sleep 1
            echo STARTED
        else
            echo SERVER DID NOT START
            exit 1
        fi
        ;;
    stop)
        echo -n "Stopping nameserver ... "
        if [ ! -f "$FEDBPIDFILE" ]
        then
             echo "no nameserver to stop (could not find file $FEDBPIDFILE)"
        else
            kill $(cat "$FEDBPIDFILE")
            rm "$FEDBPIDFILE"
            echo STOPPED
        fi    
        ;;
    restart)
        shift
        cd $CURDIR
        sh "$0" stop ${@}
        sleep 5
        sh "$0" start ${@}
        ;;
    *)
        echo "Usage: $0 {start|stop|restart}" >&2
esac    
