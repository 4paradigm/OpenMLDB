#! /bin/sh
#
# start.sh
CURDIR=`pwd`
cd "$(dirname "$0")"/../
MONITORPIDFILE="./bin/monitor.pid"
NODEEXPORTERPIDFILE="./bin/node_exporter.pid"
mkdir -p "$(dirname "$MONITORPIDFILE")"
LOGDIR=`grep ^log_dir= ./conf/monitor.conf | awk -F '=' '{print $2}'`
mkdir -p $LOGDIR
case $1 in
    start)
        echo "Starting monitor ... "
        if [ -f "$MONITORPIDFILE" ]; then
            if kill -0 `cat "$MONITORPIDFILE"` > /dev/null 2>&1; then
                echo -n monitor already running as process `cat "$MONITORPIDFILE"`
            fi
        else    
            ./bin/mon ./bin/boot_monitor.sh -d -s 10 -l $LOGDIR/monitor_mon.log -m $MONITORPIDFILE
        fi
        echo "Starting node_exporter ... "
        if [ -f "$NODEEXPORTERPIDFILE" ]; then
            if kill -0 `cat "$NODEEXPORTERPIDFILE"` > /dev/null 2>&1; then
                echo node_exporter already running as process `cat "$NODEEXPORTERPIDFILE"`
            fi
        else    
            ./bin/mon ./bin/node_exporter -d -s 10 -l $LOGDIR/monitor_mon.log -m $NODEEXPORTERPIDFILE
        fi
        if [ $? -eq 0 ]
        then
            echo STARTED
        else
            echo SERVER DID NOT START
            exit 1
        fi
        ;;
    stop)
        echo "Stopping monitor ... "
        if [ ! -f "$MONITORPIDFILE" ]
        then
             echo "no monitor to stop (could not find file $MONITORPIDFILE)"
        else
            kill $(cat "$MONITORPIDFILE")
            rm "$MONITORPIDFILE"
        fi    
        echo "Stopping node_exporter ... "
        if [ ! -f "$NODEEXPORTERPIDFILE" ]
        then
             echo "no node_exporter to stop (could not find file $NODEEXPORTERPIDFILE)"
        else
            kill $(cat "$NODEEXPORTERPIDFILE")
            rm "$NODEEXPORTERPIDFILE"
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
