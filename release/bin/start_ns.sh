#! /bin/sh
#
# start_ns.sh
cd "$(dirname "$0")"/../
RTIDBPIDFILE="./bin/ns.pid"
mkdir -p "$(dirname "$RTIDBPIDFILE")"
LOGDIR=`grep log_dir ./conf/nameserver.flags | awk -F '=' '{print $2}'`
mkdir -p $LOGDIR
case $1 in
    start)
        echo -n "Starting nameserver ... "
        if [ -f "$RTIDBPIDFILE" ]; then
            if kill -0 `cat "$RTIDBPIDFILE"` > /dev/null 2>&1; then
                echo nameserver already running as process `cat "$RTIDBPIDFILE"`.
                exit 0
            fi
        fi
        ./bin/mon ./bin/boot_ns.sh -d -s 10 -l $LOGDIR/rtidb_ns_mon.log -m $RTIDBPIDFILE
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
        if [ ! -f "$RTIDBPIDFILE" ]
        then
             echo "no nameserver to stop (could not find file $RTIDBPIDFILE)"
        else
            kill $(cat "$RTIDBPIDFILE")
            rm "$RTIDBPIDFILE"
            echo STOPPED
        fi    
        ;;
    restart)
        shift
        "$0" stop ${@}
        sleep 5
        "$0" start ${@}
        ;;
    *)
        echo "Usage: $0 {start|stop|restart}" >&2
esac    
