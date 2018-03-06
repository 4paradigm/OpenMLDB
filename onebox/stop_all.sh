#! /bin/sh
#
# stop_all.sh
ps -ef | grep rtidb | grep onebox | awk '{print $2}' | while read line; do kill -9 $line; done
