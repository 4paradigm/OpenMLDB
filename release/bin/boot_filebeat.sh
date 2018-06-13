#! /bin/sh
#
# boot_ns.sh
ulimit -c unlimited
./bin/filebeat -e -c ./conf/metricbeat.yml -d "*" --path.home $PWD
