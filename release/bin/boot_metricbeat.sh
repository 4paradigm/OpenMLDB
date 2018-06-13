#! /bin/sh
#
# boot_ns.sh
ulimit -c unlimited
./bin/metricbeat -e -c ./conf/metricbeat.yml -d "*" --path.home $PWD
