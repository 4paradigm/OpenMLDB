#! /bin/sh
#
# boot_metricbeat.sh
ulimit -c unlimited
./bin/metricbeat -e -c ./metricbeat.yml -d "*" --path.home $PWD/conf
