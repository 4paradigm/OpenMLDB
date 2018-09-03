#! /bin/sh
#
# start.sh
./bin/mon ./bin/boot.sh -d -l ./logs/rtidb_mon.log
./bin/mon ./bin/boot_metric.sh -l ./logs/rtidb_mon_m.log

