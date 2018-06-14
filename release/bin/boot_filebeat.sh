#! /bin/sh
#
# boot_ns.sh
ulimit -c unlimited
./bin/filebeat -e --path.home $PWD/conf
