#! /bin/sh
#
# boot_filebeat.sh
ulimit -c unlimited
./bin/filebeat -e --path.home $PWD/conf
