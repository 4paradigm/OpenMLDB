#! /bin/sh
#
# boot.sh
ulimit -c unlimited
./bin/rtidb --flagfile=./conf/tablet.flags
