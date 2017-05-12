#! /bin/sh
set -e -u -E # this script will exit if any sub-command fails

WORK_DIR=`pwd`
test -d /tmp/rtidb && cd /tmp/rtidb && rm -rf *
cd $WORK_DIR
cd $WORK_DIR/build/bin && ls | grep test | while read line; do ./$line ; done
