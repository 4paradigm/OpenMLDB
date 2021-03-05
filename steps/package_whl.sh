#! /bin/sh
#
# pack_whl.sh
#
set -e
WORK_DIR=`pwd`
mkdir -p $WORK_DIR/build
cd $WORK_DIR/build && cmake .. && make python_package

cd $WORK_DIR
server="http://pypi.4paradigm.com"
user=4paradigm
passwd=paradigm42

devpi use ${server}

devpi login ${user} --password=${passwd}

devpi use 4paradigm/dev

cd build/python

devpi upload --formats bdist_wheel --no-vcs

