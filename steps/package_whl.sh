#! /bin/sh
#
# pack_whl.sh
#

server="http://pypi.4paradigm.com"
user=4paradigm
passwd=paradigm4

devpi use ${server}

devpi login ${user} --password=${passwd}

devpi use 4paradigm/dev

cd build/python

devpi upload --formats bdist_wheel --no-vcs

