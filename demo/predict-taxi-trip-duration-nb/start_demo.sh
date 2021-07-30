#! /bin/sh
#
# start_demo.sh

docker run -h=`hostname`  --network=host -t 4pdosc/fedb_notebook:0.4.0 jupyter notebook

