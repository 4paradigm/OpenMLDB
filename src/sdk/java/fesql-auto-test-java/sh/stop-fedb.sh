#!/usr/bin/env bash

sh fedb-ns-1/bin/start_ns.sh stop
sh fedb-ns-2/bin/start_ns.sh stop
sh fedb-tablet-1/bin/start.sh stop
sh fedb-tablet-2/bin/start.sh stop
sh fedb-tablet-3/bin/start.sh stop
sh zookeeper-3.4.10/bin/zkServer.sh stop

rm fedb-ns-1/bin/fedb
rm fedb-ns-2/bin/fedb
rm fedb-tablet-1/bin/fedb
rm fedb-tablet-2/bin/fedb
rm fedb-tablet-3/bin/fedb

cp -r fedb fedb-ns-1/bin/
cp -r fedb fedb-ns-2/bin/
cp -r fedb fedb-tablet-1/bin/
cp -r fedb fedb-tablet-2/bin/
cp -r fedb fedb-tablet-3/bin/

sh fedb-ns-1/bin/start_ns.sh restart
sh fedb-ns-2/bin/start_ns.sh restart
sh fedb-tablet-1/bin/start.sh restart
sh fedb-tablet-2/bin/start.sh restart
sh fedb-tablet-3/bin/start.sh restart


sh fedb-ns-1/bin/start_ns.sh stop
sh fedb-ns-2/bin/start_ns.sh stop
sh fedb-tablet-1/bin/start.sh stop
sh fedb-tablet-2/bin/start.sh stop
sh fedb-tablet-3/bin/start.sh stop
sh rtidb-blob-1/bin/start.sh stop
sh rtidb-blob-proxy-1/bin/start.sh stop
sh zookeeper-3.4.10/bin/zkServer.sh stop