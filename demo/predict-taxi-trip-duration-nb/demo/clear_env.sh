#!/bin/bash
#
# clear_env.sh

cd /work/zookeeper-3.4.14 && ./bin/zkServer.sh stop
pkill mon
pkill python3
rm -rf /tmp/*
