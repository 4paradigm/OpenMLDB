#! /bin/sh
#
# stop_all.sh

ps -efx | grep fesql | grep role | grep -v grep | awk '{print $1}' | while read line; do kill -9 $line;done
