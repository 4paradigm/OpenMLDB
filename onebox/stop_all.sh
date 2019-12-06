#! /bin/sh
#
# stop_all.sh

ps -efu | grep fesql | grep role | grep -v grep | awk '{print $2}' | while read line; do kill -9 $line;done
