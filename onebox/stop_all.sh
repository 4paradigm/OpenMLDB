#! /bin/sh
#
# stop_all.sh

if [[ "$OSTYPE" == "linux-gnu"* ]] 
then
    ps -efx | grep fesql | grep role | grep -v grep | awk '{print $1}' | while read line; do kill -9 $line;done
else
    ps -efx | grep fesql | grep role | grep -v grep | awk '{print $2}' | while read line; do kill -9 $line;done
fi

