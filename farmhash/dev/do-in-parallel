#!/bin/bash

# Usage:
#   do-in-parallel [-k] MAX_PARALLELISM FILENAME

RANDOM_STRING=`date +%N%s | md5sum | tr -cd "[0-9]"`
MAKEFILE=/tmp/foo${RANDOM_STRING}-$$

k=
if [ $1 == "-k" ]
then
  k=-k
  shift
fi

N=$(wc -l $2 | cut -d " " -f 1)
echo -n .phony: all > $MAKEFILE
for i in `seq 1 $N`; do
  echo -n " t$i" >> $MAKEFILE
done
echo >> $MAKEFILE
echo >> $MAKEFILE
echo -n all: >> $MAKEFILE
for i in `seq 1 $N`; do
  echo -n " t$i" >> $MAKEFILE
done
echo >> $MAKEFILE
echo >> $MAKEFILE

sed 's/\(.\)/	\1/' $2 | awk '/./ { printf("t%d:\n", NR); print; print "" }' >> $MAKEFILE
exec make $k -j $1 -f $MAKEFILE
