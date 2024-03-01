#!/bin/bash

filename=$1
outputfilename=$2
namespace=$(basename ${filename%_gen.cc})

sed "s/SKELETON/${namespace}Test/g" < self-test-skeleton.cc > "$outputfilename"
l=$(fgrep -n 'if FARMHASH_TEST' "$filename" | cut -f 1 -d :)
fns=$(head -n $l "$filename" | grep '^[a-z].*(const char.*size_t len.*{' | sed -e 's/.* \([A-Z]\)/\1/' -e 's/[(].*//' | sort -u)
for fn in $fns
do
  [ ! -r SELFTEST$fn ] && echo "No SELFTEST$fn available! Please fix!"
  [ -r SELFTEST$fn ] && t=$(head -1 SELFTEST$fn | sed s/NAMESPACE/$namespace/g) && sed -i "/void Dump/a $t" "$outputfilename" && \
    b=$(tail -n 1 SELFTEST$fn | sed s/NAMESPACE/$namespace/g) && sed -i "/static int index/a $b" "$outputfilename"
done
# Insert special case for offset == -1: Check if the function(s) seems to do anything.
# We're making the simplifying assumption that everything in a
# farmhash??.cc file is available or nothing is.
stmt0=$(fgrep -v define "$outputfilename" | fgrep 'Check(' | head -1)
stmt1=$(fgrep -v define "$outputfilename" | fgrep 'Check(' | head -2 | tail -n 1)
stmt2=$(fgrep -v define "$outputfilename" | fgrep 'Check(' | head -3 | tail -n 1)
stmt=$(sed -e s/Check/IsAlive/g -e "s/ + offset//g" -e "s/len/len++/g" <<< "$stmt0 $stmt1 $stmt2")
sed -i "/static int index/a if (offset == -1) { int alive = 0; $stmt len -= 3; return alive > 0; }" "$outputfilename"
exit 0
