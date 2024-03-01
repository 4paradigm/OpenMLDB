#!/bin/bash

# Delete lines in a file from the first occurrence of
# $1 to the first occurrence of $2, inclusive.  The file to
# operate on is $3.  Both strings must appear in the file,
# and the first string must appear first (or on the same line).
# $1 and $2 are regular expressions.

die() {
  echo $*
  exit 1
}

from=$1
to=$2
filename=$3

tmpfile=/tmp/$RANDOM$RANDOM$RANDOM

grep "$from" "$filename" > /dev/null || exit 1
grep "$to" "$filename" > /dev/null || exit 1
l0=$(grep -n "$from" "$filename" | head -1 | cut -f 1 -d :)
l1=$(grep -n "$to" "$filename" | head -1 | cut -f 1 -d :)
[[ $l0 -lt $l1 ]] || die out of order: lines $l0 $l1
head -n $((l0 - 1)) "$filename" > $tmpfile && \
tail -n +$((l1 + 1)) "$filename" >> $tmpfile && \
exec mv $tmpfile "$filename"

