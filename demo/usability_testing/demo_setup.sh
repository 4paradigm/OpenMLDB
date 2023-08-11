#!/bin/bash
set -ex
pip3 install faker pandas numpy click pyarrow
echo "mock datat t1,t2"
python3 data_mocker.py -s "create table t1(c1 int, c2 string)" -o /tmp/openmldb_test/t1/ -nf 10 -n 1000 -f csv
python3 data_mocker.py -s "create table t1(c1 int, c2 string)" -o /tmp/openmldb_test/t2/ -nf 10 -n 1000
echo "setup offline test script"
sed "s#<src_path>#file:///tmp/openmldb_test#" offline_test.sql.template > offline_test.sql
sed -i'' "s#<dst_path>#file:///tmp/openmldb_testout#" offline_test.sql
