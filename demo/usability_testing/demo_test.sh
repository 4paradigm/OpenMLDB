#!/bin/bash
set -ex

echo "start demo test"
echo "simple test"
cat simple_test.sql | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --interactive=false

echo "offline test with mocked data"
cat offline_test.sql | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --interactive=false
OUT1=$(wc -l /tmp/openmldb_testout/t1_deep/*.csv | grep -v total | awk '{sum+=$1-1}END{print sum}')
OUT2=$(wc -l /tmp/openmldb_testout/t1_soft/*.csv | grep -v total | awk '{sum+=$1-1}END{print sum}')
OUT3=$(wc -l /tmp/openmldb_testout/t2_deep/*.csv | grep -v total | awk '{sum+=$1-1}END{print sum}')
# _double should be 2*total_row_count
OUT4=$(wc -l /tmp/openmldb_testout/t1_double/*.csv | grep -v total | awk '{sum+=$1-1}END{print sum}')
OUT5=$(wc -l /tmp/openmldb_testout/t2_double/*.csv | grep -v total | awk '{sum+=$1-1}END{print sum}')
if [ "$OUT1,$OUT2,$OUT3,$OUT4,$OUT5" != "10000,10000,10000,20000,20000" ]; then
    echo "offline test failed, $OUT1,$OUT2,$OUT3,$OUT4,$OUT5"
    exit -1
fi

echo "udf test"
curl -SLO https://openmldb.ai/download/testing/libtest_udf.so 
cp libtest_udf.so /tmp/openmldb/tablet-1/udf
cp libtest_udf.so /tmp/openmldb/tablet-2/udf
cp libtest_udf.so /work/openmldb/taskmanager/bin/udf
cat udf_test.sql | /work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --interactive=false
