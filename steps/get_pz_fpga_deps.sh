#!/bin/bash
curl ftp://172.27.12.154/pub/team_hpc/pz_rtidb/pz-cd253af2.tar.gz -o pz.tar.gz
tar zxf pz.tar.gz -C ./thirdparty
curl ftp://172.27.12.154/pub/team_hpc/pz_rtidb/rocksdb-9c9cc946.tar.gz -o rocksdb.tar.gz
tar zxf rocksdb.tar.gz -C ./thirdparty
curl "https://nexus.4pd.io/repository/raw-hosted/fpga-rte/dev/aclrte-linux64-rte-19.2.0.57-fa510q-190430-4pdx900-200408.tar.gz" -O
tar zxf aclrte-linux64-rte-19.2.0.57-fa510q-190430-4pdx900-200408.tar.gz -C ./thirdparty
cp -L ./thirdparty/aclrte-linux64/lib/* ./thirdparty/lib/