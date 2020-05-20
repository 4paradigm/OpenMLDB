#!/bin/bash
curl ftp://172.27.12.154/pub/team_hpc/titanse/pz-titanse.tar.gz -o pz.tar.gz
tar zxf pz.tar.gz -C ./thirdparty
curl ftp://172.27.12.154/pub/team_hpc/titanse/rocksdb-titanse-9e5f14ff.tar.gz -o rocksdb.tar.gz
tar zxf rocksdb.tar.gz -C ./thirdparty
curl "https://nexus.4pd.io/repository/raw-hosted/fpga-rte/release/v0.2/aclrte-linux64-rte-19.2.0.57-v0.2.tar.gz" -O
tar zxf aclrte-linux64-rte-19.2.0.57-v0.2.tar.gz -C ./thirdparty
cp -L ./thirdparty/aclrte-linux64/lib/* ./thirdparty/lib/