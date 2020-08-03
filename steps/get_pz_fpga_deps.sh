#!/bin/bash
curl ftp://ftp.4pd.io/pub/team_hpc/titanse/pz-titanse-v0.1.0-release.tar.gz -o pz.tar.gz
tar zxf pz.tar.gz -C ./thirdparty
curl ftp://ftp.4pd.io/pub/team_hpc/titanse/rocksdb-titanse-v0.0.1-release.tar.gz -o rocksdb.tar.gz
tar zxf rocksdb.tar.gz -C ./thirdparty
curl "https://nexus.4pd.io/repository/raw-hosted/fpga-rte/release/v0.2/aclrte-linux64-aclrte-19.2.0.57-v0.2-release.tar.gz" -O
tar zxf aclrte-linux64-aclrte-19.2.0.57-v0.2-release.tar.gz -C ./thirdparty
cp -L ./thirdparty/aclrte-linux64/lib/* ./thirdparty/lib/