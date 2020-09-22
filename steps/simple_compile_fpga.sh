#! /bin/sh
#
# compile.sh

WORK_DIR=`pwd`
curl ftp://ftp.4pd.io/pub/team_hpc/titanse/rocksdb-titanse-v0.0.1-release.tar.gz -o rocksdb.tar.gz
tar zxf rocksdb.tar.gz -C ./thirdparty
sed -i 's/\"Enable pz compression for ssd tables\"\ OFF/\"Enable pz compression for ssd tables\"\ ON/g' CMakeLists.txt
sh steps/gen_code.sh
mkdir -p $WORK_DIR/build
cd $WORK_DIR/build && cmake .. && make -j16 rtidb && mv bin/rtidb bin/rtidb_fpga
code=$?
cd $WORK_DIR
exit $code
