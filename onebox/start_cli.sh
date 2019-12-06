#! /bin/sh
#
# start_cli.sh
mkdir -p log/cli
../build/src/fesql --role=client --tablet_endpoint=127.0.0.1:9212 --endpoint=127.0.0.1:9211 --log_dir=log/cli


