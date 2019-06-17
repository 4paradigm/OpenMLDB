#! /bin/sh
#
# local_run.sh

docker run -v `pwd`:/usr/workdir/rtidb -p 6181:6181 \
                                       -p 9622:9622 \
                                       -p 9623:9623 \
                                       -p 9624:9624 \
                                       -p 9520:9520 \
                                       -p 9521:9621 \
                                       -p 9522:9622 \
                                       -it develop-registry.4pd.io/centos6_gcc7_rtidb_build:0.0.4 /bin/bash


