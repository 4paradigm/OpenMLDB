#! /bin/sh

docker run -v `pwd`:/fesql develop-registry.4pd.io/centos6_gcc7_fesql:0.0.10 bash -c "ls /fesql;cd /fesql && sh tools/compile_in_docker.sh"




