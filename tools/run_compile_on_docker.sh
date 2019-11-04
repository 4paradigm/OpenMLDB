#! /bin/sh

docker run -v `pwd`:/fesql develop-registry.4pd.io/fesql_build_base:0.8 bash -c "ls /fesql;cd /fesql && sh tools/compile_in_docker.sh"




