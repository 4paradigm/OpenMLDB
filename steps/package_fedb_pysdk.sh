#! /bin/sh
#
# package.sh
#

WORKDIR=`pwd`
package=fedb-pysdk-$1
rm -rf ${package} || :
mkdir ${package} || :
cp build/sql_pysdk/dist/*.whl ${package}
tar -cvzf ${package}.tar.gz ${package}
