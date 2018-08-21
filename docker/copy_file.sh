#! /bin/sh
cp ./build/bin/rtidb ./docker/tablet/bin/
cp ./build/bin/rtidb ./docker/nameserver/bin/
cp ./release/conf/tablet.flags ./docker/tablet/
cp ./release/conf/nameserver.flags ./docker/nameserver/
cd docker/tablet
if [ -f "py_env_succ" ]
then
    echo "py_env exist"
else
	wget http://pkg.4paradigm.com:81/rtidb/dev/py_env.tar.gz >/dev/null
    tar zxf py_env.tar.gz >/dev/null
    touch py_env_succ
    echo "get py_env done"
fi
cd -
