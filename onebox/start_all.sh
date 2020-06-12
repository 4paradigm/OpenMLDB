#! /bin/sh
#
# start_dbms.sh
mkdir -p log/dbms
mkdir -p log/tablet
BUILD_DIR=../build
${BUILD_DIR}/src/fesql --role=dbms  --port=9211  >dbms.log 2>&1 &
sleep 5
${BUILD_DIR}/src/fesql --role=tablet --endpoint=127.0.0.1:9212 --port=9212 --dbms_endpoint=127.0.0.1:9211 >tablet.log 2>&1 &
sleep 5

if $(ps -ef | grep -q 'src/fesql --role=dbms'); then
	echo "onebox dbms service started"
else
	echo "start onebox dbms service failed"
	cat dbms.log
	exit 1
fi

if $(ps -ef | grep -q 'src/fesql --role=tablet'); then
	echo "onebox tablet service started"
else
	echo "start onebox tablet service failed"
	cat tablet.log
	exit 1
fi
