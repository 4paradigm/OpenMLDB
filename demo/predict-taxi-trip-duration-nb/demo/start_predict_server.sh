#! /bin/bash
#
# start_predict_server.sh

echo "start predict server"
if [ $# -eq 1 ]; then
    nohup python3 predict_server_s.py $1 >/tmp/p.log 2>&1 &
else
    nohup python3 predict_server.py $1 $2 >/tmp/p.log 2>&1 &
fi
sleep 1
