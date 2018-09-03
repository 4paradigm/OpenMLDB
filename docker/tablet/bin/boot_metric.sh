#! /bin/sh
#
# boot.sh
source py_env/bin/activate
export metric_port
python ./bin/monitor.py
