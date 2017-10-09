testpath=$(cd "$(dirname "$0")"; pwd)
cd ${testpath}
sh setup.sh
sleep 2
source ./env.conf
python runall.py
