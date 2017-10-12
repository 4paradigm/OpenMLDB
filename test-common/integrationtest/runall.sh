testpath=$(cd "$(dirname "$0")"; pwd)
cd ${testpath}
sh setup.sh $1
sleep 2
source ./env.conf
python runall.py
sh setup.sh $1