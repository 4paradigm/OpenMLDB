testpath=$(cd "$(dirname "$0")"; pwd)
projectpath=${testpath}/../..
rtidbver=`echo \`grep "RTIDB_VERSION" ${projectpath}/CMakeLists.txt|awk '{print $2}'|awk -F ')' '{print $1}'\`|sed 's/\ /\./g'`
cd ${testpath}
if [ $1 ];then
    rtidbver=$1
fi
sh setup.sh ${rtidbver}
sleep 2
source ./env.conf
python runall.py
sh setup.sh ${rtidbver}