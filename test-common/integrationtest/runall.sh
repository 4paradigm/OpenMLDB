testpath=$(cd "$(dirname "$0")"; pwd)
projectpath=${testpath}/../..

# get rtidb ver
rtidbver=`echo \`grep "RTIDB_VERSION" ${projectpath}/CMakeLists.txt|awk '{print $2}'|awk -F ')' '{print $1}'\`|sed 's/\ /\./g'`
echo "RTIDB_VERSION is ${rtidbver}"

# setup test env
sh ${testpath}/setup.sh ${rtidbver}
source ${testpath}/env.conf

# start all servers
python ${testpath}/setup.py -C=true

# run integration test
if [ $1 = 1 ]; then
    sed -i 's/multidimension\ =\ false/multidimension\ =\ true/g' ${testconfpath}
else
    sed -i 's/multidimension\ =\ true/multidimension\ =\ false/g' ${testconfpath}
fi
python ${testpath}/runall.py -R="${runlist}" -N="${norunlist}"

# teardown kill services
python ${testpath}/setup.py -T=true
