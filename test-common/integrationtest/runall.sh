testpath=$(cd "$(dirname "$0")"; pwd)
projectpath=${testpath}/../..

# get rtidb ver
rtidbver=`echo \`grep "RTIDB_VERSION" ${projectpath}/CMakeLists.txt|awk '{print $2}'|awk -F ')' '{print $1}'\`|sed 's/\ /\./g'`
if [ $1 ];then
    rtidbver=$1
fi

# setup test env
sh ${testpath}/setup.sh ${rtidbver}
source ${testpath}/env.conf

# setup xmlrunner
if [ -d "${testpath}/xmlrunner" ]
then
    echo "xmlrunner exist"
else
    echo "start install xmlrunner...."
    wget -O ${projectpath}/thirdsrc/xmlrunner.tar.gz http://pkg.4paradigm.com:81/rtidb/dev/xmlrunner.tar.gz >/dev/null
    tar -zxvf ${projectpath}/thirdsrc/xmlrunner.tar.gz -C ${testpath} >/dev/null
    echo "install xmlrunner done"
fi

# run integration test
python ${testpath}/runall.py

# reset test env
sh ${testpath}/setup.sh