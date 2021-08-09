# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

testpath=$(cd "$(dirname "$0")"; pwd)
projectpath=${testpath}/../..

# get rtidb ver
rtidbver=`echo \`grep "RTIDB_VERSION" ${projectpath}/CMakeLists.txt|awk '{print $2}'|awk -F ')' '{print $1}'\`|sed 's/\ /\./g'`
echo "RTIDB_VERSION is ${rtidbver}"

# setup test env
sh ${testpath}/setup.sh ${rtidbver}
source ${testpath}/env.conf

# start all servers
python2 ${testpath}/setup.py -C=true

# run integration test
if [ $1 = 1 ]; then
    sed -i 's/multidimension\ =\ false/multidimension\ =\ true/g' ${testconfpath}
else
    sed -i 's/multidimension\ =\ true/multidimension\ =\ false/g' ${testconfpath}
fi

if [ $2 = "ns_client" -o $2 = "disk" ]; then
    sed -i 's/cluster_mode\ \=.*/cluster_mode\ \=\ cluster/g' ${testconfpath}
else    
    sed -i 's/cluster_mode\ \=.*/cluster_mode\ \=\ single/g' ${testconfpath}
fi

python2 ${testpath}/runall.py -R="${runlist}" -N="${norunlist}"
code=$?
# teardown kill services
python2 ${testpath}/setup.py -T=true
exit $code
