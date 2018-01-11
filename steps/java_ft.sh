testpath=$(cd "$(dirname "$0")"; pwd)
projectpath=${testpath}/..
sh ${projectpath}/test-common/integrationtest/setup.sh
source ${projectpath}/test-common/integrationtest/env.conf
python ${projectpath}/test-common/integrationtest/setup.py
cd ${projectpath}/java && mvn test -Dtest=com._4paradigm.rtidb.client.functiontest.cases.*Test
python ${testpath}/setup.py teardown
