# $1 should be 1(multi dimension) or 0(one dimension)
# $2 should be regex for filter testcases
ulimit -c unlimited
if [ -f "test-common/integrationtest/setup.sh" ]
then
    sh test-common/integrationtest/runall.sh $1 $2
fi
