#from server_checker import ServerChecker
from dist_conf import DistConfReader
from dist_conf import ConfParser
from conf_validator import StandaloneConfValidator
import util
from log_analysis import LogAnalysis

#dist_conf = DistConfReader('../tests/standalone_dist.yml').conf()
#dist_conf = DistConfReader('../tests/cluster_dist.yml').conf()
#server = ServerChecker(dist_conf.full_conf)
#server.check_component()
#server.run_test_sql()

#ns_conf_dict = ConfParser('../tests/work/standalone/standalone_nameserver.flags').conf()
#tablet_conf_dict = ConfParser('../tests/work/standalone/standalone_tablet.flags').conf()
#validator = StandaloneConfValidator(ns_conf_dict, tablet_conf_dict)
#validator.validate()

#cmd='ls /work/openmldb/bin/'
#path='/work/openmldb/bin/'
#version=util.get_openmldb_version(path)
#print(version)

#log_ans = LogAnalysis('tablet', '172.24.4.56:30921', [('tablet.info.log.20220729-193502.20559', '/tmp/dl_test/log/172.24.4.56:30921-tablet/tablet.info.log.20220729-193502.20559')])
#log_ans = LogAnalysis('taskmanager', '172.24.4.40:30902', [('taskmanager.log', '/tmp/dl_test/log/172.24.4.40:30902-taskmanager/taskmanager.log'), ('job_19_error.log', '/tmp/dl_test/log/172.24.4.40:30902-taskmanager/job_19_error.log')])
log_ans = LogAnalysis('taskmanager', '172.24.4.40:30902', [('job_19_error.log', '/tmp/dl_test/log/172.24.4.40:30902-taskmanager/job_19_error.log')])

log_ans.analysis_log()
