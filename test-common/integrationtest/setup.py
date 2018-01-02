import os
import sys
sys.path.append(os.getenv('testpath'))
import libs.conf as conf
from libs.clients.ns_cluster import NsCluster
from libs.clients.tb_cluster import TbCluster


nsc = NsCluster(conf.zk_endpoint, *(i[1] for i in conf.ns_endpoints))
tbc = TbCluster(conf.zk_endpoint, [i[1] for i in conf.tb_endpoints], [i[1] for i in conf.tb_scan_endpoints])
nsc.stop_zk()
nsc.clear_zk()
nsc.start_zk()
nsc.kill(*nsc.endpoints)
nsc.start(*nsc.endpoints)
tbc.kill(*tbc.endpoints)
tbc.start(tbc.endpoints, tbc.scan_endpoints)
nsc_leader = nsc.leader
