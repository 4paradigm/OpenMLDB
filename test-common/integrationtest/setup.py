import os
import sys
import time
sys.path.append(os.getenv('testpath'))
import libs.conf as conf
from libs.clients.ns_cluster import NsCluster
from libs.clients.tb_cluster import TbCluster


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser(description='setup test env')
    ap.add_argument('-T', '--teardown', default=False, help='kill all nodes of test cluster')
    ap.add_argument('-C', '--clear', default=False, help='clear all logs and db')
    args = ap.parse_args()

    nsc = NsCluster(conf.zk_endpoint, *(i for i in conf.ns_endpoints))
    tbc = TbCluster(conf.zk_endpoint, [i for i in conf.tb_endpoints])

    nsc.stop_zk()
    nsc.clear_zk()
    nsc.kill(*nsc.endpoints)
    tbc.kill(*tbc.endpoints)

    if not args.clear or args.clear.lower() == 'false':
        pass
    else:
        tbc.clear_db()
        nsc.clear_ns()

    if not args.teardown or args.teardown.lower() == 'false':
        nsc.start_zk()
        nsc.start(*nsc.endpoints)
        time.sleep(2)
        nsc.get_ns_leader()
        tbc.start(tbc.endpoints)
        nsc_leader = nsc.leader
