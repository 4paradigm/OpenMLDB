#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

    #local environment
    nsc = NsCluster(conf.zk_endpoint, *(i for i in conf.ns_endpoints))
    tbc = TbCluster(conf.zk_endpoint, [i for i in conf.tb_endpoints])

    nsc.stop_zk()
    nsc.clear_zk()
    nsc.kill(*nsc.endpoints)
    tbc.kill(*tbc.endpoints)

    #remote environment
    nsc_r = NsCluster(conf.zk_endpoint, *(i for i in conf.ns_endpoints_r))
    tbc_r = TbCluster(conf.zk_endpoint, [i for i in conf.tb_endpoints_r])

    nsc_r.kill(*nsc_r.endpoints)
    tbc_r.kill(*tbc_r.endpoints)

    if not args.clear or args.clear.lower() == 'false':
        pass
    else:
        tbc.clear_db()
        nsc.clear_ns()
        tbc_r.clear_db()
        nsc_r.clear_ns()

    if not args.teardown or args.teardown.lower() == 'false':
        nsc.start_zk()
        nsc.start(False, *nsc.endpoints)
        time.sleep(2)
        nsc.get_ns_leader()
        tbc.start(tbc.endpoints)
        nsc_leader = nsc.leader

        nsc_r.start(True,*nsc_r.endpoints)
        time.sleep(2)
        nsc_r.get_ns_leader(True)
        tbc_r.start(tbc_r.endpoints, True)
        nsc_leader_r = nsc_r.leader
