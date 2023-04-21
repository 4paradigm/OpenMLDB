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

import requests
import termplotlib as tpl

from .connector import Connector


class TableChecker:
    def __init__(self, conn: Connector):
        self.conn = conn

    def check_distribution(self, dbs: list):
        ns_leader = self._get_nameserver()
        url = f"http://{ns_leader}/NameServer/ShowTable"
        res = requests.get(url, json={"show_all": True})
        tables = res.json()["table_info"]

        exist_dbs = [db[0] for db in self.conn.execfetch("SHOW DATABASES")]
        if not exist_dbs:
            return
        if dbs == ['']:
            dbs = exist_dbs
        assert all([db in exist_dbs for db in dbs]), "some databases are not exist"

        tablet2partition = {}
        tablet2count = {}
        tablet2mem = {}
        table_infos = []
        max_values = {'mp': 0, 'mc': 0, 'mm': 0}

        for table in tables:
            if table['db'] not in dbs:
                continue
            t = {}
            t['name'] = table['db'] + "." + table['name']
            parts = table['table_partition']
            part_dist = self._collect(parts,'')
            count_dist = self._collect(parts, 'record_cnt')
            mem_dist = self._collect(parts, 'record_byte_size')
            max_values['mp'] = max(max_values["mp"], *part_dist.values())
            max_values['mc'] = max(max_values["mc"], *count_dist.values())
            max_values['mm'] = max(max_values["mm"], *mem_dist.values())
            t['part_size'] = len(parts)
            t['part_dist'] = part_dist
            t['count_dist'] = count_dist
            t['mem_dist'] = mem_dist
            table_infos.append(t)
            self._add_merge(tablet2partition, part_dist)
            self._add_merge(tablet2count, count_dist)
            self._add_merge(tablet2mem, mem_dist)

        max_values["mm"] = round(max_values["mm"] / 1024 / 1024, 4)
        max_width = 50
        for t in table_infos:
            print()
            print(t['name'])
            print('partition size:', t['part_size'])
            print('partition dist(include replica)')
            self._show_dist(t['part_dist'], max_width=max_width * max(*t['part_dist'].values()) / max_values['mp'])
            print('record count dist(include replica)')
            self._show_dist(t['count_dist'], max_width=max_width * max(*t['count_dist'].values()) / max_values['mc'])
            print('mem dist(include replica)(MB)')
            self._byte2mb(t['mem_dist'])
            self._show_dist(t['mem_dist'], max_width=max_width * max(*t['mem_dist'].values()) / max_values['mm'])

        print()
        print('total')
        print('tablet2partition')
        self._show_dist(tablet2partition)
        print('tablet2count')
        self._show_dist(tablet2count)
        print('tablet2mem(MB)')
        self._byte2mb(tablet2mem)
        self._show_dist(tablet2mem)

    def _byte2mb(self, dist: dict):
        for k, v in dist.items():
            dist[k] = round(v / 1024 / 1024, 4)

    def _show_dist(self, dist: dict, max_width=50):
        figc = tpl.figure()
        figc.barh(list(dist.values()), labels=list(dist.keys()), max_width=max_width)
        figc.show()

    def _collect(self, parts, field):
        dist = {}
        for part in parts:
            for replica in part['partition_meta']:
                if replica['endpoint'] not in dist:
                    dist[replica['endpoint']] = 0
                dist[replica['endpoint']] += replica[field] if field else 1
        return dist

    def _add_merge(self, dist, dist2):
        for key, value in dist2.items():
            dist[key] = dist.get(key, 0) + value
        return dist

    def _get_nameserver(self):
        component_list = self.conn.execfetch("SHOW COMPONENTS")
        return list(filter(lambda l: l[1]=="nameserver", component_list))[0][0]
