import ConfigParser
import string
import os
import sys


cf = ConfigParser.ConfigParser()
cf.read(os.getenv("testconfpath"))

failfast = cf.getboolean("test_opt", "failfast")

multidimension = cf.getboolean("dimension", "multidimension")
multidimension_vk = eval(cf.get("dimension", "multidimension_vk"))
multidimension_scan_vk = eval(cf.get("dimension", "multidimension_scan_vk"))

log_level = cf.get("log", "log_level")

tb_endpoints = cf.items("tb_endpoints")

ns_endpoints = cf.items("ns_endpoints")

zk_endpoint = cf.get("zookeeper", "zk")

table_meta_ele = {
    'table_partition': ['endpoint', 'pid_group', 'is_leader'],
    'column_desc': ['name', 'type', 'add_ts_idx']
}