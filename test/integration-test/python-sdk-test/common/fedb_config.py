#! /usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
import util.tools as tool

config = configparser.ConfigParser()
confPath = tool.getAbsolutePath("conf/fedb.conf")
config.read(confPath)
lists_header = config.sections()  # 配置组名, ['test', 'mysql'] # 不含'DEFAULT'
env = config['global']['env']
default_db_name = config['global']['default_db_name']
levels = config['global']['levels'].split(",")
levels = list(map(lambda l: int(l), levels))

zk_cluster = config['fedb'][env + '_zk_cluster']
zk_root_path = config['fedb'][env + '_zk_root_path']
tb_endpoint_0 = config['fedb'][env + '_tb_endpoint_0']
tb_endpoint_1 = config['fedb'][env + '_tb_endpoint_1']
tb_endpoint_2 = config['fedb'][env + '_tb_endpoint_2']
