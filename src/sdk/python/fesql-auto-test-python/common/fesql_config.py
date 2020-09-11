#! /usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
import util.tools as tool

config = configparser.ConfigParser()
confPath = tool.getAbsolutePath("fesql.conf")
config.read(confPath)
lists_header = config.sections()  # 配置组名, ['luzhuo.me', 'mysql'] # 不含'DEFAULT'
env=config['test']['env']
zk_cluster = config['fesql'][env+'_zk_cluster']
zk_root_path = config['fesql'][env+'_zk_root_path']
