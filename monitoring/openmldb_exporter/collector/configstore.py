# Copyright 2022 4Paradigm
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
"""
exporter configuration parse and management
"""

import argparse
import logging


class ConfigStore(object):
    '''
    class to init a ArgumentParser and store all exporter configurations
    '''

    _args: argparse.Namespace
    log_level: str
    zk_root: str
    zk_path: str
    listen_port: int
    telemetry_path: str
    pull_interval: float

    def __init__(self):
        parser = argparse.ArgumentParser(description="OpenMLDB exporter")
        parser.add_argument("--log.level", type=str, default="WARNING", help="config log level")
        parser.add_argument("--web.listen-address", type=int, default=8000, help="process listen port")
        parser.add_argument("--web.telemetry-path",
                            type=str,
                            default="metrics",
                            help="Path under which to expose metrics")
        parser.add_argument("--config.zk_root", type=str, default="127.0.0.1:6181", help="endpoint to zookeeper")
        parser.add_argument("--config.zk_path", type=str, default="/", help="root path in zookeeper for OpenMLDB")
        parser.add_argument("--config.interval",
                            type=float,
                            default=30.0,
                            help="interval in seconds to pull metrics periodically")
        self._args = parser.parse_args()
        self._store_cfgs()

    def _get_cfg(self, key: str):
        '''
        key fetching that handles key whose value may contains literal dot('.')
        '''
        val = self._args.__dict__.get(key)
        if val is None:
            raise KeyError(f"value for {key} not exist")
        return val

    def _store_cfgs(self):
        self.log_level = self._get_cfg("log.level")
        self.zk_root = self._get_cfg("config.zk_root")
        self.zk_path = self._get_cfg("config.zk_path")
        self.listen_port = self._get_cfg("web.listen_address")
        self.telemetry_path = self._get_cfg("web.telemetry_path")
        self.pull_interval = self._get_cfg("config.interval")

    def get_log_level(self):
        numeric_level = getattr(logging, self.log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {self.log_level}")
        return numeric_level
