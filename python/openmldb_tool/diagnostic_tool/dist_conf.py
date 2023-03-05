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
import re
from absl import logging
from absl import flags
import configparser as cfg
import yaml
from . import util

ALL_SERVER_ROLES = ["nameserver", "tablet", "apiserver", "taskmanager"]

CXX_SERVER_ROLES = ALL_SERVER_ROLES[:3]

JAVA_SERVER_ROLES = [ALL_SERVER_ROLES[3]]

COMPONENT_ROLES = [ALL_SERVER_ROLES[0], ALL_SERVER_ROLES[1], ALL_SERVER_ROLES[3]]

# dest conf path is <collect_dir>/<server-name>/<confdir>/
CONFDIR = "conf/"
# dest log path is <collect_dir>/<server-name>/<logdir>/
LOGDIR = "logs/"

flags.DEFINE_string("default_dir", "/work/openmldb", "OPENMLDB_HOME, if no path field in dist conf file, use this path")


class ServerInfo:
    def __init__(self, role, endpoint, path, is_local):
        self.role = role
        self.endpoint = endpoint
        # path shouldn't have `:`, replace it in endpoint
        self.endpoint_str = endpoint.replace(":","-")
        self.path = path
        self.host = endpoint.split(":")[0]
        self.is_local = is_local

    def __str__(self):
        return f"Server[{self.role}, {self.endpoint}, {self.path}]"

    def is_taskmanager(self):
        return self.role == "taskmanager"

    def conf_path(self):
        return f"{self.path}/conf"

    def bin_path(self):
        return f"{self.path}/bin"

    def root_path(self):
        return f"{self.path}/taskmanager/bin" if self.is_taskmanager() else self.path

    def taskmanager_path(self):
        return f"{self.path}/taskmanager"

    def server_dirname(self):
        return f"{self.endpoint_str}-{self.role}"
    
    def conf_filename(self):
        return f"{self.role}.flags" if self.role != "taskmanager" else f"{self.role}.properties"

    def conf_path_pair(self, local_root):
        config_name = self.conf_filename()
        local_prefix = self.server_dirname()
        return f"{self.path}/conf/{config_name}", f"{local_root}/{local_prefix}/{CONFDIR}"

    def remote_log4j_path(self):
        return f"{self.path}/taskmanager/conf/log4j.properties"

    # TODO(hw): openmldb glog config? will it get a too large log file? fix the settings
    def remote_local_pairs(self, rel_dir, dest, dest_suffix):
        """
        log path may be changed, so we use this general func to gen copy path pairs
        rel_dir is relative to the root path, cxx server is <path>, taskmanager is <path>/taskmanager/bin
        """
        return f"{self.root_path()}/{rel_dir}", f"{dest}/{self.endpoint_str}-{self.role}/{dest_suffix}"

    def cmd_on_host(self, cmd):
        if self.is_local:
            out, rc = util.local_cmd(cmd)
            if rc:
                logging.warning(f"run cmd in local failed, cmd {cmd}, rc {rc}, out {out}")
            return out, rc
        else:
            _, stdout, stderr = util.SSH().exec(self.host, cmd)
            rc = 0
            if stderr:
                rc = -1
                logging.warning(f"run cmd in remote failed, cmd{cmd}, err {util.buf2str(stderr)}")
            return util.buf2str(stdout), rc
    
    def smart_cp(self, src, dest, src_is_dir=False, filter="") -> bool:
        """
        src file or src dir(not recursive) to dest dir, filter only works when src_is_dir==True
        If need cp recursively, try pysftp
        """
        # ensure dest path is exists
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if self.is_local:
            cmd = f"cp {src} {dest}"
            if src_is_dir:
                filter_cmd = f"ls {src} | grep -E '{filter}' -q"
                res, rc = util.local_cmd(filter_cmd)
                if rc:
                    logging.warning("no match files")
                    return False
                cmd = f"ls {src} | grep -E '{filter}' | xargs -I {{}} cp {src}/{{}} {dest}"
            logging.debug(f"cp dir cmd: {cmd}")
            res, rc = util.local_cmd(cmd)
            if rc:
                logging.warning(f"cp failed on {self}: {src}->{dest}(cmd:{cmd}), return code {rc}, err: {res}")
                return False
        else:
            sftp = util.SSH().get_sftp(self.host)
            try:
                # src path must be a file, not a dir
                file_list = src
                if src_is_dir:
                    rx = re.compile(filter)
                    src_dir = os.path.normpath(src)
                    # if src dir doesn't exist, let it crash
                    files = [attr.__dict__ for attr in sftp.listdir_attr(src_dir)]
                    file_list = [f"{src}/{f}" for f in files if rx.match(f["filename"])]
                    # TODO get last n
                    # sort by modify time
                    # logs.sort(key=lambda x: x["st_mtime"], reverse=True)
                    # log.debug("all_logs(sorted): %s", logs)
                    # logs = [log_attr["filename"] for log_attr in logs[:last_n]]
                    # log.debug("get last %d: %s", last_n, logs)
                logging.debug(f"cp files: {file_list}")
                for f in file_list:
                    sftp.get(f, dest)
            except Exception as e:
                logging.warning(f"cp failed on {self}: {src}->{dest}, err: {e}")
                return False
            finally:
                sftp.close()
        return True

class ServerInfoMap:
    def __init__(self, server_info_map):
        # map struct: <role,[server_list]>
        self.map = server_info_map

    def items(self):
        return self.map.items()

    def for_each(self, func, roles=None, check_result=True):
        """
        even some failed, call func for all
        :param roles:
        :param func:
        :param check_result:
        :return:
        """
        if roles is None:
            roles = ALL_SERVER_ROLES
        ok = True
        for role in roles:
            if role not in self.map:
                logging.warning("role %s is not in map", role)
                ok = False
                continue
            for server_info in self.map[role]:
                res = func(server_info)
                if check_result and not res:
                    ok = False
        return ok


class DistConf:
    def __init__(self, conf_dict: dict):
        self.full_conf = conf_dict
        self.mode = self.full_conf["mode"]
        self.server_info_map = ServerInfoMap(
            self._map(
                ALL_SERVER_ROLES + ["zookeeper"],
                lambda role, s: ServerInfo(
                    role,
                    s["endpoint"],
                    s["path"] if "path" in s and s["path"] else flags.FLAGS.default_dir,
                    flags.FLAGS.local or (s["is_local"] if "is_local" in s else False),
                ),
            )
        )

        # if "zookeeper":
        # endpoint = server.endpoint.split('/')[0]
        # # host:port:zk_peer_port:zk_election_port
        # endpoint = ':'.join(endpoint.split(':')[:2])

    def is_cluster(self):
        return self.mode == "cluster"

    def __str__(self):
        return str(self.full_conf)

    def _map(self, role_list, trans):
        result = {}
        for role in role_list:
            if role not in self.full_conf:
                continue
            ss = self.full_conf[role]
            if ss:
                result[role] = []
                for s in ss:
                    result[role].append(trans(role, s) if trans is not None else s)
        return result

    def count_dict(self):
        d = {r: len(s) for r, s in self.server_info_map.items()}
        assert not self.is_cluster() or d["zookeeper"] >= 1
        return d


class YamlConfReader:
    def __init__(self, config_path):
        with open(config_path, "r") as stream:
            self.dist_conf = DistConf(yaml.safe_load(stream))

    def conf(self):
        return self.dist_conf


class HostsConfReader:
    def __init__(self, config_path):
        with open(config_path, "r") as stream:
            # hosts style to dict
            cf = cfg.ConfigParser(strict=False, delimiters=" ", allow_no_value=True)
            cf.read_file(stream)
            d = {}
            for sec in cf.sections():
                # k is endpoint, v is path or empty, multi kv means multi servers
                d[sec] = [{"endpoint": k, "path": v} for k, v in cf[sec].items()]

            d["mode"] = "cluster"
            self.dist_conf = DistConf(d)

    def conf(self):
        return self.dist_conf


def read_conf(conf_file):
    """if not yaml style, hosts style"""
    try:
        conf = YamlConfReader(conf_file).conf()
    except Exception as e:
        logging.debug(f"yaml read failed on {e}, read in hosts style")
        conf = HostsConfReader(conf_file).conf()
    return conf
