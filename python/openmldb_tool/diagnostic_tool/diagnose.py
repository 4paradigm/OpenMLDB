from diagnostic_tool.collector import Collector
from diagnostic_tool.dist_conf import DistConfReader
from diagnostic_tool.dist_conf import ConfParser
from diagnostic_tool.dist_conf import DistConf
from diagnostic_tool.dist_conf import ConfParser
from diagnostic_tool.conf_validator import YamlConfValidator
from diagnostic_tool.conf_validator import StandaloneConfValidator
from diagnostic_tool.conf_validator import ClusterConfValidator
from diagnostic_tool.conf_validator import TaskManagerConfValidator
from diagnostic_tool.log_analysis import LogAnalysis
from diagnostic_tool.server_checker import ServerChecker
import diagnostic_tool.util as util
import sys
import os
import logging
from absl import app
from diagnostic_tool.conf_option import ConfOption

LOG_FORMAT = '%(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger(__name__)

def get_standalone_version(dist_conf: DistConf):
    version_map = {}
    for role, value in dist_conf.server_info_map.map.items():
        version_map.setdefault(role, [])
        for item in value:
            version = util.get_openmldb_version(item.bin_path())
            version_map[role].append((item.host, version))
    return version_map

def check_version(version_map : dict):
    f_version = ''
    f_endpoint = ''
    f_role = ''
    flag = True
    for k, v in version_map.items():
        for endpoint, cur_version in v:
            if f_version == '':
                f_version = cur_version
                f_endpoint = endpoint
                f_role = k
            if cur_version != f_version:
                log.warn(f'version mismatch. {k} {endpoint} version {cur_version}, {f_role} {f_endpoint} version {f_version}')
                flag = False
    return flag, f_version

def get_standalone_files(dist_conf : DistConf):
    file_map = {'conf' : {}, 'log' : {}}
    for role, value in dist_conf.server_info_map.map.items():
        file_map['conf'][role] = {}
        file_map['log'][role] = {}
        for item in value:
            file_map['conf'][role].setdefault(item.endpoint, [])
            conf_file = f'standalone_{role}.flags'
            full_path = os.path.join(item.conf_path(), conf_file)
            file_map['conf'][role][item.endpoint].append((conf_file, full_path))
            detail_conf = ConfParser(full_path).conf()
            log_dir = detail_conf['openmldb_log_dir'] if 'openmldb_log_dir' in detail_conf else './logs'
            full_log_dir = log_dir if log_dir.startswith('/') else os.path.join(item.path, log_dir)
            file_map['log'][role][item.endpoint] = util.get_local_logs(full_log_dir, role)
    return file_map

def check_conf(yaml_conf_dict, conf_map):
    detail_conf_map = {}
    flag = True
    for role, v in conf_map.items():
        for endpoint, values in v.items():
            for _, path in values:
                detail_conf_map.setdefault(role, [])
                cur_conf = ConfParser(path).conf()
                detail_conf_map[role].append(cur_conf)
                if yaml_conf_dict['mode'] == 'cluster' and role == 'taskmanager':
                    taskmanager_validator = TaskManagerConfValidator(cur_conf)
                    if not taskmanager_validator.validate():
                        log.warn(f'taskmanager {endpoint} conf check failed')
                        flag = False

    if yaml_conf_dict['mode'] == 'standalone':
        conf_validator = StandaloneConfValidator(detail_conf_map['nameserver'][0], detail_conf_map['tablet'][0])
    else:
        conf_validator = ClusterConfValidator(yaml_conf_dict, detail_conf_map)
    if conf_validator.validate() and flag:
        log.info('check conf ok')
    else:
        log.warn('check conf failed')

def check_log(yaml_conf_dict, log_map):
    flag = True
    for role, v in log_map.items():
        for endpoint, values in v.items():
            log_analysis = LogAnalysis(role, endpoint, values)
            if not log_analysis.analysis_log() : flag = False
    if flag:
        log.info('check log ok')

def run_test_sql(dist_conf : DistConf):
    checker = ServerChecker(dist_conf.full_conf)
    if checker.run_test_sql():
        log.info('test sql execute ok.')

def main(argv):
    conf_opt = ConfOption()
    if not conf_opt.init():
        return

    dist_conf = DistConfReader(conf_opt.dist_conf).conf()
    yaml_validator = YamlConfValidator(dist_conf.full_conf)
    if not yaml_validator.validate():
        log.warning("check yaml conf failed")
        sys.exit()
    log.info("check yaml conf ok")

    log.info("mode is {}".format(dist_conf.mode))
    if dist_conf.mode == 'cluster':
        collector = Collector(dist_conf)
        if conf_opt.check_version():
            version_map = collector.collect_version()
        if conf_opt.check_conf():
            collector.pull_config_files(f'{conf_opt.data_dir}/conf')
        if conf_opt.check_log():
            collector.pull_log_files(f'{conf_opt.data_dir}/log')
        file_map = util.get_files(conf_opt.data_dir)
    else:
        if conf_opt.check_version():
            version_map = get_standalone_version(dist_conf)
        if conf_opt.check_conf() or conf_opt.check_log():
            file_map = get_standalone_files(dist_conf)

    if conf_opt.check_version():
        flag, version = check_version(version_map)
        if flag:
            log.info(f'openmldb version is {version}')
            log.info('check version ok')

    if conf_opt.check_conf():
        check_conf(dist_conf.full_conf, file_map['conf'])
    if conf_opt.check_log():
        check_log(dist_conf.full_conf, file_map['log'])
    if conf_opt.check_sql():
        run_test_sql(dist_conf)

def run():
    app.run(main)

if __name__ == '__main__':
    app.run(main)
