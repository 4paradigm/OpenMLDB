import yaml

from common import fedb_config
from common.fedb_client import FedbClient
from util import tools

rootPath = tools.getRootPath()
from nb_log import LogManager

log = LogManager('python-sdk-test').get_logger_and_add_handlers()


class FedbTest:

    def setup_class(self):
        self.client = FedbClient(fedb_config.zk_cluster, fedb_config.zk_root_path, fedb_config.default_db_name)
        self.connect = self.client.getConnect()
        try:
            self.connect.execute("create database {};".format(fedb_config.default_db_name))
            log.info("create db:" + fedb_config.default_db_name + ",success")
        except Exception as e:
            log.info("create db:" + fedb_config.default_db_name + ",failed . msg:"+str(e))