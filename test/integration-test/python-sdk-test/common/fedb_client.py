
import sqlalchemy as db
from nb_log import LogManager

log = LogManager('fedb-sdk-test').get_logger_and_add_handlers()


class FedbClient:

    def __init__(self, zkCluster, zkRootPath, dbName='test_fedb'):
        self.zkCluster = zkCluster
        self.zkRootPath = zkRootPath
        self.dbName = dbName

    def getConnect(self):
        engine = db.create_engine('openmldb://@/{}?zk={}&zkPath={}'.format(self.dbName, self.zkCluster, self.zkRootPath))
        connect = engine.connect()
        return connect
