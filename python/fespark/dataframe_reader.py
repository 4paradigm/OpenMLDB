
from .dataframe import FesqlDataframe
from .backend import GlobalBackend

class DataframeReader(object):

    def __init__(self, jsession):
        self.jsession = jsession

    def parquet(self, filePath):
        return FesqlDataframe(self.jsession.read(filePath, "parquet"))

    def csv(self, filePath):
        return FesqlDataframe(self.jsession.read(filePath, "csv"))

    def json(self, filePath):
        return FesqlDataframe(self.jsession.read(filePath, "json"))

    def text(self, filePath):
        return FesqlDataframe(self.jsession.read(filePath, "text"))

    def orc(self, filePath):
        return FesqlDataframe(self.jsession.read(filePath, "orc"))
