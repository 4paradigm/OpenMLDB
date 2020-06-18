
from .dataframe import FesqlDataframe
from .backend import GlobalBackend

class DataframeReader(object):

    def __init__(self, jsession):
        self.jsession = jsession

    def parquet(self, filePath, format="parquet"):
        return FesqlDataframe(self.jsession.read(filePath, format))
