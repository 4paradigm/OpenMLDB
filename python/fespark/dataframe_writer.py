
class DataframeWriter(object):

    def __init__(self, jdf):
        self.jdf = jdf
    
    def parquet(self, path, format="parquet", mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, format, mode, renameDuplicateColumns, partitionNum)
