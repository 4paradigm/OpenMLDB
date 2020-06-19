
class DataframeWriter(object):

    def __init__(self, jdf):
        self.jdf = jdf
    
    def parquet(self, path, mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, "parquet", mode, renameDuplicateColumns, partitionNum)

    def csv(self, path, mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, "csv", mode, renameDuplicateColumns, partitionNum)

    def json(self, path, mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, "json", mode, renameDuplicateColumns, partitionNum)

    def text(self, path, mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, "text", mode, renameDuplicateColumns, partitionNum)

    def orc(self, path, mode="overwrite", renameDuplicateColumns=True, partitionNum=-1):
        self.jdf.write(path, "orc", mode, renameDuplicateColumns, partitionNum)
