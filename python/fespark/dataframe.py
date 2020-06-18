
from .dataframe_writer import DataframeWriter

class FesqlDataframe(object):

    def __init__(self, jdf):
        self.jdf = jdf

    def createOrReplaceTempView(self, name):
        self.jdf.createOrReplaceTempView(name)

    @property
    def write(self):
        return DataframeWriter(self.jdf)

    def run(self):
        self.jdf.run()

    def show(self):
        self.jdf.show()

    def count(self):
        return self.jdf.count()

    def sample(self, fraction, seed=None):
        if seed is None:
            return FesqlDataframe(self.jdf.sample(fraction))
        else:
            return FesqlDataframe(self.jdf.sample(fraction, seed))

    def describe(self, *cols):
        return FesqlDataframe(self.jdf.describe(cols))

    def explain(self, extended=False):
        self.jdf.explain(extended)

    def summary(self):
        return FesqlDataframe(self.jdf.summary())

    def cache(self):
        return FesqlDataframe(self.jdf.cache())

    def __str__(self):
        return self.jdf.toString()

    def toPandas(self):
        pass