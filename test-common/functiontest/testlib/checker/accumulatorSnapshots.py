# -*- coding:utf-8 -*-
"""#TODO
"""
from util import CheckerBase


class AccumulatorSnapShots(CheckerBase):
    """

    """
    ID = 'COMMON_CHECKER_PROPHET_SCHEMA'
    def __init__(self, id, sourceMap, appName, verInfo):
        """

        :param id:
        :param dataMap:
        :param appName:
        :param jenkinsJob:
        :param jenkinsJobId:
        """
        super(AccumulatorSnapShots, self).__init__(id, sourceMap, appName, verInfo)
        self.status = (self._ver(verInfo) > self._ver("PICO-RB_0_1_0"))

    def checker(self):
        """
        JSONPath exammples: http://goessner.net/articles/JsonPath/
        :return:
        """
        if self.appName in ["lr", "fm", "linear-fractal"]:
            return self._isinstance('$.AccumulatorSnapshots[*].PicoAccumulator.progressive_auc', float) and \
                   self._isinstance('$.AccumulatorSnapshots[*].PicoAccumulator.progressive_logloss', float) and  \
                   self._isinstance('$.AccumulatorSnapshots[*].PicoAccumulator.trained_pass', int)
        elif self.appName in ["gbm"]:
            return self._isinstance('$.AccumulatorSnapshots[*].PicoAccumulator.progressive_auc', float) and \
                   self._isinstance('$.AccumulatorSnapshots[*].PicoAccumulator.progressive_logloss', float)
        elif self.appName in ["svm"]:
            return self._isinstance('$.AccumulatorSnapshots[*].PicoAccumulator.progressive_auc', float) and \
                  self._isinstance('$.AccumulatorSnapshots[*].PicoAccumulator.progressive_hingeloss', float) and \
                  self._isinstance('$.AccumulatorSnapshots[*].PicoAccumulator.trained_pass', int)

