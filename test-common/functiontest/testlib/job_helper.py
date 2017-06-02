# -*- coding:utf-8 -*-
"""

"""
import os
import sys
import inspect
import copy
import hashlib
import random
import string
import time
import atest.log as log
import util as util
from common_utils.file_util import FileUtils
from common_utils.process import Process
sys.path.append(os.path.dirname(util.get('RTIDB_CLIENT_PY')))
from rtidb_client import RtidbClient

CTIME = int(time.time())
NAME_GEN= lambda stack: util.inspect_get(stack, 'function')


class ClientContext(object):
    """

    """
    name =  lambda self, stack: NAME_GEN(stack)
    tid =   lambda self, stack: int(''.join(filter(str.isdigit, hashlib.md5(NAME_GEN(stack)).hexdigest()))[6]) + 1
    pid =   lambda self, pid: pid
    pk =    lambda self, pk: pk
    time =  lambda self, idx: CTIME + idx
    stime = lambda self, idx: CTIME + idx
    etime = lambda self: CTIME - 1
    ttl =   lambda self: 0
    value = lambda self: ''.join(random.sample(string.lowercase+string.digits, 20))
    data_ha = lambda self: False
    seconds = 1


class JobHelper(object):
    """
    """
    def __init__(self):
        """

        """
        self.rtidbClient = None
        self.input = None
        self.output = None

        self.idx = 0
        self.pid = random.randint(1, 1000000)
        self.pk = ''.join(random.sample(string.lowercase+string.digits, 20))
        self.taskList = None
        self.stack = inspect.stack()

        self._init_prepare()

    def append(self, taskFunc, **param):
        """

        :param taskFunc:
        :param param:
        :return:
        """
        seedMap = {'stack': self.stack,
                   'idx': self.idx,
                   'pk': self.pk,
                   'pid': self.pid,
                   }

        context = ClientContext()
        sub_dict = lambda data, keys: dict(filter(lambda i: i[0] in keys,
                                                  data.iteritems()))
        try:
            argsList = inspect.getargspec(taskFunc).args
            argsList.remove('self')
        except:
            argsList = []
        argsVals = []
        for attr in argsList:
            attrFunc = getattr(context, attr)
            attrParam = inspect.getargspec(attrFunc).args
            attrParam.remove('self')
            attrParam = sub_dict(seedMap, attrParam)
            argsVals.append(attrFunc(**attrParam))

        taskParam = dict(zip(argsList, argsVals))
        taskParam.update(param)

        self.taskList.append([taskFunc, taskParam])
        if self.rtidbClient.put == taskFunc:
            self.idx += 1
            self.input = [] if self.input is None else self.input
            self.input.append(taskParam)

    def run(self, failonerror=True, autoidentity=True, logcheck=True):
        """

        :return:
        """
        retStatus = True
        for idx, (taskFunc, taskParam) in enumerate(self.taskList):
            try:
                ret = taskFunc(**taskParam)
                msg = 'Func=%s, Param=%s, ret=%s' % (str(taskFunc),
                                                     str(taskParam),
                                                     str(ret))
                log.info(msg[:1000])
                # ret = False if 0 == ret else True if 1 == ret else ret
            except Exception as e:
                    msg = 'Task start fail, ' \
                      'message=%s, ' \
                      'args=%s, ' \
                      'param=%s' % (e.message, str(e.args), str(taskParam))
                    log.info(msg)
                    if failonerror:
                        raise Exception(msg)
                    else:
                        retStatus = False
            if self.rtidbClient.scan == taskFunc:
                self._scan_parser(ret)
            else:
                if not ret:
                    if failonerror:
                        raise Exception('%s run error:%s' % (str(taskFunc),
                                                             str(taskParam)))
                    else:
                        retStatus = False
        # common check
        self._common_check(autoidentity, retStatus, logcheck)

        # save log file
        logfile = util.get('RTIDB_LOG')
        logfileBackup = logfile + '.' + util.inspect_get(self.stack, 'function')
        FileUtils.cp(logfile, logfileBackup)

        return retStatus

    def input_message(self):
        """

        :return:
        """
        return copy.deepcopy(self.input)

    def scanout_message(self):
        """

        :return:
        """
        return copy.deepcopy(self.output)

    def identify(self, input, scanout, inputJunkFunc=None, scanoutJunkFunc=None):
        """

        :param baseMap:
        :param compareMap:
        :param junkFunc:
        :return:
        """
        retStatus = False
        retMsg = None

        if all(x is None for x in [input,
                                   scanout,
                                   inputJunkFunc,
                                   scanoutJunkFunc]):
            retStatus = True
        else:
            input = filter(inputJunkFunc, input) if inputJunkFunc is not None else input
            scanout = filter(scanoutJunkFunc, scanout) if scanoutJunkFunc is not None else scanout

            for i in range(len(input)):
                keys = input[i].keys()
                for key in keys:
                    if key in ['value', 'pk']:
                        continue
                    elif 'time' == key:
                        input[i]['pk'] = input[i].pop(key)
                    else:
                        input[i].pop(key)
            input = sorted(input, key=lambda k: k['pk'], reverse=True)
            retStatus = True if input == scanout else False
        retMsg = 'Input:%s, Scanout:%s' % (str(input), str(scanout))
        log.info(retMsg)

        return retStatus, retMsg

    def sleep(self, seconds):
        """

        :return:
        """
        time.sleep(seconds)

    def _scan_parser(self, it):
        """

        :param it:
        :return:
        """
        self.output = []
        while it.valid():
            self.output.append({'pk': it.get_key(),
                                'value': it.get_value(),
                                })
            it.next()

    def _init_prepare(self):
        """

        :return:
        """
        self.taskList = []

        proc = Process()
        for cmd in ['CMD_TABLE_STOP', 'CMD_TABLE_START']:
            cmd = util.get(cmd)
            proc.run(cmd)
        self.rtidbClient = RtidbClient(endpoint=util.get('ENDPOINT'))
        self.append(self.rtidbClient.drop_table)
        self.run(failonerror=False, autoidentity=False)

        self.taskList = []
        self.pk = ''.join(random.sample(string.lowercase+string.digits, 20))

    def _common_check(self, autoidentity, retStatus, logcheck):
        """

        :return:
        """
        # common check
        # 1, pid exist
        cmd = util.get('CMD_TABLE_PS')
        proc = Process()
        data, error, retCode = proc.run(cmd)
        if 0 != retCode:
            raise Exception('Tablet server status error.')
        # 2, warn/error log check
        if retStatus and logcheck:
            logTokens = ['E ', 'W ']
            logfile = util.get('RTIDB_LOG')
            for token in logTokens:
                cmd = 'grep -E "%(token)s" %(logfile)s' % locals()
                data, error, retCode = proc.run(cmd)
                if 0 == retCode:
                    raise Exception('Tablet server logchecker  %(token)s: %(data)s' % locals())
        # 3, identify
        if autoidentity and not retStatus:
            retStatus, retMsg = self.identify(self.input_message(),
                                              self.scanout_message())
            if not retStatus:
                raise Exception('Identify check error:%(retMsg)s' % locals())


