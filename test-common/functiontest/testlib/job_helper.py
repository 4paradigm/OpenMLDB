# -*- coding:utf-8 -*-
"""

"""
import os
import sys
import inspect
import hashlib
import random
import string
import time
import atest.log as log
import util as util
from common_utils.process import Process
sys.path.append(os.path.dirname(util.get('RTIDB_CLIENT_PY')))
from rtidb_client import RtidbClient

CTIME = int(time.time())
NAME_GEN= lambda stack: util.inspect_get(stack, 'function')


class ClientContext(object):
    """

    """
    name =  lambda self, stack: NAME_GEN(stack)
    tid =   lambda self, stack: int(''.join(filter(str.isdigit, hashlib.md5(NAME_GEN(stack)).hexdigest()))[6])
    pid =   lambda self, pid: pid
    pk =    lambda self, pk: pk
    time =  lambda self, idx: CTIME + idx
    stime = lambda self: CTIME
    etime = lambda self, idx: CTIME + idx
    ttl =   lambda self: 0
    value = lambda self: ''.join(random.sample(string.lowercase+string.digits, 20))
    data_ha = lambda self: False


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
        argsList = inspect.getargspec(taskFunc).args
        argsList.remove('self')
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
            self.input.append(taskParam)

    def run(self, failonerror=True):
        """

        :return:
        """
        ret = True
        for idx, (taskFunc, taskParam) in enumerate(self.taskList):
            try:
                msg = 'Func=%s, Param=%s' % (str(taskFunc),
                                             str(taskParam))
                log.info(msg)
                ret = taskFunc(**taskParam)
                ret = False if 0 == ret else True if 1 == ret else ret
            except Exception as e:
                msg = 'Task start fail, ' \
                      'message=%s, ' \
                      'args=%s, ' \
                      'param=%s' % (e.message, str(e.args), str(taskParam))
                raise Exception(msg)
            if self.rtidbClient.scan == taskFunc:
                self._scan_parser(ret)
            else:
                if not ret:
                    if failonerror or idx != (len(self.taskList) - 1):
                        raise Exception('Task run error!')
                    else:
                        return False
        return ret

    def input_message(self):
        """

        :return:
        """
        return self.input

    def scanout_message(self):
        """

        :return:
        """
        return self.output

    def identify(self, base, compare, inputJunkFunc=None, scanoutJunkFunc=None):
        """

        :param baseMap:
        :param compareMap:
        :param junkFunc:
        :return:
        """
        retStatus = False
        retMsg = None
        self.identified = True

        base = filter(inputJunkFunc, base) if inputJunkFunc is not None else base
        compare = filter(scanoutJunkFunc, compare) if scanoutJunkFunc is not None else compare

        for i in range(len(base)):
            keys = base[i].keys()
            for key in keys:
                if key in ['value', 'pk']:
                    continue
                elif 'time' == key:
                    base[i]['pk'] = base[i].pop(key)
                else:
                    base[i].pop(key)
        base = sorted(base, key=lambda k: k['pk'], reverse=True)
        retStatus = True if base == compare else False
        retMsg = 'Expect:%s, Actual:%s' % (str(base), str(compare))

        return retStatus, retMsg

    def _scan_parser(self, it):
        """

        :param it:
        :return:
        """
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
        self.input = []
        self.output = []

        self.rtidbClient = RtidbClient(endpoint=util.get('ENDPOINT'))
        self.append(self.rtidbClient.drop_table)
        self.run(failonerror=False)

        self.taskList = []
        self.input = []
        self.output = []
        self.pk = ''.join(random.sample(string.lowercase+string.digits, 20))
        self.identified = False

    def __del__(self):
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
        logTokens = ['error', 'warn']
        logfile = util.get('RTIDB_LOG')
        for token in logTokens:
            cmd = 'grep -i %(token)s %(logfile)s' % locals()
            data, error, retCode = proc.run(cmd)
            if 0 != retCode:
                raise Exception('Tablet server run %(token)s: %(data)s' % locals())
        # 3, identify
        if not self.identified:
            self.identify(self.input_message(), self.scanout_message())


