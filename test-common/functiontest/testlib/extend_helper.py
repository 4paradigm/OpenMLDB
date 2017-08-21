# -*- coding:utf-8 -*-
import os
import re
import json
import hashlib
import difflib
import pprint
from common_utils import yaml
from common_utils.file_util import FileUtils
from common_utils.process import Process
import util as util

BASE_BIN_DIR = os.path.join(util.get('PICO_ROOT'), 'build')
COMPARE_BIN_DIR = util.get('JENKINS_VER0_BIN_DIR')


class DiffFunc(object):
    """

    """
    @staticmethod
    def json_diff(baseFile, compareFile, baseJunkJpaths=None, compareJunkJpaths=None):
        """

        :param baseFile:
        :param compareFile:
        :param baseJunkJpaths:
        :param compareJunkJpaths:
        :return:
        """
        def _parse(dataFile, dataJunkPaths):
            dataJunkPaths= [] if dataJunkPaths is None else dataJunkPaths
            baseMap = ParseFunc.json(dataFile)
            for jpath in dataJunkPaths:
                util.jpath_unset(dataFile, jpath)
            return baseMap
        baseMap = _parse(baseFile, baseJunkJpaths)
        compareMap = _parse(compareFile, compareJunkJpaths)
        diff = difflib.unified_diff(
            pprint.pformat(baseMap, indent=4).splitlines(1),
            pprint.pformat(compareMap, indent=4).splitlines(1),
            fromfile=baseFile,
            tofile=compareFile)
        retMsg = ''.join(diff)
        return '' == retMsg, '\n%s' % retMsg

    @staticmethod
    def file_diff(baseFile, compareFile, baseJunkFunc=None, compareJunkFunc=None):
        """

        :param baseFile:
        :param compareFile:
        :param baseJunkFunc:
        :param compareJunkFunc:
        :return:
        """
        def _parse(dataFile, dataJunkFunc):
            if dataJunkFunc is not None:
                return dataJunkFunc(dataFile)
            else:
                return FileUtils.read(dataFile)
        baseStr = _parse(baseFile, baseJunkFunc)
        compareStr = _parse(compareFile, compareJunkFunc)
        diff = difflib.unified_diff(
            baseStr.splitlines(1),
            compareStr.splitlines(1),
            fromfile=baseFile,
            tofile=compareFile)
        retMsg = ''.join(diff)
        return '' == retMsg, '\n%s' % retMsg


class ParseFunc(object):
    """

    """
    @staticmethod
    def json(sourceFile):
        return json.load(open(sourceFile))

    @staticmethod
    def yaml(sourceFile):
        return yaml.load(FileUtils.read(sourceFile))

    @staticmethod
    def model(sourceFile):
        sourceMap = FileUtils.read2(sourceFile)
        sourceMap = filter(lambda x: x.strip() != '', sourceMap)
        sourceMap = [f.split(' ') for f in sourceMap]
        return sourceMap

    @staticmethod
    def predict(sourceFile):
        sourceMap = FileUtils.read2(sourceFile)
        sourceMap = filter(lambda x: x.strip() != '', sourceMap)
        sourceMap = [f.split(' ') for f in sourceMap]
        return sourceMap

    @staticmethod
    def md5(sourceFile):
        return hashlib.md5(open(sourceFile, 'rb').read()).hexdigest()


class PicoRunner(object):
    """

    """
    @staticmethod
    def local_wrapper(contextMap):
        """

        :param picoDir:
        :return:
        """
        cmd = (
            '%(wrapperBin)s '
            '%(appBin)s '
            '--config_file=%(yamlFile)s'
        )
        cmd = cmd % contextMap
        proc = Process()
        data, error, retCode = proc.run(cmd, timeout=300)
        return cmd, data, error, retCode

    @staticmethod
    def yarn_wrapper(contextMap):
        """

        :param picoDir:
        :return:
        """
        cmd = (
            '%(yarnBin)s jar %(hadoopPatchJar)s com.tfp.hadoop.yarn.launcher.Client '
            '--appname %(jobName)s '
            '--jar %(hadoopPatchJar)s '
            '--shell_command "./pico_yarn_runner.sh --enable-hdfs-proxy ./%(appName)s pico.yaml" '
            '--container_memory=4000 '
            '--num_containers=3 '
            '--shell_env HADOOP_USER_NAME=root '
            '--shell_env HADOOP_HOME=%(hadoopHome)s '
            '--shell_env WEBHDFS_USER=root'
            '--shell_env WEBHDFS_HDFS=%(hdfsBin)s '
            '--file %(appBin)s '
            '--file %(wrapperBin)s '
            '--file %(yamlFile)s '
            '--file %(hdfsproxyClient)s '
            '--file %(picoYarnRunnerSh)s '
            '--file %(hdfsProxyJar)s '
        )
        wrapperBin = contextMap['contextMap']
        appBin = contextMap['appBin']
        yamlFile = contextMap['yamlFile']
        appName = os.path.basename(appBin)
        jobName = 'PTest_' + appBin + '_'.join(yamlFile.split('/')[-4:-1])
        hadoopHome = util.get('HADOOP_HOME')
        yarnBin = os.path.join(hadoopHome, 'bin/yarn')
        hdfsBin = os.path.join(hadoopHome, 'bin/hdfs')
        picoYarnRunnerSh = os.path.join(util.get('TESTLIB_DIR'), 'script/pico_yarn_runner.sh')
        hadoopPatchJar = os.path.join(util.get('DEPEND_DIR'), 'hadoop-patch.jar')
        hdfsproxyClient = os.path.join(util.get('DEPEND_DIR'), 'hdfsproxy-client')
        hdfsProxyJar = os.path.join(util.get('DEPEND_DIR'), 'hdfs-proxy.jar')
        cmd = cmd % locals()
        proc = Process()
        data, error, retCode = proc.run(cmd, timeout=600)
        appId = re.findall(r"appId = application_[0-9]+_[0-9]+", data + error)[0]
        appId = appId.split('=')[-1]
        cmdLog = 'yarn logs -applicationId %(appId)s' % locals()
        data, error, _ = proc.run(cmdLog)
        return cmd, data, error, retCode

    @staticmethod
    def mpi_wrapper(contextMap):
        """

        :return:
        """
        cmd = (
            '%(mpirunBin)s '
            '-np %(np)s '
            '--allow-run-as-root '
            '%(wrapperBin)s '
            '%(appBin)s '
            '--config_file=%(yamlFile)s '
        )
        wrapperBin = contextMap['contextMap']
        appBin = contextMap['appBin']
        yamlFile = contextMap['yamlFile']
        np = contextMap['np']
        mpirunBin = os.path.join(util.get('TESTLIB_DIR'), 'bin/mpirun')
        cmd = cmd % locals()
        proc = Process()
        return proc.runXP(cmd)

