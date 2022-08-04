import logging

log = logging.getLogger(__name__)

class TaskManagerConfValidator:
    def __init__(self, conf_dict):
        self.conf_dict = conf_dict
        self.default_conf_dict = {
            'server.host' : '0.0.0.0',
            'server.port' : '9902',
            'zookeeper.cluster' : '',
            'zookeeper.root_path' : '',
            'spark.master' : 'local',            
            'spark.yarn.jars' : '',
            'spark.home' : '',
            'prefetch.jobid.num' : '1',
            'job.log.path' : '../log/',
            'external.function.dir' : './udf/',
            'job.tracker.interval' : '30',
            'spark.default.conf' : '',
            'spark.eventLog.dir' : '',
            'spark.yarn.maxAppAttempts' : '1',
            'offline.data.prefix' : 'file:///tmp/openmldb_offline_storage/',
        }
        self.fill_default_conf()

    def fill_default_conf(self):
        for key in self.default_conf_dict:
            if key not in self.conf_dict:
                self.conf_dict[key] = self.default_conf_dict[key]

    def check_noempty(self):
        no_empty_keys = ['zookeeper.cluster', 'zookeeper.root_path', 'job.log.path', 'external.function.dir', 'offline.data.prefix']
        valid = True
        for item in no_empty_keys:
            if self.conf_dict[item] == '':
                log.warning(f'{item} should not be empty')
                valid = False
        return valid

    def check_port(self):
        port = int(self.conf_dict['server.port'])
        if port < 1 or port > 65535:
            log.warning('port should be in range of 1 through 65535')

    def check_spark(self):
        spark_master = self.conf_dict['spark.master'].lower()
        is_local = spark_master.startswith('local')
        if not is_local and spark_master not in ['yarn', 'yarn-cluster', 'yarn-client']:
            log.warning('spark.master should be local, yarn, yarn-cluster or yarn-client')
        if spark_master.startswith('yarn'):
            if self.conf_dict['spark.yarn.jars'].startswith('file://'):
                log.warning('spark.yarn.jars should not use local filesystem for yarn mode')
            if self.conf_dict['spark.eventLog.dir'].startswith('file://'):
                log.warning('spark.eventLog.dir should not use local filesystem for yarn mode')
            if self.conf_dict['offline.data.prefix'].startswith('file://'):
                log.warning('offline.data.prefix should not use local filesystem for yarn mode')

        spark_default_conf = self.conf_dict['spark.default.conf']
        if spark_default_conf != '':
            spark_jars = spark_default_conf.split(';')
            for spark_jar in spark_jars:
                if spark_jar != '':
                    kv = spark_jar.split('=')
                    if len(kv) < 2:
                        log.warning(f'spark.default.conf error format of {spark_jar}')
                    elif not kv[0].startswith('spark'):
                        log.warning(f'spark.default.conf config key should start with \'spark\' but get {kv[0]}')

        if int(self.conf_dict['spark.yarn.maxAppAttempts']) < 1:
            log.warning('spark.yarn.maxAppAttempts should be larger or equal to 1')

    def check_job(self):
        if int(self.conf_dict['prefetch.jobid.num']) < 1:
            log.warning('prefetch.jobid.num should be larger or equal to 1')
        jobs_path = self.conf_dict['job.log.path']
        if jobs_path.startswith('hdfs') or jobs_path.startswith('s3'):
            log.warning('job.log.path only support local filesystem')
        if int(self.conf_dict['job.tracker.interval']) <= 0:
            log.warning('job.tracker.interval interval should be larger than 0')

    def validate(self):
        self.check_noempty()
        self.check_port()
        self.check_spark()
        self.check_job()
        return True
