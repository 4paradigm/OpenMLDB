from absl import flags
import logging

log = logging.getLogger(__name__)

FLAGS = flags.FLAGS
flags.DEFINE_string('dist_conf', '', 'the path of yaml conf')
flags.DEFINE_string('data_dir', '/tmp/diagnose_tool_data', 'the dir of data')
flags.DEFINE_string('check', 'ALL', 'the item should be check. one of ALL/CONF/LOG/SQL/VERSION')
flags.DEFINE_string('exclude', '', 'one of CONF/LOG/SQL/VERSION')

class ConfOption:
    def __init__(self):
        self.all_items = ['ALL', 'CONF', 'LOG', 'SQL', 'VERSION']
        self.check_items = []

    def init(self) -> bool:
        if FLAGS.dist_conf == '':
            log.warn('dist_conf option should be setted')
            return False
        self.dist_conf = FLAGS.dist_conf
        self.data_dir = FLAGS.data_dir
        check = FLAGS.check.upper()
        if check not in self.all_items:
            log.warn('the value of check should be ALL/CONF/LOG/SQL/VERSION')
            return False
        exclude = FLAGS.exclude.upper()
        if exclude != '' and exclude not in self.all_items[1:]:
            log.warn('the value of exclude should be CONF/LOG/SQL/VERSION')
            return False
        if check != 'ALL' and exclude != '':
            log.warn('cannot set exclude if the value of check is not \'ALL\'')
            return False
        if check == 'ALL':
            self.check_items = self.all_items[1:]
        else:
            self.check_items.append(check)
        if exclude != '':
            self.check_items = list(filter(lambda x : x != exclude, self.check_items))
        return True

    def check_version(self) -> bool:
        return 'VERSION' in self.check_items

    def check_conf(self) -> bool:
        return 'CONF' in self.check_items

    def check_log(self) -> bool:
        return 'LOG' in self.check_items

    def check_sql(self) -> bool:
        return 'SQL' in self.check_items
