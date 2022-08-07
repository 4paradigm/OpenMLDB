import logging

log = logging.getLogger(__name__)

class LogAnalysis:
    def __init__(self, role, endpoint, file_list):
        self.role = role
        self.endpoint = endpoint
        self.file_list = file_list

    def check_warning(self, name, line) -> bool:
        if self.role == 'taskmanager':
            if name.startswith('taskmanager'):
                if len(line) < 28:
                    return False
                log_level = line[24:28]
                if log_level in ['INFO', 'WARN', 'ERROR'] and log_level != 'INFO':
                    return True
            else:
                if len(line) < 22:
                    return False
                log_level = line[18:22]
                if log_level in ['INFO', 'WARN', 'ERROR'] and log_level != 'INFO':
                    return True
        else:
            if (line.startswith('W') or line.startswith('E')) and line[1].isnumeric():
                return True
        return False

    def analysis_log(self):
        flag = True
        for name, full_path in self.file_list:
            is_print_file = False
            with open(full_path, 'r', encoding='UTF-8') as f:
                line = f.readline().rstrip()
                while line:
                    if self.check_warning(name, line):
                        if not is_print_file:
                            flag = False
                            print('')
                            log.warn(f'{self.role} {self.endpoint} have error logs in {name}:')
                            print('----------------------------------------')
                            is_print_file = True
                        print(line)
                    line = f.readline().rstrip()
        return flag

