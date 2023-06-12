import os
import logging
import paramiko
from paramiko.file import BufferedFile

log = logging.getLogger(__name__)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def local_cmd(cmd):
    f = os.popen(cmd)
    result = f.read()
    status = f.close()
    # os.waitstatus_to_exitcode(status)
    return result, status


def get_local_logs(root_path, role):
    def role_filter(role, file_name):
        if not file_name.startswith(f"{role}.info.log"):
            return False
        return True

    names = os.listdir(root_path)
    files = list(filter(lambda x: role_filter(role, x), names))
    file_list = []
    for cur_file in files:
        file_list.append((cur_file, os.path.abspath(os.path.join(root_path, cur_file))))
    return file_list


def get_files(root_path):
    if not os.path.exists(root_path):
        log.warning(f"{root_path} is not exists")
        return ""
    file_map = {}
    names = os.listdir(root_path)
    for name in names:
        file_map.setdefault(name, {})
        path = os.path.abspath(os.path.join(root_path, name))
        dirs = os.listdir(path)
        for dir_name in dirs:
            arr = dir_name.split("-")
            file_map[name].setdefault(arr[1], {})
            file_map[name][arr[1]].setdefault(arr[0], [])
            cur_path = os.path.abspath(os.path.join(path, dir_name))
            files = os.listdir(cur_path)
            for cur_file in files:
                if cur_file.startswith("."):
                    continue
                file_map[name][arr[1]][arr[0]].append(
                    (cur_file, os.path.abspath(os.path.join(cur_path, cur_file)))
                )
    return file_map


def clean_dir(path):
    def rm_dirs(path):
        if os.path.isfile(path):
            try:
                os.remove(path)
            except Exception as e:
                print(e)
        elif os.path.isdir(path):
            for item in os.listdir(path):
                itempath = os.path.join(path, item)
                rm_dirs(itempath)
            try:
                os.rmdir(path)
            except Exception as e:
                print(e)

    if os.path.exists(path):
        rm_dirs(path)


class SSH(metaclass=Singleton):
    def __init__(self) -> None:
        logging.getLogger("paramiko").setLevel(logging.WARNING)
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.load_system_host_keys()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def exec(self, host, cmd) -> paramiko.SSHClient:
        self.ssh_client.connect(hostname=host)
        return self.ssh_client.exec_command(cmd)

    def get_sftp(self, host) -> paramiko.SFTPClient:
        self.ssh_client.connect(hostname=host)
        return self.ssh_client.open_sftp()


def buf2str(buf: BufferedFile) -> str:
    return buf.read().decode("utf-8")
