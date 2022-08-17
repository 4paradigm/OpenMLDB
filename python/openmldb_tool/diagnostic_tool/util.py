import os
import logging

log = logging.getLogger(__name__)

def get_openmldb_version(path) -> str:
    openmldb_file = path + '/openmldb'
    if not os.path.exists(openmldb_file):
        log.warning(f"{openmldb_file} is not exists")
        return ""
    cmd= openmldb_file + ' --version'
    result = os.popen(cmd)
    tmp = result.read()
    version=tmp.split('\n')[0].split(' ')[2][:5]
    return version

def get_local_logs(root_path, role):
    def role_filter(role, file_name):
        if not file_name.startswith(f'{role}.info.log'): return False
        return True
    names = os.listdir(root_path)
    files = list(filter(lambda x : role_filter(role, x), names))
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
            arr = dir_name.split('-')
            file_map[name].setdefault(arr[1], {})
            file_map[name][arr[1]].setdefault(arr[0], [])
            cur_path = os.path.abspath(os.path.join(path, dir_name))
            files = os.listdir(cur_path)
            for cur_file in files:
                if cur_file.startswith('.'): continue
                file_map[name][arr[1]][arr[0]].append((cur_file, os.path.abspath(os.path.join(cur_path, cur_file))))
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
