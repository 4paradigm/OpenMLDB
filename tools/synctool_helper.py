from datetime import datetime
import json
import os
import requests
import configparser as cfg
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('action', type=str, help='create, delete, status, tool-status')
parser.add_argument('-t', type=str, help='task id(db.table)')
parser.add_argument('-m', '--mode', type=int, help='task mode(0,1,2: FULL, INCREMENTAL_BY_TIMESTAMP, FULL_AND_CONTINUOUS)')
parser.add_argument('-ts', type=int, help='start timestamp if mode is INCREMENTAL_BY_TIMESTAMP')
parser.add_argument('-d', '--dest', type=str, help='sync destination, should be the path, exclude hdfs header, e.g. hdfs://<namenode>:9000/tmp/a/b/c, should be /tmp/a/b/c')

parser.add_argument('-s', '--server', type=str, default='127.0.0.1:8848',help='sync tool host:port')
parser.add_argument('-f', '--synctool_conf', type=str, default='conf/synctool.properties', help='synctool config file path, we need it to get the sync tool address and persist path')

def status_parse(status):
    assert status['response']['code'] == 0, status
    # todo: check if part tasks are alive, if one unalive task, the whole table sync task is unalive
    if not 'readableInfo' in status:
        print('no task')
        return
    size = len(status['readableInfo'])
    print(size, 'tasks(tid-pid)')
    failed_tids = set()
    tid_pid_task = {}
    for i in range(size):
        info = status['readableInfo'][i]
        d = dict([item.split('=') for item in info.split(';')])
        task = status['task'][i]
        # print('task ', task) 
        progress = task.pop('progress')
        d.update(progress)
        d.update(task)
        # print(d)
        
        # more readable ts
        d['lastUpdateTime'] = str(datetime.fromtimestamp(int(d['lastUpdateTime'])/1000))
        if d['status'] == 'FAILED':
            failed_tids.add(d['tid'])
        if d['tid'] not in tid_pid_task:
            tid_pid_task[d['tid']] = {}
        tid_pid_task[d['tid']][d['pid']] = d
        print(d)

    print('table scope')
    print('running tasks:')
    for tid, pid_tasks in tid_pid_task.items():
        if tid not in failed_tids:
            print(f'tid {tid} task is running, pid parts: {pid_tasks.keys()}')
    if failed_tids:
        print('tasks contains failed part:', failed_tids)
        for tid in failed_tids:
            print('tid:', tid)
            for p in tid_pid_task[tid].values():
                if p['status'] == 'FAILED':
                    print('failed part:', p)
    else:
        print('all tasks are running')

if __name__ == "__main__":
    args = parser.parse_args()
    if args.action in ['create', 'delete', 'status']:
        sync_tool_url = f'http://{args.server}/openmldb.datasync.SyncTool'
        if args.action == 'create':
            def create_sync_task(db, table, mode, ts, dest):
                create_task = requests.post(f'{sync_tool_url}/CreateSyncTask', json={
                    "db": db, "name": table, "mode": mode, "start_ts": ts, "dest": dest})
                print(create_task.text)
            task = args.t
            assert len(task) > 0, 'task should not be empty'
            db, table = task.split('.')
            # just use int for mode
            mode = args.mode # default is full
            ts = 0 # it's safe to set ts starts from 0 when mode is 0 or 2
            if args.mode == 1:
                ts = args.ts
            assert len(args.dest) > 0, 'dest should not be empty'
            dest = args.dest
            create_sync_task(db, table, mode, ts, dest)
        elif args.action == 'delete':
            def delete_sync_task(db, table):
                delete_task = requests.post(f'{sync_tool_url}/DeleteSyncTask', json={
                    "db": db, "name": table})
                print(delete_task.text)
            task = args.t
            db, table = task.split('.')
            delete_sync_task(db, table)
        else:
            status = requests.get(f'{sync_tool_url}/TaskStatus', json={})
            status = json.loads(status.text)
            status_parse(status)
    elif args.action == 'tool-status':
        # check sync tool status, needs properties file
        config_path = args.synctool_conf
        cf = cfg.ConfigParser()
        with open(config_path, "r") as f:
            config_string = '[dummy_section]\n' + f.read()
            cf.read_string(config_string)
        cfgs = dict(cf.items('dummy_section'))
        print(cfgs)
        sync_tool_addr = cfgs['server.host']+ ':' + cfgs['server.port']
        synctool_progress_root = cfgs['sync_task.progress_path']
        data_cache_path = cfgs['data.cache_path']
        sync_tool_url = f'http://{sync_tool_addr}/openmldb.datasync.SyncTool'

        print('sync task progress in sync tool fs')
        # List the files in the directory
        directory = synctool_progress_root
        # Walk the directory tree
        for dirpath, dirnames, filenames in os.walk(directory):
            # Loop over the files in the current directory
            for filename in filenames:
                # Construct the full path to the file
                filepath = os.path.join(dirpath, filename)
                print(filepath)
                # Open the file for reading read protobuf message is dangerous

# about data collector config or cache files, it is not necessary at the moment

# data cache
# print("""
# data cache, save the data which data collector send to sync tool
# and sync tool will watch it and sink
# """)
# print('data cache path:', data_cache_path)
# try:
#     assert os.path.isdir(data_cache_path)
#     cache_status = {}
#     for dirpath, dirnames, filenames in os.walk(data_cache_path):
#         if not dirnames:
#             # the last path
#             print(dirpath)
#             # tid = dirpath.split('/')[-2]
#             # ts = int(dirpath.split('/')[-1])
#             # cache_status[tid] = max(cache_status.get(tid, -1), ts)

#     # print('data latest cache status(including finished/deleted task)')
#     # print([(tid, str(datetime.fromtimestamp(ts/1000))) for tid, ts in cache_status.items()])
# except Exception as e:
#     pass

# data collector status(runtime needs rpc in data collector, file needs run on data collector host)
# try:
#     data_collector = '172.24.4.27:8888'
#     # no status rpc now
#     # data_collector_url = f'http://{data_collector}/DataCollector/'
#     # check dir collector_datadir datacollector/<tid>-<pid>
#     collector_datadir = '/home/huangwei/tmp/openmldb/datacollector' 
#     assert os.path.isdir(collector_datadir)
# except Exception as e:
#     pass

# task progress in data collector no rpc now
