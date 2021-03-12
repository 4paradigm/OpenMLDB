# -*-coding:utf-8 -*-
import os
import time
import sys
dir_name = os.path.dirname(os.path.realpath(sys.argv[0]))
work_dir = os.path.dirname(dir_name)
sys.path.insert(0, dir_name + "/prometheus_client-0.6.0")
from prometheus_client import start_http_server
from prometheus_client import Gauge
import traceback

try:
    import urllib.request as request
except:
    import urllib2 as request

if sys.version_info.major > 2:
    do_open = lambda filename, mode: open(filename, mode, encoding="utf-8")
else:
    do_open = lambda filename, mode: open(filename, mode)

method = ["put", "get", "scan", "query", "subquery", "batchquery", "sqlbatchrequestquery", "subbatchrequestquery"]
method_set = set(method)
monitor_key = ["count", "error", "qps", "latency", "latency_50",
               "latency_90", "latency_99", "latency_999", "latency_9999", "max_latency"]
fedb_log = {
    "tablet": [
        {
            "file_name": "/fedb_mon.log",
            "offset": 0,
            "item": {
                "name": "restart_num",
                "key": "./bin/boot.sh",
                "num": 0
            }
        },
        {
            "file_name": "/tablet.WARNING",
            "offset": 0,
            "item": {
                "name": "reconnect_num",
                "key": "reconnect zk",
                "num": 0
            }
        }],
    "ns": [
        {
            "file_name": "/fedb_ns_mon.log",
            "offset": 0,
            "item": {
                "name": "restart_num",
                "key": "./bin/boot_ns.sh",
                "num": 0
            }
        },
        {
            "file_name": "/nameserver.WARNING",
            "offset": 0,
            "item": {
                "name": "reconnect_num",
                "key": "reconnect zk",
                "num": 0
            }
        }]
}


def get_data(url):
    result = {}
    resp = request.urlopen(url)
    def parse_data():
        beginParse = False
        stageBegin = False
        server = ""
        servers = {}
        for i in resp:
          if len(i) < 0:
            continue
          l = i.decode()
          if beginParse and l[0] == '\n':
            stageBegin = False
            resp.readline()
            continue
          elif l[0] == '[':
            beginParse = True
            resp.readline()
            continue
          if not beginParse:
            continue
          if stageBegin:
            cols = l.strip().split()
            indicator = cols[0].strip(":")
            value = int(cols[1])
            servers[server][indicator] = value
          else:
            stageBegin = True
            server = l.split()[0]
            servers[server]={}
        return servers
    return parse_data()

def get_mem(url):
    memory_by_application = 0
    memory_central_cache_freelist = 0
    memory_acutal_used = 0
    resp = request.urlopen(url)
    for i in resp:
        line = i.decode().strip()
        if line.rfind("use by application") > 0:
            try:
                memory_by_application = int(line.split()[1])
            except Exception as e:
                memory_by_application = 0
        elif line.rfind("central cache freelist") > 0:
            try:
                memory_central_cache_freelist = line.split()[2]
            except Exception as e:
                memory_central_cache_freelist = 0
        elif line.rfind("Actual memory used") > 0:
            try:
                memory_acutal_used = line.split()[2]
            except Exception as e:
                memory_acutal_used = 0
        else:
            continue
    return memory_by_application, memory_central_cache_freelist, memory_acutal_used

def get_conf():
    conf_map = {}
    with do_open(work_dir + "/conf/monitor.conf", "r") as conf_file:
        for line in conf_file:
            if line.startswith("#"):
                continue
            arr = line.split("=")
            if arr[0] == "tablet_endpoint":
                conf_map["endpoint"] = arr[1].strip()
            elif arr[0] == "tablet_log_dir":
                conf_map["tablet"] = arr[1].strip()
            elif arr[0] == "ns_log_dir":
                conf_map["ns"] = arr[1].strip()
            elif arr[0] == "port":
                conf_map["port"] = int(arr[1].strip())
            elif arr[0] == "log_dir":
                conf_map["log_dir"] = arr[1].strip()
            elif arr[0] == "interval":
                conf_map["interval"] = int(arr[1].strip())
        return conf_map


def get_timestamp():
    def big_int(ct):
        try:
            return long(ct)
        except:
            return int(ct)
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    data_secs = (ct - big_int(ct)) * 1000
    today = time.strftime("%Y%m%d", local_time)
    return ["%s.%03d" % (data_head, data_secs) + " +0800", today]


def search_key(file_name, offset, keyword):
    if not os.path.exists(file_name):
        return (0, 0)
    count = 0
    with do_open(file_name, 'r') as f:
        f.seek(offset)
        for line in f:
            if line.find(keyword) != -1:
                count += 1
        return (count, f.tell())


if __name__ == "__main__":
    conf_map = get_conf()
    env_dist = os.environ
    port = conf_map["port"]
    if "metric_port" in env_dist:
        port = int(env_dist["metric_port"])
    start_http_server(port)
    gauge = {}
    for cur_method in method:
        gauge[cur_method] = Gauge(
            "fedb_" + cur_method, cur_method, ["latency", "type"])

    gauge["log"] = Gauge("fedb_log", "log", ["role", "type"])
    gauge["memory"] = Gauge("fedb_memory", "memory", ["role", "type"])

    endpoint = ""
    if "endpoint" in conf_map:
        endpoint = conf_map["endpoint"]
    if "endpoint" in env_dist:
        endpoint = env_dist["endpoint"]
    url = ""
    mem_url = ""
    if endpoint != "":
        url = "http://" + endpoint + "/status"
        mem_url = "http://{}/TabletServer/ShowMemPool".format(endpoint)

    log_file_name = conf_map["log_dir"] + "/monitor.log"
    if not os.path.exists(conf_map["log_dir"]):
        os.mkdir(conf_map["log_dir"])
    log_file = open(log_file_name, 'w')
    last_date = get_timestamp()[1]
    while True:
        try:
            time_stamp = get_timestamp()[0]
            new_date = get_timestamp()[1]
            if new_date != last_date:
                log_file.close()
                os.rename(log_file_name, log_file_name + "." + last_date)
                last_date = new_date
                log_file = open(log_file_name, 'w')
            for module in fedb_log.keys():
                if module not in conf_map:
                    continue
                for var in fedb_log[module]:
                    (count, offset) = search_key(
                        conf_map[module] + var["file_name"], var["offset"], var["item"]["key"])
                    if var["item"]["name"] == "restart_num":
                        var["offset"] = offset
                        if count > 0:
                            log_file.write(time_stamp + "\t" +
                                           module + " start\n")
                            var["item"]["num"] += count
                            fedb_log[module][1]["item"]["offset"] = 0
                            fedb_log[module][1]["item"]["num"] = 0
                        value = count - 1 if count > 0 else count
                        gauge["log"].labels(
                            module, var["item"]["name"]).set(var["item"]["num"])
                    else:
                        var["item"]["num"] += count
                        var["offset"] = offset
                        gauge["log"].labels(module, var["item"]["name"]).set(
                            str(var["item"]["num"]))
            if url == "":
                continue
            result = get_data(url)
            for method_data in result:
                lower_method = method_data.lower()
                if lower_method not in method_set:
                    continue
                data = "{}\t{}\t".format(time_stamp, lower_method)
                for key in monitor_key:
                    data = "{}\t{}:{}".format(data, key, result[method_data][key])
                    if key.find("latency") == -1:
                        gauge[lower_method].labels("type", key).set(
                            result[method_data][key])
                    else:
                        gauge[lower_method].labels("latency", key).set(
                            result[method_data][key])
                log_file.write(data + "\n")
                log_file.flush()
            if len(mem_url) < 1:
                continue
            mema, memb, memc = get_mem(mem_url)
            gauge["memory"].labels("memory", "use_by_application").set(mema)
            gauge["memory"].labels("memory", "central_cache_freelist").set(memb)
            gauge["memory"].labels("memory", "actual_memory_used").set(memc)
        except Exception as e:
            traceback.print_exc(file=log_file)
            log_file.write("has exception {}\n".format(e))
        time.sleep(conf_map["interval"])
    log_file.close()
