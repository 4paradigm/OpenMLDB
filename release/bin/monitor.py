# -*-coding:utf-8 -*-
import os
import time
import sys
dir_name = os.path.dirname(os.path.realpath(sys.argv[0]))
work_dir = os.path.dirname(dir_name)
sys.path.insert(0, dir_name + "/prometheus_client-0.6.0")
from prometheus_client import start_http_server
from prometheus_client import Gauge

method = ["put", "get", "scan"]
monitor_key = ["count", "error", "qps", "latency", "latency_50",
               "latency_90", "latency_99", "latency_999", "latency_9999", "max_latency"]

rtidb_log = {
    "tablet": [
        {
            "file_name": "/rtidb_mon.log",
            "offset": 0,
            "item": {
                "name": "restart_num",
                "key": "./bin/boot.sh",
                "num": 0
            }
        },
        {
            "file_name": "/tablet.warning.log",
            "offset": 0,
            "item": {
                "name": "reconnect_num",
                "key": "reconnect zk",
                "num": 0
            }
        }],
    "ns": [
        {
            "file_name": "/rtidb_ns_mon.log",
            "offset": 0,
            "item": {
                "name": "restart_num",
                "key": "./bin/boot_ns.sh",
                "num": 0
            }
        },
        {
            "file_name": "/nameserver.warning.log",
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
    output = os.popen("curl -s %s" % url)
    method_content = output.read().split("\n\n")

    for content in method_content:
        item_arr = content.split("\n")
        if len(item_arr) < 12:
            continue
        cur_method = item_arr[-12].split(" ")[0]
        cur_method = cur_method.lower()
        if cur_method in method:
            result.setdefault(cur_method, {})
            for item in item_arr[-11:]:
                arr = item.strip().split(":")
                if arr[0] in monitor_key:
                    result[cur_method][arr[0]] = arr[1].strip()
    return result


def get_conf():
    conf_map = {}
    with open(work_dir + "/conf/monitor.conf", "r") as conf_file:
        for line in conf_file.xreadlines():
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
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    data_secs = (ct - long(ct)) * 1000
    today = time.strftime("%Y%m%d", local_time)
    return ["%s.%03d" % (data_head, data_secs) + " +0800", today]


def search_key(file_name, offset, keyword):
    if not os.path.exists(file_name):
        return (0, 0)
    count = 0
    with open(file_name, 'r') as f:
        f.seek(offset)
        for line in f.xreadlines():
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
            "rtidb_" + cur_method, cur_method, ["latency", "type"])

    gauge["log"] = Gauge("rtidb_log", "log", ["role", "type"])

    endpoint = ""
    if "endpoint" in conf_map:
        endpoint = conf_map["endpoint"]
    if "endpoint" in env_dist:
        endpoint = env_dist["endpoint"]
    url = ""
    if endpoint != "":
        url = "http://" + endpoint + "/status"
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
            for module in rtidb_log.keys():
                if module not in conf_map:
                    continue
                for var in rtidb_log[module]:
                    (count, offset) = search_key(
                        conf_map[module] + var["file_name"], var["offset"], var["item"]["key"])
                    if var["item"]["name"] == "restart_num":
                        var["offset"] = offset
                        if count > 0:
                            log_file.write(time_stamp + "\t" +
                                           module + " start\n")
                            var["item"]["num"] += count
                            rtidb_log[module][1]["item"]["offset"] = 0
                            rtidb_log[module][1]["item"]["num"] = 0
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
                data = time_stamp + "\t" + method_data + "\t"
                for key in monitor_key:
                    data += "\t" + key + ":" + result[method_data][key]
                    if key.find("latency") == -1:
                        gauge[method_data].labels("type", key).set(
                            result[method_data][key])
                    else:
                        gauge[method_data].labels("latency", key).set(
                            result[method_data][key])
                log_file.write(data + "\n")
                log_file.flush()
        except Exception, ex:
            log_file.write("has exception {}\n".format(ex))
        time.sleep(conf_map["interval"])
    log_file.close()
