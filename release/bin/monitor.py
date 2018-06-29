import os
import time
from prometheus_client import start_http_server
from prometheus_client import Gauge

method = ["put", "get", "scan"]
monitor_key = ["count", "error", "qps", "latency", "latency_50", "latency_90", "latency_99", "latency_999", "latency_9999", "max_latency"]

def get_data(url):
    result = {}
    output = os.popen("curl %s" % url)
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
    conf_file = open("./conf/tablet.flags", "r")
    for line in conf_file.xreadlines():
        if line.startswith("#"):
            continue
        arr = line.split("=")
        if arr[0] == "--endpoint":
            conf_map["endpoint"] = arr[1].strip()
        elif arr[0] == "--log_dir":
            conf_map["log_dir"] = arr[1].strip()
    return conf_map

def get_timestamp():
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    data_secs = (ct - long(ct)) * 1000
    today = time.strftime("%Y%m%d", local_time)
    return ["%s.%03d" % (data_head, data_secs) + " +0800", today]


if __name__ == "__main__":
    env_dist = os.environ
    port = 8000
    if "metric_port" in env_dist:
        port = env_dist["metric_port"]
    start_http_server(port)    
    gauge = Gauge("my_inprogress_requests", "description of gauge")
    gauge = {}
    for cur_method in method:
        gauge[cur_method] = Gauge("rtidb_" + cur_method, cur_method, ["latency", "type"])

    conf_map = get_conf()
    endpoint = conf_map["endpoint"]
    if "endpoint" in env_dist:
        endpoint = env_dist["endpoint"]
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

            result = get_data(url)
            for method_data in result:
                data = time_stamp + "\t" + method_data + "\t"
                for key in monitor_key:
                    data += "\t" + key + ":" + result[method_data][key]
                    if key.find("latency") == -1:
                        gauge[method_data].labels("type", key).set(result[method_data][key])
                    else:
                        gauge[method_data].labels("latency", key).set(result[method_data][key])
                log_file.write(data + "\n")
                log_file.flush()
        except:
            log_file.write("has exception\n")
        time.sleep(1)
    log_file.close()
