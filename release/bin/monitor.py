import os
import time

method = ["Put", "Get", "Scan"]
monitor_key = ["count", "error", "latency", "qps"]

def get_data(url):
    result = {}
    output = os.popen("curl %s" % url)
    method_content = output.read().split("\n\n")

    for content in method_content:
        item_arr = content.split("\n")
        if len(item_arr) < 12:
            continue
        cur_method = item_arr[-12].split(" ")[0]    
        if cur_method in method:
            result.setdefault(cur_method, {})
            for item in item_arr[-11:]:
                arr = item.strip().split(":")
                if arr[0] in monitor_key:
                    result[cur_method][arr[0]] = arr[1].strip()
    return result                

def get_conf():
    conf_map = {}
    conf_file = open("./conf/rtidb.flags", "r")
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
    conf_map = get_conf()
    url = "http://" + conf_map["endpoint"] + "/status"
    log_file = conf_map["log_dir"] + "/monitor.log"
    if not os.path.exists(conf_map["log_dir"]):
        os.mkdir(conf_map["log_dir"])
    log_file = open(log_file, 'w')
    last_date = get_timestamp()[1]
    while True:
        time_stamp = get_timestamp()[0]
        new_date = get_timestamp()[1]
        if new_date != last_date:
            log_file.close()
            os.rename(log_file, log_file + "." + last_date)
            last_date = new_date
            log_file = open(log_file, 'w')

        result = get_data(url)
        for method_data in result:
            data = time_stamp + "\t" + method_data + "\t" + result[method_data]["count"] + "\t" + result[method_data]["error"] + "\t" + \
                    result[method_data]["latency"] + "\t" + result[method_data]["qps"]
            log_file.write(data + "\n")        
            log_file.flush()
        time.sleep(10)
    log_file.close()        
