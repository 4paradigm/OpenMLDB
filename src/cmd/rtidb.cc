//
// rtidb.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <sched.h>
#include <unistd.h>
#include <iostream>
#include <sstream>

#include <snappy.h>
#include <gflags/gflags.h>
#include <brpc/server.h>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include "logging.h"

#include "tablet/tablet_impl.h"
#include "nameserver/name_server_impl.h"
#include "client/tablet_client.h"
#include "client/ns_client.h"
#include "base/strings.h"
#include "base/kv_iterator.h"
#include "base/schema_codec.h"
#include "base/flat_array.h"
#include "base/file_util.h"
#include "base/hash.h"
#include "base/display.h"
#include "base/linenoise.h"
#include "timer.h"
#include "version.h"
#include "proto/tablet.pb.h"
#include "proto/client.pb.h"
#include "proto/name_server.pb.h"
#include "tprinter.h"
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <random>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_string(endpoint);
DECLARE_int32(port);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(thread_pool_size);
DECLARE_int32(put_concurrency_limit);
DECLARE_int32(scan_concurrency_limit);
DECLARE_int32(get_concurrency_limit);
DEFINE_string(role, "tablet | nameserver | client | ns_client", "Set the rtidb role for start");
DEFINE_string(cmd, "", "Set the command");
DEFINE_bool(interactive, true, "Set the interactive");

DEFINE_string(log_dir, "", "Config the log dir");
DEFINE_int32(log_file_size, 1024, "Config the log size in MB");
DEFINE_int32(log_file_count, 24, "Config the log count");
DEFINE_string(log_level, "debug", "Set the rtidb log level, eg: debug or info");
DECLARE_uint32(latest_ttl_max);
DECLARE_uint32(absolute_ttl_max);
DECLARE_uint32(skiplist_max_height);
DECLARE_uint32(latest_default_skiplist_height);
DECLARE_uint32(absolute_default_skiplist_height);
DECLARE_uint32(preview_limit_max_num);
DECLARE_uint32(preview_default_limit);
DECLARE_uint32(max_col_display_length);

void SetupLog() {
    // Config log 
    if (FLAGS_log_level == "debug") {
        ::baidu::common::SetLogLevel(DEBUG);
    }else {
        ::baidu::common::SetLogLevel(INFO);
    }
    if (!FLAGS_log_dir.empty()) {
        ::rtidb::base::Mkdir(FLAGS_log_dir);
        std::string info_file = FLAGS_log_dir + "/" + FLAGS_role + ".info.log";
        std::string warning_file = FLAGS_log_dir + "/" + FLAGS_role + ".warning.log";
        ::baidu::common::SetLogFile(info_file.c_str());
        ::baidu::common::SetWarningFile(warning_file.c_str());
    }
    ::baidu::common::SetLogCount(FLAGS_log_file_count);
    ::baidu::common::SetLogSize(FLAGS_log_file_size);
}

void StartNameServer() {
    SetupLog();
    ::rtidb::nameserver::NameServerImpl* name_server = new ::rtidb::nameserver::NameServerImpl();
    if (!name_server->Init()) {
        PDLOG(WARNING, "Fail to init");
        exit(1);
    }
    brpc::ServerOptions options;
    options.num_threads = FLAGS_thread_pool_size;
    brpc::Server server;
    if (server.AddService(name_server, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    if (FLAGS_port > 0) {
        if (server.Start(FLAGS_port, &options) != 0) {
            PDLOG(WARNING, "Fail to start server");
            exit(1);
        }
        PDLOG(INFO, "start nameserver on endpoint %d with version %d.%d.%d.%d", 
                    FLAGS_port, RTIDB_VERSION_MAJOR, RTIDB_VERSION_MEDIUM, RTIDB_VERSION_MINOR, RTIDB_VERSION_BUG);
    } else {
        if (server.Start(FLAGS_endpoint.c_str(), &options) != 0) {
            PDLOG(WARNING, "Fail to start server");
            exit(1);
        }
        PDLOG(INFO, "start nameserver on endpoint %s with version %d.%d.%d.%d", 
                    FLAGS_endpoint.c_str(), RTIDB_VERSION_MAJOR, RTIDB_VERSION_MEDIUM, RTIDB_VERSION_MINOR, RTIDB_VERSION_BUG);
    }
    std::ostringstream oss;
    oss << RTIDB_VERSION_MAJOR << "." << RTIDB_VERSION_MEDIUM << "." << RTIDB_VERSION_MINOR << "." << RTIDB_VERSION_BUG;
    server.set_version(oss.str());
    server.RunUntilAskedToQuit();
}

int THPIsEnabled() {
#ifdef __linux__
    char buf[1024];
    FILE *fp = fopen("/sys/kernel/mm/transparent_hugepage/enabled","r");
    if (!fp) {
        return 0;
    }
    if (fgets(buf, sizeof(buf), fp) == NULL) {
        fclose(fp);
        return 0;
    }
    fclose(fp);
    if (strstr(buf,"[never]") == NULL) {
        return 1;
    }
    fp = fopen("/sys/kernel/mm/transparent_hugepage/defrag","r");
    if (!fp) return 0;
    if (fgets(buf, sizeof(buf), fp) == NULL) {
        fclose(fp);
        return 0;
    }
    fclose(fp);
    return (strstr(buf,"[never]") == NULL) ? 1 : 0;
#else
    return 0;
#endif
}

int SwapIsEnabled() {
#ifdef __linux__
    char buf[1024];
    FILE *fp = fopen("/proc/swaps","r");
    if (!fp) {
        return 0;
    }
    if (fgets(buf, sizeof(buf), fp) == NULL) {
        fclose(fp);
        return 0;
    }
    // if the swap is disabled, there is only one line in /proc/swaps.
    // Filename     Type        Size    Used    Priority
    if (fgets(buf, sizeof(buf), fp) == NULL) {
        fclose(fp);
        return 0;
    }
    fclose(fp);
    return 1;
#else
    return 0;
#endif
}

void StartTablet() {
    if (THPIsEnabled()) {
        PDLOG(WARNING, "THP is enabled in your kernel. This will create latency and memory usage issues with RTIDB."
                       "To fix this issue run the command 'echo never > /sys/kernel/mm/transparent_hugepage/enabled' and "
                       "'echo never > /sys/kernel/mm/transparent_hugepage/defrag' as root");
    }
    if (SwapIsEnabled()) {
        PDLOG(WARNING, "Swap is enabled in your kernel. This will create latency and memory usage issues with RTIDB."
                       "To fix this issue run the command 'swapoff -a' as root");
    }
    SetupLog();
    ::rtidb::tablet::TabletImpl* tablet = new ::rtidb::tablet::TabletImpl();
    bool ok = tablet->Init();
    if (!ok) {
        PDLOG(WARNING, "fail to init tablet");
        exit(1);
    }
    brpc::ServerOptions options;
    options.num_threads = FLAGS_thread_pool_size;
    brpc::Server server;
    if (server.AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        PDLOG(WARNING, "Fail to add service");
        exit(1);
    }
    server.MaxConcurrencyOf(tablet, "Scan") = FLAGS_scan_concurrency_limit;
    server.MaxConcurrencyOf(tablet, "Put") = FLAGS_put_concurrency_limit;
    tablet->SetServer(&server);
    server.MaxConcurrencyOf(tablet, "Get") = FLAGS_get_concurrency_limit;
    if (FLAGS_port > 0) {
        if (server.Start(FLAGS_port, &options) != 0) {
            PDLOG(WARNING, "Fail to start server");
            exit(1);
        }
        PDLOG(INFO, "start tablet on port %d with version %d.%d.%d.%d", 
                    FLAGS_port, RTIDB_VERSION_MAJOR, RTIDB_VERSION_MEDIUM, RTIDB_VERSION_MINOR, RTIDB_VERSION_BUG);
    } else {
        if (server.Start(FLAGS_endpoint.c_str(), &options) != 0) {
            PDLOG(WARNING, "Fail to start server");
            exit(1);
        }
        PDLOG(INFO, "start tablet on endpoint %s with version %d.%d.%d.%d", 
                    FLAGS_endpoint.c_str(), RTIDB_VERSION_MAJOR, RTIDB_VERSION_MEDIUM, RTIDB_VERSION_MINOR, RTIDB_VERSION_BUG);
    }
    if (!tablet->RegisterZK()) {
        PDLOG(WARNING, "Fail to register zk");
        exit(1);
    }
    std::ostringstream oss;
    oss << RTIDB_VERSION_MAJOR << "." << RTIDB_VERSION_MEDIUM << "." << RTIDB_VERSION_MINOR << "." << RTIDB_VERSION_BUG;
    server.set_version(oss.str());
    server.RunUntilAskedToQuit();
}

int SetDimensionData(const std::map<std::string, std::string>& raw_data,
            const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>& column_key_field,
            uint32_t pid_num,
            std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& dimensions) {
    uint32_t dimension_idx = 0;
    std::set<std::string> index_name_set;
    for (const auto& column_key : column_key_field) {
        std::string index_name = column_key.index_name();
        if (index_name_set.find(index_name) != index_name_set.end()) {
            continue;
        }
        index_name_set.insert(index_name);
        std::string key;
        for (int i = 0; i < column_key.col_name_size(); i++) {
            auto pos = raw_data.find(column_key.col_name(i));
            if (pos == raw_data.end()) {
                return -1;
            }
            if (!key.empty()) {
                key += "|";
            }
            key += pos->second;
        }
        if (key.empty()) {
            auto pos = raw_data.find(index_name);
            if (pos == raw_data.end()) {
                return -1;
            }
            key = pos->second;
        }
        uint32_t pid = 0;
        if (pid_num > 0) {
            pid = (uint32_t)(::rtidb::base::hash64(key) % pid_num);
        }
        if (dimensions.find(pid) == dimensions.end()) {
            dimensions.insert(std::make_pair(pid, std::vector<std::pair<std::string, uint32_t>>()));
        }
        dimensions[pid].push_back(std::make_pair(key, dimension_idx));
        dimension_idx++;
    }
    return 0;
}

int EncodeMultiDimensionData(const std::vector<std::string>& data, 
            const std::vector<::rtidb::base::ColumnDesc>& columns,
            uint32_t pid_num,
            std::string& value, 
            std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& dimensions,
            std::vector<uint64_t>& ts_dimensions) {
    if (data.size() != columns.size()) {
        return -1;
    }
    uint8_t cnt = (uint8_t)data.size();
    ::rtidb::base::FlatArrayCodec codec(&value, cnt);
    uint32_t idx_cnt = 0;
    for (uint32_t i = 0; i < data.size(); i++) {
        if (columns[i].add_ts_idx) {
            uint32_t pid = 0;
            if (pid_num > 0) {
                pid = (uint32_t)(::rtidb::base::hash64(data[i]) % pid_num);
            }
            if (dimensions.find(pid) == dimensions.end()) {
                dimensions.insert(std::make_pair(pid, std::vector<std::pair<std::string, uint32_t>>()));
            }
            dimensions[pid].push_back(std::make_pair(data[i], idx_cnt));
            idx_cnt ++;
        }
        bool codec_ok = false;
        try {
            if (columns[i].is_ts_col) {
                ts_dimensions.push_back(boost::lexical_cast<uint64_t>(data[i]));
            }
            if (columns[i].type == ::rtidb::base::ColType::kInt32) {
                codec_ok = codec.Append(boost::lexical_cast<int32_t>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kInt64) {
                codec_ok = codec.Append(boost::lexical_cast<int64_t>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kUInt32) {
                if (!boost::algorithm::starts_with(data[i], "-")) {
                    codec_ok = codec.Append(boost::lexical_cast<uint32_t>(data[i]));
                }
            } else if (columns[i].type == ::rtidb::base::ColType::kUInt64) {
                if (!boost::algorithm::starts_with(data[i], "-")) {
                    codec_ok = codec.Append(boost::lexical_cast<uint64_t>(data[i]));
                }
            } else if (columns[i].type == ::rtidb::base::ColType::kFloat) {
                codec_ok = codec.Append(boost::lexical_cast<float>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kDouble) {
                codec_ok = codec.Append(boost::lexical_cast<double>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kString) {
                codec_ok = codec.Append(data[i]);
            } else if (columns[i].type == ::rtidb::base::ColType::kTimestamp) {
                codec_ok = codec.AppendTimestamp(boost::lexical_cast<uint64_t>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kDate) {
                std::string date = data[i] + " 00:00:00";
                tm tm_s;
                time_t time;
                char buf[20]= {0};
                strcpy(buf, date.c_str());
                char* result = strptime(buf, "%Y-%m-%d %H:%M:%S", &tm_s);
                if (result == NULL) {
                    printf("date format is YY-MM-DD. ex: 2018-06-01\n");
                    return -1;
                }
                tm_s.tm_isdst = -1;
                time = mktime(&tm_s) * 1000;
                codec_ok = codec.AppendDate(uint64_t(time));
            } else if (columns[i].type == ::rtidb::base::ColType::kInt16) {
                codec_ok = codec.Append(boost::lexical_cast<int16_t>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kUInt16) {
                codec_ok = codec.Append(boost::lexical_cast<uint16_t>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kBool) {
                bool value = false;
                std::string raw_value = data[i];
                std::transform(raw_value.begin(), raw_value.end(), raw_value.begin(), ::tolower);
                if (raw_value == "true") {
                    value = true;
                } else if (raw_value == "false") {
                    value = false;
                } else {
                    return -1;
                }
                codec_ok = codec.Append(value);
            } else {
                codec_ok = codec.AppendNull();
            }
        } catch(std::exception const& e) {
            std::cout << e.what() << std::endl;
            return -1;
        } 
        if (!codec_ok) {
            return -1;
        }
    }
    codec.Build();
    return 0;
}

int EncodeMultiDimensionData(const std::vector<std::string>& data, 
            const std::vector<::rtidb::base::ColumnDesc>& columns,
            uint32_t pid_num,
            std::string& value, 
            std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& dimensions) {
    std::vector<uint64_t> ts_dimensions;
    return EncodeMultiDimensionData(data, columns, pid_num, value, dimensions, ts_dimensions);
}

int PutData(uint32_t tid, const std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& dimensions,
            const std::vector<uint64_t>& ts_dimensions, uint64_t ts, const std::string& value,
            const google::protobuf::RepeatedPtrField<::rtidb::nameserver::TablePartition>& table_partition) {
    std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>> clients;
    for (auto iter = dimensions.begin(); iter != dimensions.end(); iter++) {
        uint32_t pid = iter->first;
        std::string endpoint;
        for (const auto& cur_table_partition : table_partition) {
            if (cur_table_partition.pid() != pid) {
                continue;
            }
            for (int inner_idx = 0; inner_idx < cur_table_partition.partition_meta_size(); inner_idx++) {
                if (cur_table_partition.partition_meta(inner_idx).is_leader() && 
                         cur_table_partition.partition_meta(inner_idx).is_alive()) {
                    endpoint = cur_table_partition.partition_meta(inner_idx).endpoint();
                    break;
                }
            }
            break;
        }
        if (endpoint.empty()) {
            printf("put error. cannot find healthy endpoint. pid is %u\n", pid);
            return -1;
        }
        if (clients.find(endpoint) == clients.end()) {
            clients.insert(std::make_pair(endpoint, std::make_shared<::rtidb::client::TabletClient>(endpoint)));
            if (clients[endpoint]->Init() < 0) {
                printf("tablet client init failed, endpoint is %s\n", endpoint.c_str());
                return -1;
            }
        }
        if (ts_dimensions.empty()) {
            if (!clients[endpoint]->Put(tid, pid, ts, value, iter->second)) {
                printf("put failed. tid %u pid %u endpoint %s\n", tid, pid, endpoint.c_str()); 
                return -1;
            }
        } else {
            if (!clients[endpoint]->Put(tid, pid, iter->second, ts_dimensions, value)) {
                printf("put failed. tid %u pid %u endpoint %s\n", tid, pid, endpoint.c_str()); 
                return -1;
            }
        }
    }
    std::cout << "Put ok" << std::endl;
    return 0;
}        

int SplitPidGroup(const std::string& pid_group, std::set<uint32_t>& pid_set) {
    try {
        if (::rtidb::base::IsNumber(pid_group)) {
            pid_set.insert(boost::lexical_cast<uint32_t>(pid_group));
        } else if (pid_group.find('-') != std::string::npos) {
            std::vector<std::string> vec;
            boost::split(vec, pid_group, boost::is_any_of("-"));
            if (vec.size() != 2 || !::rtidb::base::IsNumber(vec[0]) || !::rtidb::base::IsNumber(vec[1])) {
                return -1;
            }
            uint32_t start_index = boost::lexical_cast<uint32_t>(vec[0]);
            uint32_t end_index = boost::lexical_cast<uint32_t>(vec[1]);
            while (start_index <= end_index) {
                pid_set.insert(start_index);
                start_index++;
            }
        } else if (pid_group.find(',') != std::string::npos) {
            std::vector<std::string> vec;
            boost::split(vec, pid_group, boost::is_any_of(","));
            for (const auto& pid_str : vec) {
                if (!::rtidb::base::IsNumber(pid_str)) {
                    return -1;
                }
                pid_set.insert(boost::lexical_cast<uint32_t>(pid_str));
            }
        } else {
            return -1;
        }
    } catch (const std::exception& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
        return -1;
    }
    return 0;
}

std::shared_ptr<::rtidb::client::TabletClient> GetTabletClient(const ::rtidb::nameserver::TableInfo& table_info,
            uint32_t pid, std::string& msg) {
    std::string endpoint;
    for (int idx = 0; idx < table_info.table_partition_size(); idx++) {
        if (table_info.table_partition(idx).pid() != pid) {
            continue;
        }
        for (int inner_idx = 0; inner_idx < table_info.table_partition(idx).partition_meta_size(); inner_idx++) {
            if (table_info.table_partition(idx).partition_meta(inner_idx).is_leader() && 
                     table_info.table_partition(idx).partition_meta(inner_idx).is_alive()) {
                endpoint = table_info.table_partition(idx).partition_meta(inner_idx).endpoint();
                break;
            }
        }
        break;
    }
    if (endpoint.empty()) {
        msg = "cannot find healthy endpoint. pid is " + std::to_string(pid);
        return std::shared_ptr<::rtidb::client::TabletClient>();
    }
    std::shared_ptr<::rtidb::client::TabletClient> tablet_client = std::make_shared<::rtidb::client::TabletClient>(endpoint);
    if (tablet_client->Init() < 0) {
        msg = "tablet client init failed, endpoint is " + endpoint;
        tablet_client.reset();
    }
    return tablet_client;
}

void HandleNSClientSetTTL(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "bad setttl format, eg settl t1 absolute 10" <<std::endl;
        return;
    }
    std::string ts_name;
    if (parts.size() == 5) {
        ts_name = parts[4];
    }
    try {
        std::string value;
        std::string err;
        uint64_t ttl = boost::lexical_cast<uint64_t>(parts[3]);
        bool ok = client->UpdateTTL(parts[1], parts[2], ttl, ts_name, err);
        if (ok) {
            std::cout << "Set ttl ok !" << std::endl;
        }else {
            std::cout << "Set ttl failed! "<< err << std::endl; 
        }
    } catch(std::exception const& e) {
        std::cout << "Invalid args ttl which should be uint64_t" << std::endl;
    }
}

void HandleNSClientCancelOP(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "bad cancelop format, eg cancelop 1002" <<std::endl;
        return;
    }
    try {
        std::string err;
        if (boost::lexical_cast<int64_t>(parts[1]) <= 0) {
            std::cout << "Invalid args. op_id should be large than zero" << std::endl;
            return;
        }
        uint64_t op_id = boost::lexical_cast<uint64_t>(parts[1]);
        bool ok = client->CancelOP(op_id, err);
        if (ok) {
            std::cout << "Cancel op ok!" << std::endl;
        } else {
            std::cout << "Cancel op failed! "<< err << std::endl; 
        }
    } catch(std::exception const& e) {
        std::cout << "Invalid args. op_id should be uint64_t" << std::endl;
    }
}

void HandleNSShowTablet(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    std::vector<std::string> row;
    row.push_back("endpoint");
    row.push_back("state");
    row.push_back("age");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    std::vector<::rtidb::client::TabletInfo> tablets;
    std::string msg;
    bool ok = client->ShowTablet(tablets, msg);
    if (!ok) {
        std::cout << "Fail to show tablets. error msg: " << msg << std::endl;
        return;
    }
    for (size_t i = 0; i < tablets.size(); i++) { 
        std::vector<std::string> row;
        row.push_back(tablets[i].endpoint);
        row.push_back(tablets[i].state);
        row.push_back(::rtidb::base::HumanReadableTime(tablets[i].age));
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleNSShowNameServer(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client, 
        std::shared_ptr<ZkClient> zk_client) {
    if (FLAGS_zk_cluster.empty() || !zk_client) {
        std::cout << "Show nameserver failed. zk_cluster is empty" << std::endl;
        return;
    }    
    std::string node_path = FLAGS_zk_root_path + "/leader";
    std::vector<std::string> children;
    if (!zk_client->GetChildren(node_path, children) || children.empty()) {
        std::cout << "get children failed" << std::endl;
        return;
    }
    std::vector<std::string> endpoint_vec;
    for (auto path : children) {
        std::string endpoint;
        std::string real_path = node_path + "/" + path;
        if (!zk_client->GetNodeValue(real_path, endpoint)) {
            std::cout << "get endpoint failed. path " << real_path << std::endl;
            return;
        }
        if (std::find(endpoint_vec.begin(), endpoint_vec.end(), endpoint) == endpoint_vec.end()) {
            endpoint_vec.push_back(endpoint);
        }
    }
    std::vector<std::string> row;
    row.push_back("endpoint");
    row.push_back("role");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    for (size_t i = 0; i < endpoint_vec.size(); i++) { 
        std::vector<std::string> row;
        row.push_back(endpoint_vec[i]);
        if (i == 0) {
            row.push_back("leader");
        } else {
            row.push_back("standby");
        }
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleNSMakeSnapshot(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    try {
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        std::string msg;
        bool ok = client->MakeSnapshot(parts[1], pid, msg);
        if (!ok) {
            std::cout << "Fail to makesnapshot. error msg:" << msg << std::endl;
            return;
        }
        std::cout << "MakeSnapshot ok" << std::endl;
    } catch(std::exception const& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
    } 
}

void HandleNSAddReplica(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::set<uint32_t> pid_set;
    if (SplitPidGroup(parts[2], pid_set) < 0) {
        printf("pid group[%s] format error\n", parts[2].c_str());
        return;
    }
    if (pid_set.empty()) {
        std::cout << "has not valid pid" << std::endl;
        return;
    }
    std::string msg;
    bool ok = client->AddReplica(parts[1], pid_set, parts[3], msg);
    if (!ok) {
        std::cout << "Fail to addreplica. error msg:" << msg  << std::endl;
        return;
    }
    std::cout << "AddReplica ok" << std::endl;
}

void HandleNSDelReplica(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::set<uint32_t> pid_set;
    if (SplitPidGroup(parts[2], pid_set) < 0) {
        printf("pid group[%s] format error\n", parts[2].c_str());
        return;
    }
    if (pid_set.empty()) {
        std::cout << "has not valid pid" << std::endl;
        return;
    }
    std::string msg;
    bool ok = client->DelReplica(parts[1], pid_set, parts[3], msg);
    if (!ok) {
        std::cout << "Fail to delreplica. error msg:" << msg << std::endl;
        return;
    }
    std::cout << "DelReplica ok" << std::endl;
}
    
void HandleNSClientDropTable(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    if (FLAGS_interactive) {
        printf("Drop table %s? yes/no\n", parts[1].c_str());
        std::string input;
        std::cin >> input;
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);
        if (input != "yes") {
            printf("'drop %s' cmd is canceled!\n", parts[1].c_str());
            return;
        }
    }
    std::string msg;
    bool ret = client->DropTable(parts[1], msg);
    if (!ret) {
        std::cout << "failed to drop. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "drop ok" << std::endl;
}

void HandleNSClientConfSet(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string msg;
    bool ret = client->ConfSet(parts[1], parts[2], msg);
    if (!ret) {
         printf("failed to set %s. error msg: %s\n", parts[1].c_str(), msg.c_str());
        return;
    }
    printf("set %s ok\n", parts[1].c_str());
}

void HandleNSClientConfGet(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 1) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string msg;
    std::map<std::string, std::string> conf_map;
    std::string key;
    if (parts.size() > 1) {
        key = parts[1];
    }
    bool ret = client->ConfGet(key, conf_map, msg);
    if (!ret) {
         printf("failed to set %s. error msg: %s\n", parts[1].c_str(), msg.c_str());
        return;
    }
    std::vector<std::string> row;
    row.push_back("key");
    row.push_back("value");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    for (const auto& kv : conf_map) {
        row.clear();
        row.push_back(kv.first);
        row.push_back(kv.second);
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleNSClientChangeLeader(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    try {
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        std::string msg;
        std::string candidate_leader;
        if (parts.size() > 3) {
            candidate_leader = parts[3];
        }
        bool ret = client->ChangeLeader(parts[1], pid, candidate_leader, msg);
        if (!ret) {
            std::cout << "failed to change leader. error msg: " << msg << std::endl;
            return;
        }
    } catch(const std::exception& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
        return;
    }
    std::cout << "change leader ok" << std::endl;
}   

void HandleNSClientOfflineEndpoint(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string msg;
    uint32_t concurrency = 0;
    if (parts.size() > 2) {
        try {
            if (boost::lexical_cast<int32_t>(parts[2]) <= 0) {
                std::cout << "Invalid args. concurrency should be greater than 0" << std::endl;
                return;
            }
            concurrency = boost::lexical_cast<uint32_t>(parts[2]);
        } catch (const std::exception& e) {
            std::cout << "Invalid args. concurrency should be uint32_t" << std::endl;
            return;
        }
    }
    bool ret = client->OfflineEndpoint(parts[1], concurrency, msg);
    if (!ret) {
        std::cout << "failed to offline endpoint. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "offline endpoint ok" << std::endl;
}

void HandleNSClientMigrate(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad format. eg, migrate 127.0.0.1:9991 table1 1-10 127.0.0.1:9992" << std::endl;
        return;
    }
    if (parts[1] == parts[4]) {
        std::cout << "migrate error. src_endpoint is same as des_endpoint" << std::endl;
        return;
    }
    std::string msg;
    std::set<uint32_t> pid_set;
    if (SplitPidGroup(parts[3], pid_set) < 0) {
        printf("pid group[%s] format error\n", parts[3].c_str());
        return;
    }
    if (pid_set.empty()) {
        std::cout << "has not valid pid" << std::endl;
        return;
    }
    bool ret = client->Migrate(parts[1], parts[2], pid_set, parts[4], msg);
    if (!ret) {
        std::cout << "failed to migrate partition. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "partition migrate ok" << std::endl;
}

void HandleNSClientRecoverEndpoint(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    bool need_restore = false;
    if (parts.size() > 2) {
        std::string value = parts[2];
        std::transform(value.begin(), value.end(), value.begin(), ::tolower);
        if (value == "true") {
            need_restore = true;
        } else if (value == "false") {
            need_restore = false;
        } else {
            std::cout << "Invalid args. need_restore should be true or false" << std::endl;
            return;
        }
    }
	uint32_t concurrency = 0;
    if (parts.size() > 3) {
        try {
            if (boost::lexical_cast<int32_t>(parts[3]) <= 0) {
                std::cout << "Invalid args. concurrency should be greater than 0" << std::endl;
                return;
            }
            concurrency = boost::lexical_cast<uint32_t>(parts[3]);
        } catch (const std::exception& e) {
            std::cout << "Invalid args. concurrency should be uint32_t" << std::endl;
            return;
        }
    }
    std::string msg;
    bool ret = client->RecoverEndpoint(parts[1], need_restore, concurrency, msg);
    if (!ret) {
        std::cout << "failed to recover endpoint. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "recover endpoint ok" << std::endl;
}    

void HandleNSClientRecoverTable(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    try {
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        std::string msg;
        bool ok = client->RecoverTable(parts[1], pid, parts[3], msg);
        if (!ok) {
            std::cout << "Fail to recover table. error msg:" << msg  << std::endl;
            return;
        }
        std::cout << "recover table ok" << std::endl;
    } catch(std::exception const& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
    } 
}    

void HandleNSClientConnectZK(const std::vector<std::string> parts, ::rtidb::client::NsClient* client) {
    std::string msg;
    bool ok = client->ConnectZK(msg);
    if (ok) {
        std::cout << "connect zk ok" << std::endl;
    } else {
        std::cout << "Fail to connect zk" << std::endl;
    }
}

void HandleNSClientDisConnectZK(const std::vector<std::string> parts, ::rtidb::client::NsClient* client) {
    std::string msg;
    bool ok = client->DisConnectZK(msg);
    if (ok) {
        std::cout << "disconnect zk ok" << std::endl;
    } else {
        std::cout << "Fail to disconnect zk" << std::endl;
    }
}

void HandleNSClientShowTable(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    std::string name;
    if (parts.size() >= 2) {
        name = parts[1];
    }
    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(name, tables, msg);
    if (!ret) {
        std::cout << "failed to showtable. error msg: " << msg << std::endl;
        return;
    }
    ::rtidb::base::PrintTableInfo(tables);
}

void HandleNSClientShowSchema(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "showschema format error. eg: showschema tablename" << std::endl;
        return;
    }
    std::string name = parts[1];
    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(name, tables, msg);
    if (!ret) {
        std::cout << "failed to showschema. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("table %s is not exist\n", name.c_str());
        return;
    }
    if (tables[0].column_desc_v1_size() > 0) {
        ::rtidb::base::PrintSchema(tables[0].column_desc_v1());
        printf("\n#ColumnKey\n");
        std::string ttl_suff = tables[0].ttl_type() == "kLatestTime" ? "" : "min";
        ::rtidb::base::PrintColumnKey(tables[0].ttl(), ttl_suff, tables[0].column_desc_v1(), tables[0].column_key());
    } else if (tables[0].column_desc_size() > 0) {
        ::rtidb::base::PrintSchema(tables[0].column_desc());
    } else {
        printf("table %s has not schema\n", name.c_str());
    }
}

void HandleNSDelete(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "delete format error. eg: delete table_name key | delete table_name key idx_name" << std::endl;
        return;
    }
    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(parts[1], tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("delete failed! table %s is not exist\n", parts[1].c_str());
        return;
    }
    uint32_t tid = tables[0].tid();
    std::string key = parts[2];
    uint32_t pid = (uint32_t)(::rtidb::base::hash64(key) % tables[0].table_partition_size());
    std::shared_ptr<::rtidb::client::TabletClient> tablet_client = GetTabletClient(tables[0], pid, msg);
    if (!tablet_client) {
        std::cout << "failed to delete. error msg: " << msg << std::endl;
        return;
    }
    std::string idx_name;
    if (tables[0].column_desc_size() >= 0 && parts.size() > 3) {
        std::vector<::rtidb::base::ColumnDesc> columns;
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0], columns) < 0) {
            std::cout << "convert table column desc failed" << std::endl; 
            return;
        }
        for (uint32_t i = 0; i < columns.size(); i++) {
            if (columns[i].name == parts[3]) {
                idx_name = parts[3];
                break;
            }
        }
        if (idx_name.empty()) {
            printf("idx_name %s is not exist\n", parts[3].c_str()); 
            return;
        }
    }
    msg.clear();
    if (tablet_client->Delete(tid, pid, key, idx_name, msg)) {
        std::cout << "delete ok" << std::endl;
    } else {
        std::cout << "delete failed. error msg: " << msg << std::endl; 
    }
}

void HandleNSGet(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 4) {
        std::cout << "get format error. eg: get table_name key ts | get table_name key idx_name ts | get table_name=xxx key=xxx index_name=xxx ts=xxx ts_name=xxx " << std::endl;
        return;
    }
    std::vector<std::string> temp_vec;
    ::rtidb::base::SplitString(parts[1],"=", &temp_vec);
    std::map<std::string, std::string> parameter_map;
    bool has_ts_col = false;
    if (temp_vec.size() == 2 && temp_vec[0] == "table_name" && !temp_vec[1].empty()) {
        has_ts_col = true;
        parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        for (uint32_t i = 2; i < parts.size(); i++) {
            ::rtidb::base::SplitString(parts[i],"=", &temp_vec);
            if (temp_vec.size() < 2 || temp_vec[1].empty()) {
                std::cout << "get format error. eg: get table_name=xxx key=xxx index_name=xxx ts=xxx ts_name=xxx " << std::endl;
                return;
            }
            parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        }
    }
    std::string table_name;
    std::string key;
    std::string index_name;
    uint64_t timestamp = 0;
    std::string ts_name;
    auto iter = parameter_map.begin();
    if (has_ts_col) {
		iter = parameter_map.find("table_name");
        if (iter != parameter_map.end()) {
            table_name = iter->second;
        } else {
            std::cout<<"get format error: table_name does not exist!"<<std::endl;
            return;
        }
        iter = parameter_map.find("key");
        if (iter != parameter_map.end()) {
            key = iter->second;
        } else {
            std::cout<<"get format error: key does not exist!"<<std::endl;
            return;
        }   
        iter = parameter_map.find("index_name");
        if (iter != parameter_map.end()) {
            index_name = iter->second;
        } else {
            std::cout<<"get format error: index_name does not exist!"<<std::endl;
            return;
        }
        iter = parameter_map.find("ts");
        if (iter != parameter_map.end()) {
            timestamp = boost::lexical_cast<uint64_t>(iter->second);
        } else {
            std::cout<<"get format error: ts does not exist!"<<std::endl;
            return;
        }
        iter = parameter_map.find("ts_name");
        if (iter != parameter_map.end()) {
            ts_name = iter->second;
        } else {
            std::cout<<"get format error: ts_name does not exist!"<<std::endl;
            return;
        }
    }
    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = false;
    if (has_ts_col) {
        ret = client->ShowTable(table_name, tables, msg);
    } else {
        ret = client->ShowTable(parts[1], tables, msg);
        key = parts[2];
    }
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("get failed! table %s is not exist\n", parts[1].c_str());
        return;
    }
    uint32_t tid = tables[0].tid();
    uint32_t pid = (uint32_t)(::rtidb::base::hash64(key) % tables[0].table_partition_size());
    std::shared_ptr<::rtidb::client::TabletClient> tablet_client = GetTabletClient(tables[0], pid, msg);
    if (!tablet_client) {
        std::cout << "failed to get. error msg: " << msg << std::endl;
        return;
    }
    if (tables[0].column_desc_size() == 0 && tables[0].column_desc_v1_size() == 0) {
        std::string value;
        uint64_t ts = 0;
        try {
            std::string msg;
            bool ok = tablet_client->Get(tid, pid, key,
                                  boost::lexical_cast<uint64_t>(parts[3]),
                                  value,
                                  ts,
                                  msg);
            if (ok) {
                if (tables[0].compress_type() == ::rtidb::nameserver::kSnappy) {
                    std::string uncompressed;
                    ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
                    value = uncompressed;
                }
                std::cout << "value :" << value << std::endl;
            } else {
                std::cout << "Get failed. error msg: " << msg << std::endl; 
            }
        } catch (std::exception const& e) {
            printf("Invalid args. ts should be unsigned int\n");
            return;
        } 
    } else {
        std::vector<::rtidb::base::ColumnDesc> columns;
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0], columns) < 0) {
            std::cout << "convert table column desc failed" << std::endl; 
            return;
        }
        ::baidu::common::TPrinter tp(columns.size() + 2, FLAGS_max_col_display_length);
        std::vector<std::string> row;
        row.push_back("#");
        row.push_back("ts");
        for (uint32_t i = 0; i < columns.size(); i++) {
            row.push_back(columns[i].name);
        }
        tp.AddRow(row);
        std::string value;
        uint64_t ts = 0;
        try {
            std::string msg;
            if (parts.size() == 6) {
                if (!tablet_client->Get(tid, pid, key, 
                               timestamp, index_name, ts_name, value, ts, msg)) {
                   std::cout << "Fail to get value! error msg: " << msg << std::endl;
                   return;
                }
            } else if (parts.size() == 5) {
                if (!tablet_client->Get(tid, pid, key, 
                                boost::lexical_cast<uint64_t>(parts[4]),
                                parts[3], value, ts, msg)) {
                    std::cout << "Fail to get value! error msg: " << msg << std::endl;
                    return;
                }
            } else {
                if (!tablet_client->Get(tid, pid, key,
                                  boost::lexical_cast<uint64_t>(parts[3]),
                                  value, ts, msg)) {
                    std::cout << "Fail to get value! error msg: " << msg << std::endl;
                    return;
                }
            }
        } catch (std::exception const& e) {
            printf("Invalid args. ts should be unsigned int\n");
            return;
        } 
        if (tables[0].compress_type() == ::rtidb::nameserver::kSnappy) {
            std::string uncompressed;
            ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
            value = uncompressed;
        }
        row.clear();
        row.push_back("1");
        row.push_back(std::to_string(ts));
        ::rtidb::base::FillTableRow(columns, value.c_str(), value.size(), row);
        tp.AddRow(row);
        tp.Print(true);
    }
}

void HandleNSScan(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 5) {
        std::cout << "scan format error. eg: scan table_name pk start_time end_time [limit] | scan table_name key key_name start_time end_time [limit] | scan table_name=xxx key=xxx index_name=xxx st=xxx et=xxx ts_name=xxx [limit=xxx]"  << std::endl;
        return;
    }
    std::vector<std::string> temp_vec;
    ::rtidb::base::SplitString(parts[1],"=", &temp_vec);
    std::map<std::string, std::string> parameter_map;
    bool has_ts_col = false;
    if (temp_vec.size() == 2 && temp_vec[0] == "table_name" && !temp_vec[1].empty()) {
        has_ts_col = true;
        parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        for (uint32_t i = 2; i < parts.size(); i++) {
            ::rtidb::base::SplitString(parts[i],"=", &temp_vec);
            if (temp_vec.size() < 2 || temp_vec[1].empty()) {
                std::cout << "scan table_name=xxx key=xxx index_name=xxx st=xxx et=xxx ts_name=xxx [limit=xxx]" << std::endl;
                return;
            }
            parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        }
    }
    std::string table_name;
    std::string key;
    std::string index_name;
    uint64_t st = 0;
    uint64_t et = 0;
    std::string ts_name;
    uint32_t limit = 0;
    auto iter = parameter_map.begin();
    if (has_ts_col) {
        iter = parameter_map.find("table_name");
        if (iter != parameter_map.end()) {
            table_name = iter->second;
        } else {
            std::cout<<"scan format error: table_name does not exist!"<<std::endl;
            return;
        }
        iter = parameter_map.find("key");
        if (iter != parameter_map.end()) {
            key = iter->second;
        } else {
            std::cout<<"scan format error: key does not exist!"<<std::endl;
            return;
        }   
        iter = parameter_map.find("index_name");
        if (iter != parameter_map.end()) {
            index_name = iter->second;
        } else {
            std::cout<<"scan format error: index_name does not exist!"<<std::endl;
            return;
        }
        iter = parameter_map.find("st");
        if (iter != parameter_map.end()) {
            st = boost::lexical_cast<uint64_t>(iter->second);
        } else {
            std::cout<<"scan format error: st does not exist!"<<std::endl;
            return;
        }
        iter = parameter_map.find("et");
        if (iter != parameter_map.end()) {
            et = boost::lexical_cast<uint64_t>(iter->second);
        } else {
            std::cout<<"scan format error: et does not exist!"<<std::endl;
            return;
        }
        iter = parameter_map.find("ts_name");
        if (iter != parameter_map.end()) {
            ts_name = iter->second;
        } else {
            std::cout<<"scan format error: ts_name does not exist!"<<std::endl;
            return;
        }
        iter = parameter_map.find("limit");
        if (iter != parameter_map.end()) {
            limit = boost::lexical_cast<uint32_t>(iter->second);
        }
    }

    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = false;
    if (has_ts_col) {
        ret = client->ShowTable(table_name, tables, msg);
    } else {
        ret = client->ShowTable(parts[1], tables, msg);
        key = parts[2];
    }
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("scan failed! table %s is not exist\n", parts[1].c_str());
        return;
    }
    uint32_t tid = tables[0].tid();
    uint32_t pid = (uint32_t)(::rtidb::base::hash64(key) % tables[0].table_partition_size());
    std::shared_ptr<::rtidb::client::TabletClient> tablet_client = GetTabletClient(tables[0], pid, msg);
    if (!tablet_client) {
        std::cout << "failed to scan. error msg: " << msg << std::endl;
        return;
    }
    if (tables[0].column_desc_size() == 0 && tables[0].column_desc_v1_size() == 0) {
        try {
            if (parts.size() > 5) {
                limit = boost::lexical_cast<uint32_t>(parts[5]);
            }
            std::string msg;
            ::rtidb::base::KvIterator* it = tablet_client->Scan(tid, pid, key,  
                    boost::lexical_cast<uint64_t>(parts[3]), 
                    boost::lexical_cast<uint64_t>(parts[4]),
                    limit, msg);
            if (it == NULL) {
                std::cout << "Fail to scan table. error msg: " << msg << std::endl;
            } else {
                ::rtidb::base::ShowTableRows(key, it, tables[0].compress_type());
                delete it;
            }
        } catch (std::exception const& e) {
            printf("Invalid args. st and et should be unsigned int\n");
            return;
        } 
    } else {
        std::vector<::rtidb::base::ColumnDesc> columns;
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0], columns) < 0) {
            std::cout << "convert table column desc failed" << std::endl; 
            return;
        }
        try {
            ::rtidb::base::KvIterator* it = NULL;
            std::string msg;
            if (has_ts_col) {
                it = tablet_client->Scan(tid, pid, key,  
                    st, et, index_name, ts_name, limit, msg);    
            } else {
                if (parts.size() > 6) {
                    limit = boost::lexical_cast<uint32_t>(parts[6]);
                }
                it = tablet_client->Scan(tid, pid, key,  
                        boost::lexical_cast<uint64_t>(parts[4]), 
                        boost::lexical_cast<uint64_t>(parts[5]),
                        parts[3], limit, msg); 
            }
            if (it == NULL) {
                std::cout << "Fail to scan table. error msg: " << msg << std::endl;
            } else {
                ::rtidb::base::ShowTableRows(columns, it, tables[0].compress_type());
                delete it;
            }
        } catch (std::exception const& e) {
            printf("Invalid args. st and et should be unsigned int\n");
        }
    }
}

void HandleNSCount(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "count format error" << std::endl;
        return;
    }
    std::string table_name;
    std::string key;
    std::string idx_name;
    std::string ts_name;
    bool filter_expired_data = false;
    uint64_t value = 0;
    std::vector<std::string> temp_vec;
    ::rtidb::base::SplitString(parts[1],"=", &temp_vec);
    bool has_ts_col = false;
    if (temp_vec[0] == "table_name") {
        has_ts_col = true; 
        if (parts.size() == 5 || parts.size() == 6) {
            table_name = temp_vec[1];
            ::rtidb::base::SplitString(parts[2],"=", &temp_vec);
            if (temp_vec[0] == "key") {
                key = temp_vec[1];
            }
            ::rtidb::base::SplitString(parts[3],"=", &temp_vec);
            if (temp_vec[0] == "index_name") {
                idx_name = temp_vec[1];
            }
            ::rtidb::base::SplitString(parts[4],"=", &temp_vec);
            if (temp_vec[0] == "ts_name") {
                ts_name = temp_vec[1];
            }    
        } 
        if (parts.size() == 6) {
            ::rtidb::base::SplitString(parts[5],"=", &temp_vec);
            if (temp_vec[0] == "filter_expired_data") {
                if (temp_vec[1] == "true") {
                    filter_expired_data = true;
                } else if (temp_vec[1] == "false") {
                    filter_expired_data = false;
                } else {
                    printf("filter_expired_data parameter should be true or false\n");
                    return;
                }   
            } 
        }
        if (parts.size() != 5 && parts.size() != 6) {
            std::cout << "count format error" << std::endl;
            return;
        }
    } else {
        table_name = parts[1];
        key = parts[2];
        if (parts.size() == 4) {
            if (parts[3] == "true") {
                filter_expired_data = true;
            } else if (parts[3] == "false") {
                filter_expired_data = false;
            } else {
                idx_name = parts[3];
            }
        } else if (parts.size() == 5) {
            idx_name =  parts[3];
            if (parts[4] == "true") {
                filter_expired_data = true;
            } else if (parts[4] == "false") {
                filter_expired_data = false;
            } else {
                printf("filter_expired_data parameter should be true or false\n");
                return;
            }
        } else if (parts.size() != 3){
            std::cout << "count format error" << std::endl;
            return;
        }
    }
    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(table_name, tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("get failed! table %s is not exist\n", parts[1].c_str());
        return;
    }
    uint32_t tid = tables[0].tid();
    uint32_t pid = (uint32_t)(::rtidb::base::hash64(key) % tables[0].table_partition_size());
    std::shared_ptr<::rtidb::client::TabletClient> tablet_client = GetTabletClient(tables[0], pid, msg);
    if (!tablet_client) {
        std::cout << "failed to count. cannot not found tablet client, pid is " << pid << std::endl;
        return;
    }
    bool ok = false;
    if (has_ts_col) {
        ok = tablet_client->Count(tid, pid, key, idx_name, ts_name, filter_expired_data, value, msg); 
    } else {
        ok = tablet_client->Count(tid, pid, key, idx_name, filter_expired_data, value, msg);
    }
    if (ok) {
        std::cout << "count: " << value << std::endl;
    } else {
        std::cout << "Count failed. error msg: " << msg << std::endl; 
    }
}
void HandleNSPreview(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "preview format error. eg: preview table_name [limit]" << std::endl;
        return;
    }
    uint32_t limit = FLAGS_preview_default_limit;
    if (parts.size() > 2) {
        try {
            int64_t tmp = boost::lexical_cast<int64_t>(parts[2]);
            if (tmp < 0) {
                printf("preview error. limit should be unsigned int\n");
                return;
            }
            limit = boost::lexical_cast<uint32_t>(parts[2]);
        } catch (std::exception const& e) {
            printf("preview error. limit should be unsigned int\n");
            return;
        }
        if (limit > FLAGS_preview_limit_max_num) {
            printf("preview error. limit is greater than the max num %u\n", FLAGS_preview_limit_max_num);
            return;
        } else if (limit == 0) {
            printf("preview error. limit must be greater than zero\n");
            return;
        }
    }
    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(parts[1], tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("preview failed! table %s is not exist\n", parts[1].c_str());
        return;
    }
    uint32_t tid = tables[0].tid();
    std::vector<::rtidb::base::ColumnDesc> columns;
    if (tables[0].column_desc_v1_size() > 0 || tables[0].column_desc_size() > 0) {
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0], columns) < 0) {
            std::cout << "convert table column desc failed" << std::endl; 
            return;
        }
    }
    bool has_ts_col = ::rtidb::base::SchemaCodec::HasTSCol(columns);
    std::vector<std::string> row;
    if (columns.empty()) {
        row.push_back("#");
        row.push_back("key");
        row.push_back("ts");
        row.push_back("data");
    } else {
        row.push_back("#");
        if (!has_ts_col) {
            row.push_back("ts");
        }    
        for (uint32_t i = 0; i < columns.size(); i++) {
            row.push_back(columns[i].name);
        }
    }
    ::baidu::common::TPrinter tp(row.size(), FLAGS_max_col_display_length);
    tp.AddRow(row);
    uint32_t index = 1;
    for (uint32_t pid = 0; pid < (uint32_t)tables[0].table_partition_size(); pid++) {
        if (limit == 0) {
            break;
        }
        std::shared_ptr<::rtidb::client::TabletClient> tablet_client = GetTabletClient(tables[0], pid, msg);
        if (!tablet_client) {
            std::cout << "failed to preview. error msg: " << msg << std::endl;
            return;
        }
        uint32_t count = 0;
        ::rtidb::base::KvIterator* it = tablet_client->Traverse(tid, pid, "", "", 0, limit, count);
        if (it == NULL) {
            std::cout << "Fail to preview table" << std::endl;
            return;
        }
        limit -= count;
        while (it->Valid()) {
            row.clear();
            row.push_back(std::to_string(index));
            if (columns.empty()) {
                std::string value = it->GetValue().ToString();
                if (tables[0].compress_type() == ::rtidb::nameserver::kSnappy) {
                    std::string uncompressed;
                    ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
                    value = uncompressed;
                }
                row.push_back(it->GetPK());
                row.push_back(std::to_string(it->GetKey()));
                row.push_back(value);
            } else {
                if (!has_ts_col) {
                    row.push_back(std::to_string(it->GetKey()));
                }    
                if (tables[0].compress_type() == ::rtidb::nameserver::kSnappy) {
                    std::string uncompressed;
                    ::snappy::Uncompress(it->GetValue().data(), it->GetValue().size(), &uncompressed);
                    ::rtidb::base::FillTableRow(columns, uncompressed.c_str(), uncompressed.length(), row); 
                } else {
                    ::rtidb::base::FillTableRow(columns, it->GetValue().data(), it->GetValue().size(), row); 
                }
            }
            tp.AddRow(row);
            index++;
            it->Next();
        }
        delete it;
    }
    tp.Print(true);
}


void HandleNSPut(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 5) {
        std::cout << "put format error. eg: put table_name pk ts value | put table_name [ts] field1 field2 ..." << std::endl;
        return;
    }
    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::string msg;
    bool ret = client->ShowTable(parts[1], tables, msg);
    if (!ret) {
        std::cout << "failed to get table info. error msg: " << msg << std::endl;
        return;
    }
    if (tables.empty()) {
        printf("put failed! table %s is not exist\n", parts[1].c_str());
        return;
    }
    uint32_t tid = tables[0].tid();
    if (tables[0].column_desc_v1_size() > 0) {
        uint64_t ts = 0;
        uint32_t start_index = 0;
        if (parts.size() == (uint32_t)tables[0].column_desc_v1_size() + 3) {
            try {
                ts = boost::lexical_cast<uint64_t>(parts[2]);
            } catch (std::exception const& e) {
                printf("Invalid args. ts %s should be unsigned int\n", parts[2].c_str());
                return;
            } 
            start_index = 3;
        } else if (parts.size() == (uint32_t)tables[0].column_desc_v1_size() + 2) {
            start_index = 2;
        } else {
            printf("put format error! input value does not match the schema\n");
            return;
        }
        std::vector<::rtidb::base::ColumnDesc> columns;
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0].column_desc_v1(), columns) < 0) {
            std::cout << "convert table column desc failed" << std::endl; 
            return;
        }
        std::string buffer;
        std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>> dimensions;
        std::vector<uint64_t> ts_dimensions;
        if (EncodeMultiDimensionData(std::vector<std::string>(parts.begin() + start_index, parts.end()), columns, 
                    tables[0].table_partition_size(), buffer, dimensions, ts_dimensions) < 0) {
            std::cout << "Encode data error" << std::endl;
            return;
        }
        if (tables[0].column_key_size() > 0) {
            std::map<std::string, std::string> raw_value;
            for (uint32_t idx = start_index; idx < parts.size(); idx++) {
                raw_value.insert(std::make_pair(tables[0].column_desc_v1(idx - start_index).name(), parts[idx]));
            }
            dimensions.clear();
            if (SetDimensionData(raw_value, tables[0].column_key(), 
                        tables[0].table_partition_size(), dimensions) < 0) {
                std::cout << "Set dimension data error" << std::endl;
                return;
            }
        }
        std::string value = buffer;
        if (tables[0].compress_type() == ::rtidb::nameserver::kSnappy) {
            std::string compressed;
            ::snappy::Compress(value.c_str(), value.length(), &compressed);
            value = compressed;
        }
        PutData(tid, dimensions, ts_dimensions, ts, value, tables[0].table_partition());
    } else if (tables[0].column_desc_size() > 0) {
        uint64_t ts = 0;
        try {
            ts = boost::lexical_cast<uint64_t>(parts[2]);
        } catch (std::exception const& e) {
            printf("Invalid args. ts %s should be unsigned int\n", parts[2].c_str());
            return;
        } 
        std::vector<::rtidb::base::ColumnDesc> columns;
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0], columns) < 0) {
            std::cout << "convert table column desc failed" << std::endl; 
            return;
        }
        std::string buffer;
        uint32_t cnt = parts.size() - 3;
        if (cnt != columns.size()) {
            std::cout << "Input value mismatch schema" << std::endl;
            return;
        }
        std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>> dimensions;
        if (EncodeMultiDimensionData(std::vector<std::string>(parts.begin() + 3, parts.end()), columns, 
                    tables[0].table_partition_size(), buffer, dimensions) < 0) {
            std::cout << "Encode data error" << std::endl;
            return;
        }
        std::string value = buffer;
        if (tables[0].compress_type() == ::rtidb::nameserver::kSnappy) {
            std::string compressed;
            ::snappy::Compress(value.c_str(), value.length(), &compressed);
            value = compressed;
        }
        PutData(tid, dimensions, std::vector<uint64_t>(), ts, value, tables[0].table_partition());
    } else {
        std::string pk = parts[2];
        uint64_t ts = 0;
        try {
            ts = boost::lexical_cast<uint64_t>(parts[3]);
        } catch (std::exception const& e) {
            printf("Invalid args. ts %s should be unsigned int\n", parts[3].c_str());
            return;
        } 
        uint32_t pid = (uint32_t)(::rtidb::base::hash64(pk) % tables[0].table_partition_size());
        std::shared_ptr<::rtidb::client::TabletClient> tablet_client = GetTabletClient(tables[0], pid, msg);
        if (!tablet_client) {
            std::cout << "Failed to put. error msg: " << msg << std::endl;
            return;
        }
        std::string value = parts[4];
        if (tables[0].compress_type() == ::rtidb::nameserver::kSnappy) {
            std::string compressed;
            ::snappy::Compress(value.c_str(), value.length(), &compressed);
            value = compressed;
        }
        if (tablet_client->Put(tid, pid, pk, ts, value)) {
            std::cout << "Put ok" << std::endl;
        } else {
            std::cout << "Put failed" << std::endl; 
        }
    }
}

int GenTableInfo(const std::string& path, const std::set<std::string>& type_set, 
            ::rtidb::nameserver::TableInfo& ns_table_info) {
    ::rtidb::client::TableInfo table_info;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cout << "can not open file " << path << std::endl;
        return -1;
    }
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    if (!google::protobuf::TextFormat::Parse(&fileInput, &table_info)) {
        std::cout << "table meta file format error" << std::endl;
        return -1;
    }

    ns_table_info.set_name(table_info.name());
    std::string ttl_type = table_info.ttl_type();
    std::transform(ttl_type.begin(), ttl_type.end(), ttl_type.begin(), ::tolower);
    uint32_t default_skiplist_height = FLAGS_absolute_default_skiplist_height;
    if (ttl_type == "kabsolutetime") {
        ns_table_info.set_ttl_type("kAbsoluteTime");
    } else if (ttl_type == "klatesttime" || ttl_type == "latest") {
        ns_table_info.set_ttl_type("kLatestTime");
        default_skiplist_height = FLAGS_latest_default_skiplist_height;
    } else {
        printf("ttl type %s is invalid\n", table_info.ttl_type().c_str());
        return -1;
    }
    ns_table_info.set_ttl(table_info.ttl());
    std::string compress_type = table_info.compress_type();
    std::transform(compress_type.begin(), compress_type.end(), compress_type.begin(), ::tolower);
    if (compress_type == "knocompress" || compress_type == "nocompress" || compress_type == "no") {
        ns_table_info.set_compress_type(::rtidb::nameserver::kNoCompress);
    } else if (compress_type == "ksnappy" || compress_type == "snappy") {
        ns_table_info.set_compress_type(::rtidb::nameserver::kSnappy);
    } else {
        printf("compress type %s is invalid\n", table_info.compress_type().c_str());
        return -1;
    }
    ns_table_info.set_storage_mode(table_info.storage_mode());
    if (table_info.has_key_entry_max_height()) {
        if (table_info.key_entry_max_height() > FLAGS_skiplist_max_height) {
            printf("Fail to create table. key_entry_max_height %u is greater than the max heght %u\n", 
                        table_info.key_entry_max_height(), FLAGS_skiplist_max_height);
            return -1;
        }
        if (table_info.key_entry_max_height() == 0) {
            printf("Fail to create table. key_entry_max_height must be greater than 0\n"); 
            return -1;
        }
        ns_table_info.set_key_entry_max_height(table_info.key_entry_max_height());
    }else {
        // config default height
        ns_table_info.set_key_entry_max_height(default_skiplist_height);
    }
    ns_table_info.set_seg_cnt(table_info.seg_cnt());
    if (table_info.table_partition_size() > 0) {
        std::map<uint32_t, std::string> leader_map;
        std::map<uint32_t, std::set<std::string>> follower_map;
        for (int idx = 0; idx < table_info.table_partition_size(); idx++) {
            std::string pid_group = table_info.table_partition(idx).pid_group();
            uint32_t start_index = 0;
            uint32_t end_index = 0;
            if (::rtidb::base::IsNumber(pid_group)) {
                start_index = boost::lexical_cast<uint32_t>(pid_group);
                end_index = start_index;
            } else {
                std::vector<std::string> vec;
                boost::split(vec, pid_group, boost::is_any_of("-"));
                if (vec.size() != 2 || !::rtidb::base::IsNumber(vec[0]) || !::rtidb::base::IsNumber(vec[1])) {
                    printf("Fail to create table. pid_group[%s] format error.\n", pid_group.c_str());
                    return -1;
                }
                start_index = boost::lexical_cast<uint32_t>(vec[0]);
                end_index = boost::lexical_cast<uint32_t>(vec[1]);

            }
            for (uint32_t pid = start_index; pid <= end_index; pid++) {
                if (table_info.table_partition(idx).is_leader()) {
                    if (leader_map.find(pid) != leader_map.end()) {
                        printf("Fail to create table. pid %u has two leader\n", pid);
                        return -1;
                    }
                    leader_map.insert(std::make_pair(pid, table_info.table_partition(idx).endpoint()));
                } else {
                    if (follower_map.find(pid) == follower_map.end()) {
                        follower_map.insert(std::make_pair(pid, std::set<std::string>()));
                    }
                    if (follower_map[pid].find(table_info.table_partition(idx).endpoint()) != follower_map[pid].end()) {
                        printf("Fail to create table. pid %u has same follower on %s\n", pid, table_info.table_partition(idx).endpoint().c_str());
                        return -1;
                    }
                    follower_map[pid].insert(table_info.table_partition(idx).endpoint());
                }
            }
        }    
        if (leader_map.empty()) {
            printf("Fail to create table. has not leader pid\n");
            return -1;
        }
        // check leader pid
        auto iter = leader_map.rbegin();
        if (iter->first != leader_map.size() -1) {
            printf("Fail to create table. pid is not start with zero and consecutive\n");
            return -1;
        }

        // check follower's leader 
        for (const auto& kv : follower_map) {
            auto iter = leader_map.find(kv.first);
            if (iter == leader_map.end()) {
                printf("pid %u has not leader\n", kv.first);
                return -1;
            }
            if (kv.second.find(iter->second) != kv.second.end()) {
                printf("pid %u leader and follower at same endpoint %s\n", kv.first, iter->second.c_str());
                return -1;
            }
        }

        for (const auto& kv : leader_map) {
            ::rtidb::nameserver::TablePartition* table_partition = ns_table_info.add_table_partition();
            table_partition->set_pid(kv.first);
            ::rtidb::nameserver::PartitionMeta* partition_meta = table_partition->add_partition_meta();
            partition_meta->set_endpoint(kv.second);
            partition_meta->set_is_leader(true);
            auto iter = follower_map.find(kv.first);
            if (iter == follower_map.end()) {
                continue;
            }
            // add follower
            for (const auto& endpoint : iter->second) {
                ::rtidb::nameserver::PartitionMeta* partition_meta = table_partition->add_partition_meta();
                partition_meta->set_endpoint(endpoint);
                partition_meta->set_is_leader(false);
            }
        }
    } else {
        if (table_info.has_partition_num()) {
            ns_table_info.set_partition_num(table_info.partition_num());
        }
        if (table_info.has_replica_num()) {
            ns_table_info.set_replica_num(table_info.replica_num());
        }
    }

    std::map<std::string, std::string> name_map;
    std::set<std::string> index_set;
    std::set<std::string> ts_col_set;
    for (int idx = 0; idx < table_info.column_desc_size(); idx++) {
        std::string cur_type = table_info.column_desc(idx).type();
        std::transform(cur_type.begin(), cur_type.end(), cur_type.begin(), ::tolower);
        if (type_set.find(cur_type) == type_set.end()) {
            printf("type %s is invalid\n", table_info.column_desc(idx).type().c_str());
            return -1;
        }
        if (table_info.column_desc(idx).name() == "" || 
                name_map.find(table_info.column_desc(idx).name()) != name_map.end()) {
            printf("check column_desc name failed. name is %s\n", table_info.column_desc(idx).name().c_str());
            return -1;
        }
        if (table_info.column_desc(idx).add_ts_idx() && ((cur_type == "float") || (cur_type == "double"))) {
            printf("float or double column can not be index: %s\n", cur_type.c_str());
            return -1;
        }
        if (table_info.column_desc(idx).add_ts_idx()) {
            index_set.insert(table_info.column_desc(idx).name());
        }
        if (table_info.column_desc(idx).is_ts_col()) {
            if (table_info.column_desc(idx).add_ts_idx()) {
                printf("index column cannot be ts column\n");
                return -1;
            }
            if (cur_type != "timestamp" && cur_type != "int64" && cur_type != "uint64") {
                printf("ts column type should be int64, uint64 or timestamp\n");
                return -1;
            }
            if (table_info.column_desc(idx).has_ttl()) {
                if (ns_table_info.ttl_type() == "kAbsoluteTime") {
                    if (table_info.column_desc(idx).ttl() > FLAGS_absolute_ttl_max) {
                        printf("the max ttl is %u\n", FLAGS_absolute_ttl_max);
                        return -1;
                    }
                } else {
                    if (table_info.column_desc(idx).ttl() > FLAGS_latest_ttl_max) {
                        printf("the max ttl is %u\n", FLAGS_latest_ttl_max);
                        return -1;
                    }
                }
            }
            ts_col_set.insert(table_info.column_desc(idx).name());
        }
        name_map.insert(std::make_pair(table_info.column_desc(idx).name(), cur_type));
        ::rtidb::common::ColumnDesc* column_desc = ns_table_info.add_column_desc_v1();
        column_desc->CopyFrom(table_info.column_desc(idx));
    }
    if (ts_col_set.size() > 1 && table_info.column_key_size() == 0) {
        printf("column_key should be set when has two or more ts columns\n");
        return -1;
    }
    if (table_info.column_key_size() > 0) {
        index_set.clear();
        std::set<std::string> key_set;
        for (int idx = 0; idx < table_info.column_key_size(); idx++) {
            if (!table_info.column_key(idx).has_index_name() ||
                    table_info.column_key(idx).index_name().size() == 0) {
                printf("not set index_name in column_key\n");
                return -1;
            }
            if (index_set.find(table_info.column_key(idx).index_name()) != index_set.end()) {
                printf("duplicate index_name %s\n", table_info.column_key(idx).index_name().c_str());
                return -1;
            }
            index_set.insert(table_info.column_key(idx).index_name());
            std::string cur_key;
            if (table_info.column_key(idx).col_name_size() > 0) {
                for (const auto& name : table_info.column_key(idx).col_name()) {
                    auto iter = name_map.find(name);
                    if (iter == name_map.end()) {
                        printf("column :%s is not member of columns\n", name.c_str());
                        return -1;
                    }
                    if ((iter->second == "float") || (iter->second == "double")) {
                        printf("float or double column can not be index\n");
                        return -1;
                    }
                    if (cur_key.empty()) {
                        cur_key = name;
                    } else {
                        cur_key += "|" + name;
                    }
                }
            } else {
                cur_key = table_info.column_key(idx).index_name();
            }
            if (key_set.find(cur_key) != key_set.end()) {
                printf("duplicate column_key\n");
                return -1;
            }
            key_set.insert(cur_key);
            for (const auto& ts_name : table_info.column_key(idx).ts_name()) {
                if (ts_col_set.find(ts_name) == ts_col_set.end()) {
                    printf("invalid ts_name %s\n", ts_name.c_str());
                    return -1;
                }
            }
            ::rtidb::common::ColumnKey* column_key = ns_table_info.add_column_key();
            column_key->CopyFrom(table_info.column_key(idx));
        }
    }
    if (index_set.empty() && table_info.column_desc_size() > 0) {
        std::cout << "no index" << std::endl;
        return -1;
    }
    return 0;
}

void HandleNSCreateTable(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    std::set<std::string> type_set;
    type_set.insert("int32");
    type_set.insert("uint32");
    type_set.insert("int64");
    type_set.insert("uint64");
    type_set.insert("float");
    type_set.insert("double");
    type_set.insert("string");
    type_set.insert("bool");
    type_set.insert("timestamp");
    type_set.insert("date");
    type_set.insert("int16");
    type_set.insert("uint16");
    ::rtidb::nameserver::TableInfo ns_table_info;
    if (parts.size() == 2) {
        if (GenTableInfo(parts[1], type_set, ns_table_info) < 0) {
            return;
        }
    } else if (parts.size() > 4) {
        ns_table_info.set_name(parts[1]);
        std::string type = "kAbsoluteTime";
        try {
            std::vector<std::string> vec;
            ::rtidb::base::SplitString(parts[2], ":", &vec);
            if (vec.size() > 1) {
                if ((vec[0] == "latest" || vec[0] == "kLatestTime"))  {
                    type = "kLatestTime";
                } else {
                    std::cout << "invalid ttl type" << std::endl;
                    return;
                }    
            }
            ns_table_info.set_ttl(boost::lexical_cast<uint64_t>(vec[vec.size() - 1]));
            uint32_t partition_num = boost::lexical_cast<uint32_t>(parts[3]);
            if (partition_num == 0) {
                 std::cout << "partition_num should be large than zero" << std::endl;
                 return;
            }
            ns_table_info.set_partition_num(partition_num);
            uint32_t replica_num = boost::lexical_cast<uint32_t>(parts[4]);
            if (replica_num == 0) {
                 std::cout << "replica_num should be large than zero" << std::endl;
                 return;
            }
            ns_table_info.set_replica_num(replica_num);
        } catch (std::exception const& e) {
            std::cout << "Invalid args. pid should be uint32_t" << std::endl;
            return;
        } 
        ns_table_info.set_ttl_type(type);
        bool has_index = false;
        std::set<std::string> name_set;
        for (uint32_t i = 5; i < parts.size(); i++) {
            std::vector<std::string> kv;
            ::rtidb::base::SplitString(parts[i], ":", &kv);
            if (kv.size() < 2) {
                std::cout << "create failed! schema format is illegal" << std::endl;
                return;
            }
            if (name_set.find(kv[0]) != name_set.end()) {
                printf("Duplicated column %s\n", kv[0].c_str());
                return;
            }
            std::string cur_type = kv[1];
            std::transform(cur_type.begin(), cur_type.end(), cur_type.begin(), ::tolower);
            if (type_set.find(cur_type) == type_set.end()) {
                printf("type %s is invalid\n", kv[1].c_str());
                return;
            }
            name_set.insert(kv[0]);
            ::rtidb::nameserver::ColumnDesc* column_desc = ns_table_info.add_column_desc();
            column_desc->set_name(kv[0]);
            column_desc->set_add_ts_idx(false);
            column_desc->set_type(cur_type);

            if (kv.size() > 2 && kv[2] == "index") {
                if ((cur_type == "float") || (cur_type == "double")) {
                    printf("float or double column can not be index\n");
                    return;
                }
                column_desc->set_add_ts_idx(true);
                has_index = true;
            }
        }
        if (parts.size() > 5 && !has_index) {
            std::cout << "create failed! schema has no index" << std::endl;
            return;
        }
    } else {
        std::cout << "create format error! ex: create table_meta_file | create name ttl partition_num replica_num [name:type:index ...]" << std::endl;
        return;
    }
    if (ns_table_info.ttl_type() == "kAbsoluteTime") {
        if (ns_table_info.ttl() > FLAGS_absolute_ttl_max) {
            std::cout << "Create failed. The max num of AbsoluteTime ttl is " 
                      << FLAGS_absolute_ttl_max << std::endl;
            return;
        }
    } else {
        if (ns_table_info.ttl() > FLAGS_latest_ttl_max) {
            std::cout << "Create failed. The max num of latest LatestTime is " 
                      << FLAGS_latest_ttl_max << std::endl;
            return;
        }
    }
    std::string msg;
    if (!client->CreateTable(ns_table_info, msg)) {
        std::cout << "Fail to create table. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "Create table ok" << std::endl;
}

void HandleNSClientHelp(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() == 1) {
        printf("addreplica - add replica to leader\n");
        printf("cancelop - cancel the op\n");
        printf("create - create table\n");
        printf("confset - update conf\n");
        printf("confget - get conf\n");
        printf("count - count the num of data in specified key\n");
        printf("changeleader - select leader again when the endpoint of leader offline\n");
        printf("delete - delete pk\n");
        printf("delreplica - delete replica from leader\n");
        printf("drop - drop table\n");
        printf("exit - exit client\n");
        printf("get - get only one record\n");
        printf("gettablepartition - get partition info\n");
        printf("help - get cmd info\n");
        printf("makesnapshot - make snapshot\n");
        printf("migrate - migrate partition form one endpoint to another\n");
        printf("man - get cmd info\n");
        printf("offlineendpoint - select leader and delete replica when endpoint offline\n");
        printf("preview - preview data\n");
        printf("put -  insert data into table\n");
        printf("quit - exit client\n");
        printf("recovertable - recover only one table partition\n");
        printf("recoverendpoint - recover all tables in endpoint when online\n");
        printf("scan - get records for a period of time\n");
        printf("showtable - show table info\n");
        printf("showtablet - show tablet info\n");
        printf("showns - show nameserver info\n");
        printf("showschema - show schema info\n");
        printf("showopstatus - show op info\n");
        printf("settablepartition - update partition info\n");
        printf("setttl - set table ttl\n");
        printf("updatetablealive - update table alive status\n");
    } else if (parts.size() == 2) {
        if (parts[1] == "create") {
            printf("desc: create table\n");
            printf("usage: create table_meta_file_path\n");
            printf("usage: create table_name ttl partition_num replica_num [colum_name1:type:index colum_name2:type ...]\n");
            printf("ex: create ./table_meta.txt\n");
            printf("ex: create table1 144000 8 3\n");
            printf("ex: create table2 latest:10 8 3\n");
            printf("ex: create table3 latest:10 8 3 card:string:index mcc:string:index value:float\n");
        } else if (parts[1] == "drop") {
            printf("desc: drop table\n");
            printf("usage: drop table_name\n");
            printf("ex: drop table1\n");
        } else if (parts[1] == "put") {
            printf("desc: insert data into table\n");
            printf("usage: put table_name pk ts value\n");
            printf("usage: put table_name ts key1 key2 ... value1 value2 ...\n");
            printf("ex: put table1 key1 1528872944000 value1\n");
            printf("ex: put table2 1528872944000 card0 mcc0 1.3\n");
        } else if (parts[1] == "scan") {
            printf("desc: get records for a period of time\n");
            printf("usage: scan table_name pk start_time end_time [limit]\n");
            printf("usage: scan table_name key key_name start_time end_time [limit]\n");
            printf("ex: scan table1 key1 1528872944000 1528872930000\n");
            printf("ex: scan table1 key1 1528872944000 1528872930000 10\n");
            printf("ex: scan table1 key1 0 0 10\n");
            printf("ex: scan table2 card0 card 1528872944000 1528872930000\n");
            printf("ex: scan table2 card0 card 1528872944000 1528872930000 10\n");
            printf("ex: scan table2 card0 card  0 0 10\n");
        } else if (parts[1] == "get") {
            printf("desc: get only one record\n");
            printf("usage: get table_name key ts\n");
            printf("usage: get table_name key idx_name ts\n");
            printf("ex: get table1 key1 1528872944000\n");
            printf("ex: get table1 key1 0\n");
            printf("ex: get table2 card0 card 1528872944000\n");
            printf("ex: get table2 card0 card 0\n");
        } else if (parts[1] == "delete") {
            printf("desc: delete pk\n");
            printf("usage: delete table_name key idx_name\n");
            printf("ex: delete table1 key1\n");
            printf("ex: delete table2 card0 card\n");
        } else if (parts[1] == "count") {
            printf("desc: count the num of data in specified key\n");
            printf("usage: count table_name key [filter_expired_data]\n");
            printf("usage: count table_name key idx_name [filter_expired_data]\n");
            printf("ex: count table1 key1\n");
            printf("ex: count table1 key1 true\n");
            printf("ex: count table2 card0 card\n");
            printf("ex: count table2 card0 card true\n");
        } else if (parts[1] == "preview") {
            printf("desc: preview data in table\n");
            printf("usage: preview table_name [limit]\n");
            printf("ex: preview table1\n");
            printf("ex: preview table1 10\n");
        } else if (parts[1] == "showtable") {
            printf("desc: show table info\n");
            printf("usage: showtable [table_name]\n");
            printf("ex: showtable\n");
            printf("ex: showtable table1\n");
        } else if (parts[1] == "showtablet") {
            printf("desc: show tablet info\n");
            printf("usage: showtablet\n");
            printf("ex: showtablet\n");
        } else if (parts[1] == "showns") {
            printf("desc: show nameserver info\n");
            printf("usage: showns\n");
            printf("ex: showns\n");
        } else if (parts[1] == "showschema") {
            printf("desc: show schema info\n");
            printf("usage: showschema table_name\n");
            printf("ex: showschema table1\n");
        } else if (parts[1] == "showopstatus") {
            printf("desc: show op info\n");
            printf("usage: showopstatus [table_name pid]\n");
            printf("ex: showopstatus\n");
            printf("ex: showopstatus table1\n");
            printf("ex: showopstatus table1 0\n");
        } else if (parts[1] == "makesnapshot") {
            printf("desc: make snapshot\n");
            printf("usage: makesnapshot name pid\n");
            printf("ex: makesnapshot table1 0\n");
        } else if (parts[1] == "addreplica") {
            printf("desc: add replica to leader\n");
            printf("usage: addreplica name pid_group endpoint\n");
            printf("ex: addreplica table1 0 172.27.128.31:9527\n");
            printf("ex: addreplica table1 0,3,5 172.27.128.31:9527\n");
            printf("ex: addreplica table1 1-5 172.27.128.31:9527\n");
        } else if (parts[1] == "delreplica") {
            printf("desc: delete replica from leader\n\n");
            printf("usage: delreplica name pid_group endpoint\n");
            printf("ex: delreplica table1 0 172.27.128.31:9527\n");
            printf("ex: delreplica table1 0,3,5 172.27.128.31:9527\n");
            printf("ex: delreplica table1 1-5 172.27.128.31:9527\n");
        } else if (parts[1] == "confset") {
            printf("desc: update conf\n");
            printf("usage: confset auto_failover true/false\n");
            printf("ex: confset auto_failover true\n");
        } else if (parts[1] == "confget") {
            printf("desc: get conf\n");
            printf("usage: confget\n");
            printf("usage: confget conf_name\n");
            printf("ex: confget\n");
            printf("ex: confget auto_failover\n");
        } else if (parts[1] == "changeleader") {
            printf("desc: select leader again when the endpoint of leader offline\n");
            printf("usage: changeleader table_name pid [candidate_leader]\n");
            printf("ex: changeleader table1 0\n");
            printf("ex: changeleader table1 0 auto\n");
            printf("ex: changeleader table1 0 172.27.128.31:9527\n");
        } else if (parts[1] == "offlineendpoint") {
            printf("desc: select leader and delete replica when endpoint offline\n");
            printf("usage: offlineendpoint endpoint [concurrency]\n");
            printf("ex: offlineendpoint 172.27.128.31:9527\n");
            printf("ex: offlineendpoint 172.27.128.31:9527 2\n");
        } else if (parts[1] == "recovertable") {
            printf("desc: recover only one table partition\n");
            printf("usage: recovertable table_name pid endpoint\n");
            printf("ex: recovertable table1 0 172.27.128.31:9527\n");
        } else if (parts[1] == "recoverendpoint") {
            printf("desc: recover all tables in endpoint when online\n");
            printf("usage: recoverendpoint endpoint [need_restore] [concurrency]\n");
            printf("ex: recoverendpoint 172.27.128.31:9527\n");
            printf("ex: recoverendpoint 172.27.128.31:9527 false\n");
            printf("ex: recoverendpoint 172.27.128.31:9527 true 2\n");
        } else if (parts[1] == "migrate") {
            printf("desc: migrate partition form one endpoint to another\n");
            printf("usage: migrate src_endpoint table_name pid_group des_endpoint\n");
            printf("ex: migrate 172.27.2.52:9991 table1 1 172.27.2.52:9992\n");
            printf("ex: migrate 172.27.2.52:9991 table1 1,3,5 172.27.2.52:9992\n");
            printf("ex: migrate 172.27.2.52:9991 table1 1-5 172.27.2.52:9992\n");
        } else if (parts[1] == "gettablepartition") {
            printf("desc: get partition info\n");
            printf("usage: gettablepartition table_name pid\n");
            printf("ex: gettablepartition table1 0\n");
        } else if (parts[1] == "settablepartition") {
            printf("desc: set partition info\n");
            printf("usage: settablepartition table_name partition_file_path\n");
            printf("ex: settablepartition table1 ./partition_file.txt\n");
        } else if (parts[1] == "exit" || parts[1] == "quit") {
            printf("desc: exit client\n");
            printf("ex: quit\n");
            printf("ex: exit\n");
        } else if (parts[1] == "help" || parts[1] == "man") {
            printf("desc: get cmd info\n");
            printf("usage: help [cmd]\n");
            printf("usage: man [cmd]\n");
            printf("ex:help\n");
            printf("ex:help create\n");
            printf("ex:man\n");
            printf("ex:man create\n");
        } else if (parts[1] == "setttl") {
            printf("desc: set table ttl \n");
            printf("usage: setttl table_name ttl_type ttl [ts_name]\n");
            printf("ex: setttl t1 absolute 10\n");
            printf("ex: setttl t2 latest 5\n");
            printf("ex: setttl t3 latest 5 ts1\n");
        } else if (parts[1] == "cancelop") {
            printf("desc: cancel the op\n");
            printf("usage: cancelop op_id\n");
            printf("ex: cancelop 5\n");
        } else if (parts[1] == "updatetablealive") {
            printf("desc: update table alive status\n");
            printf("usage: updatetablealive table_name pid endppoint is_alive\n");
            printf("ex: updatetablealive t1 * 172.27.2.52:9991 no\n");
            printf("ex: updatetablealive t1 0 172.27.2.52:9991 no\n");
        } else {
            printf("unsupport cmd %s\n", parts[1].c_str());
        }
    } else {
        printf("help format error!\n");
        printf("usage: help [cmd]\n");
        printf("usage: man [cmd]\n");
        printf("ex: help\n");
        printf("ex: help create\n");
        printf("ex:man\n");
        printf("ex:man create\n");
    }
}

void HandleNSClientSetTablePartition(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string name = parts[1];
    int fd = open(parts[2].c_str(), O_RDONLY);
    if (fd < 0) {
        std::cout << "can not open file " << parts[2] << std::endl;
        return;
    }
    ::rtidb::nameserver::TablePartition table_partition;
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    if (!google::protobuf::TextFormat::Parse(&fileInput, &table_partition)) {
        std::cout << "table partition file format error" << std::endl;
        return;
    }
    std::set<std::string> leader_set;
    std::set<std::string> follower_set;
    for (int idx = 0; idx < table_partition.partition_meta_size(); idx++) {
        std::string endpoint = table_partition.partition_meta(idx).endpoint();
        if (table_partition.partition_meta(idx).is_leader()) {
            if (leader_set.find(endpoint) != leader_set.end()) {
                std::cout << "has same leader " << endpoint<< std::endl;
                return;
            }
            leader_set.insert(endpoint);
        } else {
            if (follower_set.find(endpoint) != follower_set.end()) {
                std::cout << "has same follower" << endpoint<< std::endl;
                return;
            }
            follower_set.insert(endpoint);
        }
    }
    if (leader_set.empty()) {
        std::cout << "has no leader" << std::endl;
        return;
    }

    std::string msg;
    if (!client->SetTablePartition(name, table_partition, msg)) {
        std::cout << "Fail to set table partition. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "set table partition ok" << std::endl;
}

void HandleNSClientGetTablePartition(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string name = parts[1];
    uint32_t pid = 0;
    try {
        pid = boost::lexical_cast<uint32_t>(parts[2]);
    } catch (std::exception const& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
        return;
    } 
    ::rtidb::nameserver::TablePartition table_partition;
    std::string msg;
    if (!client->GetTablePartition(name, pid, table_partition, msg)) {
        std::cout << "Fail to get table partition. error msg: " << msg << std::endl;
        return;
    }
	std::string value;
	google::protobuf::TextFormat::PrintToString(table_partition, &value);
	std::string file_name = name + "_" + parts[2] + ".txt";
    FILE* fd_write = fopen(file_name.c_str(), "w");
    if (fd_write == NULL) {
        PDLOG(WARNING, "fail to open file %s", file_name.c_str());
		std::cout << "fail to open file" << file_name << std::endl;
        return;
    }
    bool io_error = false;
    if (fputs(value.c_str(), fd_write) == EOF) {
        std::cout << "write error" << std::endl;
        io_error = true;
    }
    if (!io_error && ((fflush(fd_write) == EOF) || fsync(fileno(fd_write)) == -1)) {
        std::cout << "flush error" << std::endl;
        io_error = true;
    }
    fclose(fd_write);
	if (!io_error) {
		std::cout << "get table partition ok" << std::endl;
	}
}

void HandleNSClientUpdateTableAlive(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    std::string name = parts[1];
    std::string endpoint = parts[3];
    bool is_alive = false;
    if (parts[4] == "yes") {
        is_alive = true;
    } else if (parts[4] == "no") {
        is_alive = false;
    } else {
        std::cout << "is_alive should be yes or no" << std::endl;
        return;
    }
    uint32_t pid = UINT32_MAX;
    if (parts[2] != "*") {
        try {
            int pid_tmp  = boost::lexical_cast<int32_t>(parts[2]);
            if (pid_tmp < 0) {
                std::cout << "Invalid args. pid should be uint32_t" << std::endl;
                return;
            }
            pid = pid_tmp;
        } catch (std::exception const& e) {
            std::cout << "Invalid args. pid should be uint32_t" << std::endl;
            return;
        } 
    }
    std::string msg;
    if (!client->UpdateTableAliveStatus(endpoint, name, pid, is_alive, msg)) {
        std::cout << "Fail to update table alive. error msg: " << msg << std::endl;
        return;
    }
    std::cout << "update ok" << std::endl;
}

void HandleNSShowOPStatus(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    std::vector<std::string> row;
    row.push_back("op_id");
    row.push_back("op_type");
    row.push_back("name");
    row.push_back("pid");
    row.push_back("status");
    row.push_back("start_time");
    row.push_back("execute_time");
    row.push_back("end_time");
    row.push_back("cur_task");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    ::rtidb::nameserver::ShowOPStatusResponse response;
    std::string msg;
    std::string name;
    uint32_t pid = ::rtidb::client::INVALID_PID;
    if (parts.size() > 1) {
        name = parts[1];
    }
    if (parts.size() > 2) {
        try {
            pid = boost::lexical_cast<uint32_t>(parts[2]);
        } catch(std::exception const& e) {
            std::cout << "Invalid args pid should be uint32_t" << std::endl;
            return;
        }
    }
    bool ok = client->ShowOPStatus(response, name, pid, msg);
    if (!ok) {
        std::cout << "Fail to show tablets. error msg: " << msg << std::endl;
        return;
    }
    for (int idx = 0; idx < response.op_status_size(); idx++) { 
        std::vector<std::string> row;
        row.push_back(std::to_string(response.op_status(idx).op_id()));
        row.push_back(response.op_status(idx).op_type());
        if (response.op_status(idx).has_name() && response.op_status(idx).has_pid()) {
            row.push_back(response.op_status(idx).name());
            row.push_back(std::to_string(response.op_status(idx).pid()));
        } else {
            row.push_back("-");
            row.push_back("-");
        }
        row.push_back(response.op_status(idx).status());
        if (response.op_status(idx).start_time() > 0) {
            time_t rawtime = (time_t)response.op_status(idx).start_time();
            tm* timeinfo = localtime(&rawtime);
            char buf[20];
            strftime(buf, 20, "%Y%m%d%H%M%S", timeinfo);
            row.push_back(buf);
            if (response.op_status(idx).end_time() != 0) {
                row.push_back(std::to_string(response.op_status(idx).end_time() - response.op_status(idx).start_time()) + "s");
                rawtime = (time_t)response.op_status(idx).end_time();
                timeinfo = localtime(&rawtime);
                buf[0] = '\0';
                strftime(buf, 20, "%Y%m%d%H%M%S", timeinfo);
                row.push_back(buf);
            } else {
                uint64_t cur_time = ::baidu::common::timer::now_time();
                row.push_back(std::to_string(cur_time - response.op_status(idx).start_time()) + "s");
                row.push_back("-");
            }
        } else {
            row.push_back("-");
            row.push_back("-");
            row.push_back("-");
        }
        row.push_back(response.op_status(idx).task_type());
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleClientSetTTL(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad setttl format, eg setttl tid pid type ttl [ts_name]" << std::endl;
        return;
    }
    std::string ts_name;
    if (parts.size() == 5) {
        ts_name = parts[5];
    }
    try {
        std::string value;
        uint64_t ttl = boost::lexical_cast<uint64_t>(parts[4]);
        ::rtidb::api::TTLType type = ::rtidb::api::kLatestTime;
        if (parts[3] == "absolute") {
            type = ::rtidb::api::kAbsoluteTime; 
        }
        bool ok = client->UpdateTTL(boost::lexical_cast<uint32_t>(parts[1]),
                                    boost::lexical_cast<uint32_t>(parts[2]),
                                    type, ttl, ts_name);
        if (ok) {
            std::cout << "Set ttl ok !" << std::endl;
        } else {
            std::cout << "Set ttl failed! " << std::endl; 
        }
    
    } catch(std::exception const& e) {
        std::cout << "Invalid args tid and pid should be uint32_t" << std::endl;
    }
}

void HandleClientGet(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad get format, eg get tid pid key time" << std::endl;
        return;
    }
    try {
        std::string value;
        uint64_t ts = 0;
        std::string msg;
        bool ok = client->Get(boost::lexical_cast<uint32_t>(parts[1]),
                              boost::lexical_cast<uint32_t>(parts[2]),
                              parts[3],
                              boost::lexical_cast<uint64_t>(parts[4]),
                              value,
                              ts,
                              msg);
        if (ok) {
            std::cout << "value :" << value << std::endl;
        }else {
            std::cout << "Get failed! error msg: " << msg << std::endl; 
        }

    
    } catch(std::exception const& e) {
        std::cout << "Invalid args tid and pid should be uint32_t" << std::endl;
    }

}

void HandleClientBenGet(std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    try {
        uint32_t tid = boost::lexical_cast<uint32_t>(parts[1]);
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        uint64_t key_num = 1000000;
        if (parts.size() >= 4) {
            key_num = ::boost::lexical_cast<uint64_t>(parts[3]);
        }
        uint32_t times = 10000;
        if (parts.size() >= 5) {
            times = ::boost::lexical_cast<uint32_t>(parts[4]);
        }
        int num = 100;
        if (parts.size() >= 6) {
            num = ::boost::lexical_cast<int>(parts[5]);
        }
        std::string value;
        uint64_t base = 100000000;
        std::random_device rd;
        std::default_random_engine engine(rd());
        std::uniform_int_distribution<> dis(1, key_num);
        std::string msg;
        while(num > 0) {
            for (uint32_t i = 0; i < times; i++) {
                std::string key = std::to_string(base + dis(engine));
                uint64_t ts = 0;
                client->Get(tid, pid, key, 0, value, ts, msg);
            }
            client->ShowTp();
            num--;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "put argument error!" << std::endl;
    }
}

// the input format like put 1 1 key time value
void HandleClientPut(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad put format, eg put tid pid key time value" << std::endl;
        return;
    }
    try {
        bool ok = client->Put(boost::lexical_cast<uint32_t>(parts[1]),
                            boost::lexical_cast<uint32_t>(parts[2]),
                            parts[3],
                            boost::lexical_cast<uint64_t>(parts[4]),
                            parts[5]);
        if (ok) {
            std::cout << "Put ok" << std::endl;
        }else {
            std::cout << "Put failed" << std::endl; 
        }
    } catch(std::exception const& e) {
        std::cout << "Invalid args tid and pid should be uint32_t" << std::endl;
    } 
}

void HandleClientBenPut(std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    try {
        uint32_t tid = boost::lexical_cast<uint32_t>(parts[1]);
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        uint64_t key_num = 1000000;
        if (parts.size() >= 4) {
            key_num = ::boost::lexical_cast<uint64_t>(parts[3]);
        }
        uint32_t times = 10000;
        if (parts.size() >= 5) {
            times = ::boost::lexical_cast<uint32_t>(parts[4]);
        }
        int num = 100;
        if (parts.size() >= 6) {
            num = ::boost::lexical_cast<int>(parts[5]);
        }
        std::string value(128, 'a');
        uint64_t base = 100000000;
        std::random_device rd;
        std::default_random_engine engine(rd());
        std::uniform_int_distribution<> dis(1, key_num);
        while(num > 0) {
            for (uint32_t i = 0; i < times; i++) {
                std::string key = std::to_string(base + dis(engine));
                uint64_t ts = ::baidu::common::timer::get_micros() / 1000;
                client->Put(tid, pid, key, ts, value);
            }
            client->ShowTp();
            num--;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "put argument error!" << std::endl;
    }
}

// the input format like create name tid pid ttl leader endpoints 
void HandleClientCreateTable(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad create format, input like create <name> <tid> <pid> <ttl> <seg_cnt>" << std::endl;
        return;
    }

    try {
        int64_t ttl = 0;
        ::rtidb::api::TTLType type = ::rtidb::api::TTLType::kAbsoluteTime;
        if (parts.size() > 4) {
            std::vector<std::string> vec;
            ::rtidb::base::SplitString(parts[4], ":", &vec);
            ttl = boost::lexical_cast<uint64_t>(vec[vec.size() - 1]);
            if (vec.size() > 1) {
                if (vec[0] == "latest") {
                    type = ::rtidb::api::TTLType::kLatestTime;
                    if (ttl > FLAGS_latest_ttl_max) {
                        std::cout << "Create failed. The max num of latest LatestTime is " 
                                  << FLAGS_latest_ttl_max << std::endl;
                        return;
                    }
                } else {
                    std::cout << "invalid ttl type" << std::endl;
                    return;
                }
            } else {
                if (ttl > FLAGS_absolute_ttl_max) {
                    std::cout << "Create failed. The max num of AbsoluteTime ttl is " 
                              << FLAGS_absolute_ttl_max << std::endl;
                    return;
                }
            }
        }
        if (ttl < 0) {
            std::cout << "ttl should be equal or greater than 0" << std::endl;
            return;
        }
        uint32_t seg_cnt = 16;
        if (parts.size() > 5) {
            seg_cnt = boost::lexical_cast<uint32_t>(parts[5]);
        }
        bool is_leader = true;
        if (parts.size() > 6 && parts[6] == "false") {
            is_leader = false;
        }
        std::vector<std::string> endpoints;
        ::rtidb::api::CompressType compress_type = ::rtidb::api::CompressType::kNoCompress;
        if (parts.size() > 7) {
            std::string raw_compress_type = parts[7];
            std::transform(raw_compress_type.begin(), raw_compress_type.end(), raw_compress_type.begin(), ::tolower);
            if (raw_compress_type == "knocompress" || raw_compress_type == "nocompress") {
                compress_type = ::rtidb::api::CompressType::kNoCompress;
            } else if (raw_compress_type == "ksnappy" || raw_compress_type == "snappy") {
                compress_type = ::rtidb::api::CompressType::kSnappy;
            } else {
                printf("compress type %s is invalid\n", parts[7].c_str());
                return;
            }
        }
        bool ok = client->CreateTable(parts[1], 
                                      boost::lexical_cast<uint32_t>(parts[2]),
                                      boost::lexical_cast<uint32_t>(parts[3]), 
                                      (uint64_t)ttl, is_leader, endpoints, type, seg_cnt, 0, compress_type);
        if (!ok) {
            std::cout << "Fail to create table" << std::endl;
        }else {
            std::cout << "Create table ok" << std::endl;
        }

    } catch(std::exception const& e) {
        std::cout << "Invalid args, tid , pid or ttl should be uint32_t" << std::endl;
    }
}

void HandleClientDropTable(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad drop command, you should input like 'drop tid pid' "<< std::endl;
        return;
    }
    try {
        bool ok = client->DropTable(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]));
        if (ok) {
            std::cout << "Drop table ok" << std::endl;
        }else {
            std::cout << "Fail to drop table" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad drop format" << std::endl;
    }
}

void HandleClientAddReplica(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad addreplica format" << std::endl;
        return;
    }
    try {
        bool ok = client->AddReplica(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), parts[3]);
        if (ok) {
            std::cout << "AddReplica ok" << std::endl;
        }else {
            std::cout << "Fail to Add Replica" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad addreplica format" << std::endl;
    }
}

void HandleClientDelReplica(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad delreplica format" << std::endl;
        return;
    }
    try {
        bool ok = client->DelReplica(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), parts[3]);
        if (ok) {
            std::cout << "DelReplica ok" << std::endl;
        }else {
            std::cout << "Fail to Del Replica" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad delreplica format" << std::endl;
    }
}

void HandleClientSetExpire(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    try {
        bool ok = client->SetExpire(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]),
                            parts[3] == "true" ? true : false);
        if (ok) {
            std::cout << "setexpire ok" << std::endl;
        } else {
            std::cout << "Fail to setexpire" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad format" << std::endl;
    }

}

void HandleClientConnectZK(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    bool ok = client->ConnectZK();
    if (ok) {
        std::cout << "connect zk ok" << std::endl;
    } else {
        std::cout << "Fail to connect zk" << std::endl;
    }
}

void HandleClientDisConnectZK(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    bool ok = client->DisConnectZK();
    if (ok) {
        std::cout << "disconnect zk ok" << std::endl;
    } else {
        std::cout << "Fail to disconnect zk" << std::endl;
    }
}

void HandleClientHelp(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 2) {
        printf("addreplica - add replica to leader\n");
        printf("changerole - change role\n");
        printf("count - count the num of data in specified key\n");
        printf("create - create table\n");
        printf("delreplica - delete replica from leader\n");
        printf("delete - delete pk\n");
        printf("drop - drop table\n");
        printf("exit - exit client\n");
        printf("get - get only one record\n");
        printf("gettablestatus - get table status\n");
        printf("getfollower - get follower\n");
        printf("help - get cmd info\n");
        printf("loadtable - create table and load data\n");
        printf("man - get cmd info\n");
        printf("makesnapshot - make snapshot\n");
        printf("pausesnapshot - pause snapshot\n");
        printf("preview - preview data\n");
        printf("put - insert data into table\n");
        printf("quit - exit client\n");
        printf("recoversnapshot - recover snapshot\n");
        printf("sput - insert data into table of multi dimension\n");
        printf("screate - create multi dimension table\n");
        printf("scan - get records for a period of time\n");
        printf("sscan - get records for a period of time from multi dimension table\n");
        printf("sget - get only one record from multi dimension table\n");
        printf("sendsnapshot - send snapshot to another endpoint\n");
        printf("setexpire - enable or disable ttl\n");
        printf("showschema - show schema\n");
        printf("setttl - set ttl for partition\n");
        printf("setlimit - set tablet max concurrency limit\n");
    } else if (parts.size() == 2) {
        if (parts[1] == "create") {
            printf("desc: create table\n");
            printf("usage: create name tid pid ttl segment_cnt [is_leader compress_type]\n");
            printf("ex: create table1 1 0 144000 8\n");
            printf("ex: create table1 1 0 144000 8 true snappy\n");
            printf("ex: create table1 1 0 144000 8 false\n");
        } else if (parts[1] == "screate") {
            printf("desc: create multi dimension table\n");
            printf("usage: screate table_name tid pid ttl segment_cnt is_leader schema\n");
            printf("ex: screate table1 1 0 144000 8 true card:string:index merchant:string:index amt:double\n");
        } else if (parts[1] == "drop") {
            printf("desc: drop table\n");
            printf("usage: drop tid pid\n");
            printf("ex: drop 1 0\n");
        } else if (parts[1] == "put") {
            printf("desc: insert data into table\n");
            printf("usage: put tid pid pk ts value\n");
            printf("ex: put 1 0 key1 1528858466000 value1\n");
        } else if (parts[1] == "sput") {
            printf("desc: insert data into table of multi dimension\n");
            printf("usage: sput tid pid ts key1 key2 ... value\n");
            printf("ex: sput 1 0 1528858466000 card0 merchant0 1.1\n");
        } else if (parts[1] == "scan") {
            printf("desc: get records for a period of time\n");
            printf("usage: scan tid pid pk starttime endtime [limit]\n");
            printf("ex: scan 1 0 key1 1528858466000 1528858300000\n");
            printf("ex: scan 1 0 key1 1528858466000 1528858300000 10\n");
            printf("ex: scan 1 0 key1 0 0 10\n");
        } else if (parts[1] == "sscan") {
            printf("desc: get records for a period of time from multi dimension table\n");
            printf("usage: sscan tid pid key key_name starttime endtime [limit]\n");
            printf("ex: sscan 1 0 card0 card 1528858466000 1528858300000\n");
            printf("ex: sscan 1 0 card0 card 1528858466000 1528858300000 10\n");
            printf("ex: sscan 1 0 card0 card 0 0 10\n");
        } else if (parts[1] == "get") {
            printf("desc: get only one record\n");
            printf("usage: get tid pid key ts\n");
            printf("ex: get 1 0 key1 1528858466000\n");
            printf("ex: get 1 0 key1 0\n");
        } else if (parts[1] == "sget") {
            printf("desc: get only one record from multi dimension table\n");
            printf("usage: sget tid pid key key_name ts\n");
            printf("ex: sget 1 0 card0 card 1528858466000\n");
            printf("ex: sget 1 0 card0 card 0\n");
        } else if (parts[1] == "delete") {
            printf("desc: delete pk\n");
            printf("usage: delete tid pid key [key_name]\n");
            printf("ex: delete 1 0 key1\n");
            printf("ex: delete 1 0 card0 card\n");
        } else if (parts[1] == "count") {
            printf("desc: count the num of data in specified key\n");
            printf("usage: count tid pid key [filter_expired_data]\n");
            printf("usage: count tid pid key key_name [filter_expired_data]\n");
            printf("ex: count 1 0 key1\n");
            printf("ex: count 1 0 key1 true\n");
            printf("ex: count 2 0 card0 card\n");
            printf("ex: count 2 0 card0 card true\n");
        } else if (parts[1] == "preview") {
            printf("desc: preview data in table\n");
            printf("usage: preview tid pid [limit]\n");
            printf("ex: preview 1 0\n");
            printf("ex: preview 1 0 10\n");
        } else if (parts[1] == "addreplica") {
            printf("desc: add replica to leader\n");
            printf("usage: addreplica tid pid endpoint\n");
            printf("ex: addreplica 1 0 172.27.2.52:9992\n");
        } else if (parts[1] == "delreplica") {
            printf("desc: delete replica from leader\n");
            printf("usage: delreplica tid pid endpoint\n");
            printf("ex: delreplica 1 0 172.27.2.52:9992\n");
        } else if (parts[1] == "makesnapshot") {
            printf("desc: make snapshot\n");
            printf("usage: makesnapshot tid pid\n");
            printf("ex: makesnapshot 1 0\n");
        } else if (parts[1] == "pausesnapshot") {
            printf("desc: pause snapshot\n");
            printf("usage: pausesnapshot tid pid\n");
            printf("ex: pausesnapshot 1 0\n");
        } else if (parts[1] == "recoversnapshot") {
            printf("desc: recover snapshot\n");
            printf("usage: recoversnapshot tid pid\n");
            printf("ex: recoversnapshot 1 0\n");
        } else if (parts[1] == "sendsnapshot") {
            printf("desc: send snapshot\n");
            printf("usage: sendsnapshot tid pid endpoint\n");
            printf("ex: sendsnapshot 1 0 172.27.128.32:8541\n");
        } else if (parts[1] == "loadtable") {
            printf("desc: create table and load data\n");
            printf("usage: loadtable table_name tid pid ttl segment_cnt\n");
            printf("ex: loadtable table1 1 0 144000 8\n");
        } else if (parts[1] == "changerole") {
            printf("desc: change role\n");
            printf("usage: changerole tid pid role\n");
            printf("ex: changerole 1 0 leader\n");
            printf("ex: changerole 1 0 follower\n");
        } else if (parts[1] == "setexpire") {
            printf("desc: enable or disable ttl\n");
            printf("usage: setexpire tid pid is_expire\n");
            printf("ex: setexpire 1 0 true\n");
            printf("ex: setexpire 1 0 false\n");
        } else if (parts[1] == "showschema") {
            printf("desc: show schema\n");
            printf("usage: showschema tid pid\n");
            printf("ex: showschema 1 0\n");
        } else if (parts[1] == "gettablestatus") {
            printf("desc: get table status\n");
            printf("usage: gettablestatus [tid pid]\n");
            printf("ex: gettablestatus\n");
            printf("ex: gettablestatus 1 0\n");
        } else if (parts[1] == "getfollower") {
            printf("desc: get table follower\n");
            printf("usage: getfollower tid pid\n");
            printf("ex: getfollower 1 0\n");
        } else if (parts[1] == "exit" || parts[1] == "quit") {
            printf("desc: exit client\n");
            printf("ex: quit\n");
            printf("ex: exit\n");
        } else if (parts[1] == "help" || parts[1] == "man") {
            printf("desc: get cmd info\n");
            printf("usage: help [cmd]\n");
            printf("usage: man [cmd]\n");
            printf("ex:help\n");
            printf("ex:help create\n");
            printf("ex:man\n");
            printf("ex:man create\n");
        } else if (parts[1] == "setttl") {
            printf("desc: set table ttl \n");
            printf("usage: setttl tid pid ttl_type ttl [ts_name]\n");
            printf("ex: setttl 1 0 absolute 10\n");
            printf("ex: setttl 2 0 latest 10\n");
            printf("ex: setttl 3 0 latest 10 ts1\n");
        } else if (parts[1] == "setlimit") {
            printf("desc: setlimit for tablet interface\n");
            printf("usage: setlimit method limit\n");
            printf("ex:setlimit Server 10, limit the server max concurrency to 10\n");
            printf("ex:setlimit Put 10, limit the server put  max concurrency to 10\n");
            printf("ex:setlimit Get 10, limit the server get  max concurrency to 10\n");
            printf("ex:setlimit Scan 10, limit the server scan  max concurrency to 10\n");
        }else {
            printf("unsupport cmd %s\n", parts[1].c_str());
        }
    } else {
        printf("help format error!\n");
        printf("usage: help [cmd]\n");
        printf("ex: help\n");
        printf("ex: help create\n");
    }
}

void HandleClientSetTTLClock(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    struct tm tm;
    time_t timestamp;
    if (parts[3].length() == 14 && ::rtidb::base::IsNumber(parts[3]) &&
            strptime(parts[3].c_str(), "%Y%m%d%H%M%S", &tm) != NULL) {
        timestamp = mktime(&tm);
    } else {
        printf("time format error (e.g 20171108204001)");
        return;
    }
    try {
        bool ok = client->SetTTLClock(boost::lexical_cast<uint32_t>(parts[1]), 
                                    boost::lexical_cast<uint32_t>(parts[2]), 
                                    timestamp);
        if (ok) {
            std::cout << "setttlclock ok" << std::endl;
        } else {
            std::cout << "Fail to setttlclock" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad format" << std::endl;
    }

}

void AddPrintRow(const ::rtidb::api::TableStatus& table_status, ::baidu::common::TPrinter& tp) {
    std::vector<std::string> row;
    row.push_back(std::to_string(table_status.tid()));
    row.push_back(std::to_string(table_status.pid()));
    row.push_back(std::to_string(table_status.offset()));
    row.push_back(::rtidb::api::TableMode_Name(table_status.mode()));
    row.push_back(::rtidb::api::TableState_Name(table_status.state()));
    if (table_status.is_expire()) {
        row.push_back("true");
    } else {
        row.push_back("false");
    }
    if (table_status.ttl_type() == ::rtidb::api::TTLType::kLatestTime) {
        row.push_back(std::to_string(table_status.ttl()));
    } else {
        row.push_back(std::to_string(table_status.ttl()) + "min");
    }
    row.push_back(std::to_string(table_status.time_offset()) + "s");
    row.push_back(::rtidb::base::HumanReadableString(table_status.record_byte_size() + table_status.record_idx_byte_size()));
    row.push_back(::rtidb::api::CompressType_Name(table_status.compress_type()));
    row.push_back(std::to_string(table_status.skiplist_height()));
    tp.AddRow(row);
}

void HandleClientGetTableStatus(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    std::vector<std::string> row;
    row.push_back("tid");
    row.push_back("pid");
    row.push_back("offset");
    row.push_back("mode");
    row.push_back("state");
    row.push_back("enable_expire");
    row.push_back("ttl");
    row.push_back("ttl_offset");
    row.push_back("memused");
    row.push_back("compress_type");
    row.push_back("skiplist_height");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    if (parts.size() == 3) {
        ::rtidb::api::TableStatus table_status;
        try {
            if (client->GetTableStatus(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), table_status)) {
                AddPrintRow(table_status, tp);
                tp.Print(true);
            } else {
                std::cout << "gettablestatus failed" << std::endl;
            }
        } catch (boost::bad_lexical_cast& e) {
            std::cout << "Bad gettablestatus format" << std::endl;

        }
    } else if (parts.size() == 1) {
        ::rtidb::api::GetTableStatusResponse response;
        if (!client->GetTableStatus(response)) {
            std::cout << "gettablestatus failed" << std::endl;
            return;
        }
        for (int idx = 0; idx < response.all_table_status_size(); idx++) {
            AddPrintRow(response.all_table_status(idx), tp);
        }
        tp.Print(true);
    } else {
        std::cout << "Bad gettablestatus format" << std::endl;
        return;
    }
}

void HandleClientMakeSnapshot(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad MakeSnapshot format" << std::endl;
        return;
    }
    bool ok = client->MakeSnapshot(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]));
    if (ok) {
        std::cout << "MakeSnapshot ok" << std::endl;
    } else {
        std::cout << "Fail to MakeSnapshot" << std::endl;
    }
}

void HandleClientPauseSnapshot(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad PauseSnapshot format" << std::endl;
        return;
    }
    try {
        bool ok = client->PauseSnapshot(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]));
        if (ok) {
            std::cout << "PauseSnapshot ok" << std::endl;
        }else {
            std::cout << "Fail to PauseSnapshot" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad PauseSnapshot format" << std::endl;
    }
}

void HandleClientRecoverSnapshot(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad RecoverSnapshot format" << std::endl;
        return;
    }
    try {
        bool ok = client->RecoverSnapshot(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]));
        if (ok) {
            std::cout << "RecoverSnapshot ok" << std::endl;
        }else {
            std::cout << "Fail to RecoverSnapshot" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad RecoverSnapshot format" << std::endl;
    }
}

void HandleClientSendSnapshot(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad SendSnapshot format" << std::endl;
        return;
    }
    try {
        bool ok = client->SendSnapshot(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), parts[3]);
        if (ok) {
            std::cout << "SendSnapshot ok" << std::endl;
        }else {
            std::cout << "Fail to SendSnapshot" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad SendSnapshot format" << std::endl;
    }
}

void HandleClientLoadTable(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad LoadTable format eg loadtable <name> <tid> <pid> <ttl> <seg_cnt> " << std::endl;
        return;
    }
    try {
        uint64_t ttl = 0;
        if (parts.size() > 4) {
            ttl = boost::lexical_cast<uint64_t>(parts[4]);
        }
        uint32_t seg_cnt = 16;
        if (parts.size() > 5) {
            seg_cnt = boost::lexical_cast<uint32_t>(parts[5]);
        }
        bool is_leader = true;
        if (parts.size() > 6 && parts[6] == "false") {
            is_leader = false;
        }
        std::vector<std::string> endpoints;
        for (size_t i = 7; i < parts.size(); i++) {
            endpoints.push_back(parts[i]);
        }

        bool ok = client->LoadTable(parts[1], boost::lexical_cast<uint32_t>(parts[2]),
                                    boost::lexical_cast<uint32_t>(parts[3]), 
                                    ttl,
                                    is_leader, endpoints, seg_cnt);
        if (ok) {
            std::cout << "LoadTable ok" << std::endl;
        }else {
            std::cout << "Fail to LoadTable" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad LoadTable format" << std::endl;
    }
}

void HandleClientSetLimit(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad set limit format" << std::endl;
        return;
    }
    try {
        std::string key = parts[1];
        if (std::isupper(key[0])) {
            std::string subname = key.substr(1);
            for (char e : subname) {
                if (std::isupper(e)) {
                    std::cout<<"Invalid args name which should be Put , Scan , Get or Server"<<std::endl;
                    return;
                }
            }
        } else {
            std::cout<<"Invalid args name which should be Put , Scan , Get or Server"<<std::endl;
            return;
        }
        int32_t limit = boost::lexical_cast<int32_t> (parts[2]);
        bool ok = client->SetMaxConcurrency(key, limit);
        if (ok) {
            std::cout << "Set Limit ok" << std::endl;
        }else {
            std::cout << "Fail to set limit" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad set limit format" << std::endl;
    }
}

void HandleClientChangeRole(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad changerole format" << std::endl;
        return;
    }
    try {
        uint64_t termid = 0;
        if (parts.size() > 4) {
            termid = boost::lexical_cast<uint64_t>(parts[4]);
        }
        if (parts[3].compare("leader") == 0) {
            bool ok = client->ChangeRole(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), true, termid);
            if (ok) {
                std::cout << "ChangeRole ok" << std::endl;
            } else {
                std::cout << "Fail to change leader" << std::endl;
            }
        } else if (parts[3].compare("follower") == 0) {
            bool ok = client->ChangeRole(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), false, termid);
            if (ok) {
                std::cout << "ChangeRole ok" << std::endl;
            } else {
                std::cout << "Fail to change follower" << std::endl;
            }
        } else {
            std::cout << "role must be leader or follower" << std::endl;
        }
    } catch (boost::bad_lexical_cast& e) {
        std::cout << "Bad changerole format" << std::endl;
    }
}

void HandleClientPreview(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout << "preview format error. eg: preview tid pid [limit]" << std::endl;
        return;
    }
    uint32_t limit = FLAGS_preview_default_limit;
    uint32_t tid, pid;
    try {
        tid =  boost::lexical_cast<uint32_t>(parts[1]);
        pid =  boost::lexical_cast<uint32_t>(parts[2]);
        if (parts.size() > 3) {
            int64_t tmp = boost::lexical_cast<int64_t>(parts[3]);
            if (tmp < 0) {
                printf("preview error. limit should be unsigned int\n");
                return;
            }
            limit = boost::lexical_cast<uint32_t>(parts[3]);
            if (limit > FLAGS_preview_limit_max_num) {
                printf("preview error. limit is greater than the max num %u\n", FLAGS_preview_limit_max_num);
                return;
            } else if (limit == 0) {
                printf("preview error. limit must be greater than zero\n");
                return;
            }
        }
    } catch (std::exception const& e) {
        printf("Invalid args. tid, pid and limit should be unsigned int\n");
        return;
    }
    ::rtidb::api::TableStatus table_status;
    if (!client->GetTableStatus(tid, pid, true, table_status)) {
        std::cout << "Fail to get table status" << std::endl;
        return;
    }
    std::string schema = table_status.schema();
    std::vector<::rtidb::base::ColumnDesc> columns;
    if (!schema.empty()) {
        ::rtidb::base::SchemaCodec codec;
        codec.Decode(schema, columns);
    }
    uint32_t column_num = columns.empty() ? 4 : columns.size() + 2;
    ::baidu::common::TPrinter tp(column_num, FLAGS_max_col_display_length);
    std::vector<std::string> row;
    if (schema.empty()) {
        row.push_back("#");
        row.push_back("key");
        row.push_back("ts");
        row.push_back("data");
    } else {
        row.push_back("#");
        row.push_back("ts");
        for (uint32_t i = 0; i < columns.size(); i++) {
            row.push_back(columns[i].name);
        }
    }
    tp.AddRow(row);
    uint32_t index = 1;
    uint32_t count = 0;
    ::rtidb::base::KvIterator* it = client->Traverse(tid, pid, "", "", 0, limit, count);
    if (it == NULL) {
        std::cout << "Fail to preview table" << std::endl;
        return;
    }
    while (it->Valid()) {
        row.clear();
        row.push_back(std::to_string(index));
        if (schema.empty()) {
            std::string value = it->GetValue().ToString();
            if (table_status.compress_type() == ::rtidb::api::CompressType::kSnappy) {
                std::string uncompressed;
                ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
                value = uncompressed;
            }
            row.push_back(it->GetPK());
            row.push_back(std::to_string(it->GetKey()));
            row.push_back(value);
        } else {
            row.push_back(std::to_string(it->GetKey()));
            if (table_status.compress_type() == ::rtidb::api::CompressType::kSnappy) {
                std::string uncompressed;
                ::snappy::Uncompress(it->GetValue().data(), it->GetValue().size(), &uncompressed);
                ::rtidb::base::FillTableRow(columns, uncompressed.c_str(), uncompressed.length(), row); 
            } else {
                ::rtidb::base::FillTableRow(columns, it->GetValue().data(), it->GetValue().size(), row); 
            }
        }
        tp.AddRow(row);
        index++;
        it->Next();
    }
    delete it;
    tp.Print(true);
}

// the input format like scan tid pid pk st et
void HandleClientScan(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad scan format! eg. scan tid pid pk start_time end_time [limit]" << std::endl;
        return;
    }
    try {
        uint32_t limit = 0;
        if (parts.size() > 6) {
            limit = boost::lexical_cast<uint32_t>(parts[6]);
        }
        std::string msg;
        ::rtidb::base::KvIterator* it = client->Scan(boost::lexical_cast<uint32_t>(parts[1]), 
                boost::lexical_cast<uint32_t>(parts[2]),
                parts[3], boost::lexical_cast<uint64_t>(parts[4]), 
                boost::lexical_cast<uint64_t>(parts[5]),
                limit, msg);
        if (it == NULL) {
            std::cout << "Fail to scan table. error msg: " << msg << std::endl;
        }else {
            bool print = true;
            if (parts.size() >= 7) {
                if (parts[6] == "false") {
                    print = false;
                }
            }
            std::cout << "#\tTime\tData" << std::endl;
            uint32_t index = 1;
            while (it->Valid()) {
                if (print) {
                    std::cout << index << "\t" << it->GetKey() << "\t" << it->GetValue().ToString() << std::endl;
                } 
                index ++;
                it->Next();
            }
            delete it;
        }
    } catch (std::exception const& e) {
        std::cout<< "Invalid args, tid pid should be uint32_t, st and et should be uint64_t" << std::endl;
    }
}

void HandleClientBenchmarkPut(uint32_t tid, uint32_t pid,
                              uint32_t val_size, uint32_t run_times,
                              uint32_t ns,
        ::rtidb::client::TabletClient* client) {
    char val[val_size];
    for (uint32_t i = 0; i < val_size; i++) {
        val[i] ='0';
    }
    std::string sval(val);
    for (uint32_t i = 0 ; i < run_times; i++) {
        std::string key = boost::lexical_cast<std::string>(ns) + "test" + boost::lexical_cast<std::string>(i);
        for (uint32_t j = 0; j < 4000; j++) {
            client->Put(tid, pid, key, j, sval);
        }
        client->ShowTp();
    }
}

void HandleClientBenchmarkScan(uint32_t tid, uint32_t pid,
        uint32_t run_times, 
        uint32_t ns,
        ::rtidb::client::TabletClient* client) {
    uint64_t st = 999;
    uint64_t et = 0;
    std::string msg;
    for (uint32_t j = 0; j < run_times; j++) {
        for (uint32_t i = 0; i < 500 * 4; i++) {
            std::string key =boost::lexical_cast<std::string>(ns) + "test" + boost::lexical_cast<std::string>(i);
            ::rtidb::base::KvIterator* it = client->Scan(tid, pid, key, st, et, 0, msg);
            delete it;
        }
        client->ShowTp();
    }
}


void HandleClientBenchmark(::rtidb::client::TabletClient* client) {
    uint32_t size = 40;
    uint32_t times = 10;
    std::cout << "Percentile:Start benchmark put size:40" << std::endl;
    HandleClientBenchmarkPut(1, 1, size, times, 1, client);
    std::cout << "Percentile:Start benchmark put size:80" << std::endl;
    HandleClientBenchmarkPut(1, 1, 80, times, 2, client);
    std::cout << "Percentile:Start benchmark put size:200" << std::endl;
    HandleClientBenchmarkPut(1, 1, 200, times, 3, client);
    std::cout << "Percentile:Start benchmark put ha size:400" << std::endl;
    HandleClientBenchmarkPut(1, 1, 400, times, 4, client);

    std::cout << "Percentile:Start benchmark put with one replica size:40" << std::endl;
    HandleClientBenchmarkPut(2, 1, size, times, 1, client);
    std::cout << "Percentile:Start benchmark put with one replica size:80" << std::endl;
    HandleClientBenchmarkPut(2, 1, 80, times, 2, client);
    std::cout << "Percentile:Start benchmark put with one replica  size:200" << std::endl;
    HandleClientBenchmarkPut(2, 1, 200, times, 3, client);
    std::cout << "Percentile:Start benchmark put with one replica size:400" << std::endl;
    HandleClientBenchmarkPut(2, 1, 400, times, 4, client);

    std::cout << "Percentile:Start benchmark Scan 1000 records key size:40" << std::endl;
    HandleClientBenchmarkScan(1, 1, times, 1, client);
    std::cout << "Percentile:Start benchmark Scan 1000 records key size:80" << std::endl;
    HandleClientBenchmarkScan(1, 1, times, 2, client);
    std::cout << "Percentile:Start benchmark Scan 1000 records key size:200" << std::endl;
    HandleClientBenchmarkScan(1, 1, times, 3, client);
    std::cout << "Percentile:Start benchmark Scan 1000 records key size:400" << std::endl;
    HandleClientBenchmarkScan(1, 1, times, 4, client);
}

void HandleClientSCreateTable(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 8) {
        std::cout << "Bad create format, input like screate <name> <tid> <pid> <ttl> <seg_cnt> <is_leader> <schema>" << std::endl;
        return;
    }
    std::set<std::string> type_set;
    type_set.insert("int32");
    type_set.insert("uint32");
    type_set.insert("int64");
    type_set.insert("uint64");
    type_set.insert("float");
    type_set.insert("double");
    type_set.insert("string");
    type_set.insert("bool");
    type_set.insert("timestamp");
    type_set.insert("date");
    type_set.insert("int16");
    type_set.insert("uint16");
    try {
        int64_t ttl = 0;
        ::rtidb::api::TTLType type = ::rtidb::api::TTLType::kAbsoluteTime;
        std::vector<std::string> vec;
        ::rtidb::base::SplitString(parts[4], ":", &vec);
        ttl = boost::lexical_cast<int64_t>(vec[vec.size() - 1]);
        if (vec.size() > 1) {
            if (vec[0] == "latest") {
                type = ::rtidb::api::TTLType::kLatestTime;
                if (ttl > FLAGS_latest_ttl_max) {
                    std::cout << "Create failed. The max num of latest LatestTime is " 
                              << FLAGS_latest_ttl_max << std::endl;
                    return;
                }
            } else {
                std::cout << "invalid ttl type " << std::endl;
                return;
            }
        } else {
            if (ttl > FLAGS_absolute_ttl_max) {
                std::cout << "Create failed. The max num of AbsoluteTime ttl is " 
                          << FLAGS_absolute_ttl_max << std::endl;
                return;
            }
        }        
        if (ttl < 0) {
            std::cout << "invalid ttl which should be equal or greater than 0" << std::endl;
            return;
        }
        uint32_t seg_cnt = boost::lexical_cast<uint32_t>(parts[5]);
        bool leader = true;
        if (parts[6].compare("false") != 0 && parts[6].compare("true") != 0) {
            std::cout << "create failed! is_leader parameter should be true or false" << std::endl;
            return;
        }
        if (parts[6].compare("false") == 0) {
            leader = false;
        }
        std::vector<::rtidb::base::ColumnDesc> columns;
        // check duplicate column
        std::set<std::string> used_column_names;
        bool has_index = false;
        for (uint32_t i = 7; i < parts.size(); i++) {
            std::vector<std::string> kv;
            ::rtidb::base::SplitString(parts[i], ":", &kv);
            if (kv.size() < 2) {
                std::cout << "create failed! schema format is illegal" << std::endl;
                return;
            }
            if (used_column_names.find(kv[0]) != used_column_names.end()) {
                std::cout << "Duplicated column " << kv[0] << std::endl;
                return;
            }
            std::string cur_type = kv[1];
            std::transform(cur_type.begin(), cur_type.end(), cur_type.begin(), ::tolower);
            if (type_set.find(cur_type) == type_set.end()) {
                printf("type %s is invalid\n", kv[1].c_str());
                return;
            }
            used_column_names.insert(kv[0]);
            ::rtidb::base::ColumnDesc desc;
            desc.add_ts_idx = false;
            if (kv.size() > 2 && kv[2] == "index") {
                if ((cur_type == "float") || (cur_type == "double")) {
                    printf("float or double column can not be index\n");
                    return;
                }
                desc.add_ts_idx = true;
                has_index = true;
            }
            desc.type = rtidb::base::SchemaCodec::ConvertType(cur_type);
            desc.name = kv[0];
            columns.push_back(desc);
        }
        if (!has_index) {
            std::cout << "create failed! schema has no index" << std::endl;
            return;
        }
        bool ok = client->CreateTable(parts[1], 
                                      boost::lexical_cast<uint32_t>(parts[2]),
                                      boost::lexical_cast<uint32_t>(parts[3]), 
                                      (uint64_t)ttl, seg_cnt, columns, type, leader,
                                      std::vector<std::string>());
        if (!ok) {
            std::cout << "Fail to create table" << std::endl;
        }else {
            std::cout << "Create table ok" << std::endl;
        }

    } catch(std::exception const& e) {
        std::cout << "Invalid args " << e.what() << std::endl;
    }
}

void HandleClientGetFollower(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout <<  "Bad get follower format" << std::endl;
        return;
    }
    uint32_t tid = 0;
    uint32_t pid = 0;
    try {
        tid = boost::lexical_cast<uint32_t>(parts[1]);
        pid = boost::lexical_cast<uint32_t>(parts[2]);
    } catch (std::exception const& e) {
        std::cout << "Invalid args" << std::endl;
        return;
    }
    std::map<std::string, uint64_t> info_map;
    uint64_t offset = 0;
    std::string msg;
    if (!client->GetTableFollower(tid, pid, offset, info_map, msg)) {
        std::cout << "get failed. msg: " << msg << std::endl;
        return;
    }
    std::vector<std::string> header;
    header.push_back("#");
    header.push_back("tid");
    header.push_back("pid");
    header.push_back("leader_offset");
    header.push_back("follower");
    header.push_back("offset");
    ::baidu::common::TPrinter tp(header.size());

    tp.AddRow(header);
    int idx = 0;
    for (const auto& kv : info_map) {
        std::vector<std::string> row;
        row.push_back(std::to_string(idx));
        idx++;
        row.push_back(std::to_string(tid));
        row.push_back(std::to_string(pid));
        row.push_back(std::to_string(offset));
        row.push_back(kv.first);
        row.push_back(std::to_string(kv.second));
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleClientCount(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "count format error! eg. count tid pid key [col_name] [filter_expired_data] | count tid=xxx pid=xxx key=xxx index_name=xxx ts=xxx ts_name=xxx [filter_expired_data]" << std::endl;
        return;
    }
	std::vector<std::string> temp_vec;
    ::rtidb::base::SplitString(parts[1],"=", &temp_vec);
    std::map<std::string, std::string> parameter_map;
    bool has_ts_col = false;
    if (temp_vec.size() == 2 && temp_vec[0] == "tid" && !temp_vec[1].empty()) {
        has_ts_col = true;
        parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        for (uint32_t i = 2; i < parts.size(); i++) {
            ::rtidb::base::SplitString(parts[i],"=", &temp_vec);
            if (temp_vec.size() < 2 || temp_vec[1].empty()) {
                std::cout << "count format erro! eg. count tid=xxx pid=xxx key=xxx index_name=xxx ts=xxx ts_name=xxx [filter_expired_data]" << std::endl;
                return;
            }
            parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        }
    }
    uint32_t tid = 0;
    uint32_t pid = 0;
	bool filter_expired_data = false;
	std::string key;
    std::string index_name;
	std::string ts_name;
    uint64_t value = 0;
	auto iter = parameter_map.begin();
    try {
		if (has_ts_col) {
        	iter = parameter_map.find("tid");
        	if (iter != parameter_map.end()) {
        		tid = boost::lexical_cast<uint32_t>(iter->second);
        	} else {
            	std::cout<<"count format error: tid does not exist!"<<std::endl;
            	return;
        	}
			iter = parameter_map.find("pid");
        	if (iter != parameter_map.end()) {
        		pid = boost::lexical_cast<uint32_t>(iter->second);
        	} else {
            	std::cout<<"count format error: pid does not exist!"<<std::endl;
            	return;
        	}
        	iter = parameter_map.find("key");
        	if (iter != parameter_map.end()) {
            	key = iter->second;
        	} else {
            	std::cout<<"count format error: key does not exist!"<<std::endl;
            	return;
        	}	   
        	iter = parameter_map.find("index_name");
        		if (iter != parameter_map.end()) {
            	index_name = iter->second;
        	} else {
            	std::cout<<"count format error: index_name does not exist!"<<std::endl;
            	return;
        	}
        	iter = parameter_map.find("ts_name");
        	if (iter != parameter_map.end()) {
            	ts_name = iter->second;
        	} else {
            	std::cout<<"count format error: ts_name does not exist!"<<std::endl;
            	return;
        	}
        	iter = parameter_map.find("filter_expired_data");
        	if (iter != parameter_map.end()) {
				std::string temp_str = iter->second;
				if (temp_str == "true") {
            		filter_expired_data = true;
        		} else if (temp_str == "false") {
            		filter_expired_data = false;
        		} else {
            		printf("filter_expired_data parameter should be true or false\n");
            		return;
        		}
        	}
    	} else {
			tid = boost::lexical_cast<uint32_t>(parts[1]);
			pid = boost::lexical_cast<uint32_t>(parts[2]);
			if (parts.size() == 5) {
        		if (parts[4] == "true") {
            		filter_expired_data = true;
        		} else if (parts[4] == "false") {
            		filter_expired_data = false;
        		} else {
            		index_name = parts[4];
        		}
    		} else if (parts.size() > 5) {
        		index_name =  parts[4];
        		if (parts[5] == "true") {
            		filter_expired_data = true;
        		} else if (parts[5] == "false") {
            		filter_expired_data = false;
        		} else {
            		printf("filter_expired_data parameter should be true or false\n");
            		return;
        		}
    		}
		}
    } catch (std::exception const& e) {
        std::cout << "Invalid args. tid and pid should be uint32" << std::endl;
        return;
    }
    std::string msg;
	bool ok;
	if (has_ts_col) {
		ok = client->Count(tid, pid, key, index_name, ts_name, filter_expired_data, value, msg);
	} else {
		key = parts[3];
    	ok = client->Count(tid, pid, key, index_name, filter_expired_data, value, msg);
	}
    if (ok) {
        std::cout << "count: " << value << std::endl;
    } else {
        std::cout << "Count failed. error msg: " << msg << std::endl; 
    }
}    

void HandleClientShowSchema(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout <<  "Bad show schema format" << std::endl;
        return;
    }
    ::rtidb::api::TableMeta table_meta;
    std::string schema;
    try {
        bool ok = client->GetTableSchema(boost::lexical_cast<uint32_t>(parts[1]),
                                        boost::lexical_cast<uint32_t>(parts[2]), table_meta);
        schema = table_meta.schema();
        if(!ok) {
            std::cout << "ShowSchema failed" << std::endl;
            return;
        }
    } catch (std::exception const& e) {
        std::cout << "Invalid args" << std::endl;
        return;
    }
    if (table_meta.column_desc_size() > 0) {
        ::rtidb::base::PrintSchema(table_meta.column_desc());
        printf("\n#ColumnKey\n");
        std::string ttl_suff = table_meta.ttl_type() == ::rtidb::api::kLatestTime ? "" : "min";
        ::rtidb::base::PrintColumnKey(table_meta.ttl(), ttl_suff, table_meta.column_desc(), table_meta.column_key());
    } else if (!schema.empty()){
        ::rtidb::base::PrintSchema(schema);
    } else {
        std::cout << "No schema for table" << std::endl;
    }
}

uint32_t GetDimensionIndex(const std::vector<::rtidb::base::ColumnDesc>& columns,
                           const std::string& dname) {
    uint32_t dindex = 0;
    for (uint32_t i = 0; i < columns.size(); i++) {
        if (columns[i].name == dname) {
            return dindex;
        }
        if (columns[i].add_ts_idx) {
            dindex ++;
        }
    }
    return 0;
}

void HandleClientSGet(const std::vector<std::string>& parts, 
                      ::rtidb::client::TabletClient* client){
	if (parts.size() < 5) {
		std::cout << "Bad sget format, eg. sget tid pid key index_name ts | sget table_name=xxx key=xxx index_name=xxx ts=xxx ts_name=xxx" << std::endl;
		return;
	}
	std::vector<std::string> temp_vec;
    ::rtidb::base::SplitString(parts[1],"=", &temp_vec);
    std::map<std::string, std::string> parameter_map;
    bool has_ts_col = false;
    if (temp_vec.size() == 2 && temp_vec[0] == "tid" && !temp_vec[1].empty()) {
        has_ts_col = true;
        parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        for (uint32_t i = 2; i < parts.size(); i++) {
            ::rtidb::base::SplitString(parts[i],"=", &temp_vec);
            if (temp_vec.size() < 2 || temp_vec[1].empty()) {
                std::cout << "sget format erro! eg. sget table_name=xxx key=xxx index_name=xxx ts=xxx ts_name=xxx" << std::endl;
                return;
            }
            parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        }
    }
	uint32_t tid = 0;
    uint32_t pid = 0;
	std::string key;
    std::string index_name;
	uint64_t timestamp = 0;
	std::string ts_name;
	auto iter = parameter_map.begin();
    try {
		if (has_ts_col) {
        	iter = parameter_map.find("tid");
        	if (iter != parameter_map.end()) {
        		tid = boost::lexical_cast<uint32_t>(iter->second);
        	} else {
            	std::cout<<"sget format error: tid does not exist!"<<std::endl;
            	return;
        	}
			iter = parameter_map.find("pid");
        	if (iter != parameter_map.end()) {
        		pid = boost::lexical_cast<uint32_t>(iter->second);
        	} else {
            	std::cout<<"sget format error: pid does not exist!"<<std::endl;
            	return;
        	}
        	iter = parameter_map.find("key");
        	if (iter != parameter_map.end()) {
            	key = iter->second;
        	} else {
            	std::cout<<"sget format error: key does not exist!"<<std::endl;
            	return;
        	}	   
        	iter = parameter_map.find("index_name");
        		if (iter != parameter_map.end()) {
            	index_name = iter->second;
        	} else {
            	std::cout<<"sget format error: index_name does not exist!"<<std::endl;
            	return;
        	}
        	iter = parameter_map.find("ts");
        	if (iter != parameter_map.end()) {
            	timestamp = boost::lexical_cast<uint64_t>(iter->second);
        	} else {
            	std::cout<<"sget format error: ts does not exist!"<<std::endl;
            	return;
        	}
			iter = parameter_map.find("ts_name");
        	if (iter != parameter_map.end()) {
            	ts_name = iter->second;
        	} else {
            	std::cout<<"sget format error: ts_name does not exist!"<<std::endl;
            	return;
        	}
    	} else {
        	tid = boost::lexical_cast<uint32_t>(parts[1]);
        	pid = boost::lexical_cast<uint32_t>(parts[2]);
			key = parts[3];
			index_name = parts[4];
			if (parts.size() > 5) {
				timestamp = boost::lexical_cast<uint64_t>(parts[5]);
			}
		}
    } catch (std::exception const& e) {
        std::cout << "Invalid args. tid pid should be uint32_t, ts should be uint64_t, " << std::endl;
        return;
    }
	::rtidb::api::TableMeta table_meta;
	bool ok = client->GetTableSchema(tid, pid, table_meta);
	if(!ok) {
		std::cout << "No schema for table ,please use command get" << std::endl;
		return;
	}
	std::string schema = table_meta.schema();
	std::vector<::rtidb::base::ColumnDesc> raw;
	::rtidb::base::SchemaCodec codec;
	codec.Decode(schema, raw);
	::baidu::common::TPrinter tp(raw.size() + 2, FLAGS_max_col_display_length);
	std::vector<std::string> row;
	row.push_back("#");
	row.push_back("ts");
	for (uint32_t i = 0; i < raw.size(); i++) {
		row.push_back(raw[i].name);
	}
	tp.AddRow(row);

	std::string value;
	uint64_t ts = 0;
	std::string msg;
	if (has_ts_col) {
		ok = client->Get(tid, pid, key, timestamp, index_name, ts_name, value, ts, msg);
	} else {
		ok = client->Get(tid, pid, key, timestamp, index_name, value, ts, msg);
	}
	if (!ok) {
		std::cout << "Fail to sget value! error msg: " << msg << std::endl;
		return;
	}
	row.clear();
	row.push_back("1");
	row.push_back(std::to_string(ts));
	::rtidb::base::FillTableRow(raw, value.c_str(), value.size(), row);
	tp.AddRow(row);
	tp.Print(true);

}

void HandleClientSScan(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 7) {
        std::cout << "Bad scan format! eg.sscan tid pid key col_name start_time end_time [limit] | sscan table_name=xxx key=xxx index_name=xxx st=xxx et=xxx ts_name=xxx [limit=xxx]" << std::endl;
        return;
    }
	std::vector<std::string> temp_vec;
    ::rtidb::base::SplitString(parts[1],"=", &temp_vec);
    std::map<std::string, std::string> parameter_map;
    bool has_ts_col = false;
    if (temp_vec.size() == 2 && temp_vec[0] == "tid" && !temp_vec[1].empty()) {
        has_ts_col = true;
        parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        for (uint32_t i = 2; i < parts.size(); i++) {
            ::rtidb::base::SplitString(parts[i],"=", &temp_vec);
            if (temp_vec.size() < 2 || temp_vec[1].empty()) {
                std::cout << "scan format erro! eg. sscan table_name=xxx key=xxx index_name=xxx st=xxx et=xxx ts_name=xxx [limit=xxx]" << std::endl;
                return;
            }
            parameter_map.insert(std::make_pair(temp_vec[0], temp_vec[1]));
        }
    }
	uint32_t tid = 0;
    uint32_t pid = 0;
	std::string key;
    std::string index_name;
	uint64_t st = 0;
	uint64_t et = 0;
	std::string ts_name;
	uint32_t limit = 0;
	auto iter = parameter_map.begin();
    try {
		if (has_ts_col) {
        	iter = parameter_map.find("tid");
        	if (iter != parameter_map.end()) {
        		tid = boost::lexical_cast<uint32_t>(iter->second);
        	} else {
            	std::cout<<"sscan format error: tid does not exist!"<<std::endl;
            	return;
        	}
			iter = parameter_map.find("pid");
        	if (iter != parameter_map.end()) {
        		pid = boost::lexical_cast<uint32_t>(iter->second);
        	} else {
            	std::cout<<"sscan format error: pid does not exist!"<<std::endl;
            	return;
        	}
        	iter = parameter_map.find("key");
        	if (iter != parameter_map.end()) {
            	key = iter->second;
        	} else {
            	std::cout<<"sscan format error: key does not exist!"<<std::endl;
            	return;
        	}	   
        	iter = parameter_map.find("index_name");
        		if (iter != parameter_map.end()) {
            	index_name = iter->second;
        	} else {
            	std::cout<<"sscan format error: index_name does not exist!"<<std::endl;
            	return;
        	}
        	iter = parameter_map.find("st");
        	if (iter != parameter_map.end()) {
            	st = boost::lexical_cast<uint64_t>(iter->second);
        	} else {
            	std::cout<<"sscan format error: st does not exist!"<<std::endl;
            	return;
        	}
			iter = parameter_map.find("et");
        	if (iter != parameter_map.end()) {
            	et = boost::lexical_cast<uint64_t>(iter->second);
        	} else {
            	std::cout<<"sscan format error: et does not exist!"<<std::endl;
            	return;
        	}
			iter = parameter_map.find("ts_name");
        	if (iter != parameter_map.end()) {
            	ts_name = iter->second;
        	} else {
            	std::cout<<"sscan format error: ts_name does not exist!"<<std::endl;
            	return;
        	}
			iter = parameter_map.find("limit");
        	if (iter != parameter_map.end()) {
            	limit = boost::lexical_cast<uint32_t>(iter->second);
        	}
    	} else {
        	if (parts.size() > 7) {
            	limit = boost::lexical_cast<uint32_t>(parts[7]);
        	}
        	tid = boost::lexical_cast<uint32_t>(parts[1]);
        	pid = boost::lexical_cast<uint32_t>(parts[2]);
			key = parts[3];
			index_name = parts[4];
			st = boost::lexical_cast<uint64_t>(parts[5]);	
			et = boost::lexical_cast<uint64_t>(parts[6]);	
		}
    } catch (std::exception const& e) {
        std::cout << "Invalid args. tid pid should be uint32_t, st and et should be uint64_t, limit should be uint32" << std::endl;
        return;
    }
	std::string msg;
	::rtidb::base::KvIterator* it = NULL;
	if (has_ts_col) {
		it = client->Scan(tid, pid, key, st, et, index_name, ts_name, limit, msg); 
	} else {
		it = client->Scan(tid, pid, key, st, et, index_name, limit, msg); 
	}
	if (it == NULL) {
		std::cout << "Fail to scan table. error msg: " << msg << std::endl;
	} else {
		::rtidb::api::TableMeta table_meta;
		bool ok = client->GetTableSchema(tid, pid, table_meta);
		if(!ok) {
			std::cout << "No schema for table, please use command scan" << std::endl;
			return;
		}
		std::string schema = table_meta.schema();
		::rtidb::api::TableStatus table_status;
		if (!client->GetTableStatus(tid, pid, table_status)) {
			std::cout << "Fail to get table status" << std::endl;
			return;
		}
		std::vector<::rtidb::base::ColumnDesc> raw;
		::rtidb::base::SchemaCodec codec;
		codec.Decode(schema, raw);
		::rtidb::nameserver::CompressType compress_type = ::rtidb::nameserver::kNoCompress;
		if (table_status.compress_type() == ::rtidb::api::CompressType::kSnappy) {
			compress_type = ::rtidb::nameserver::kSnappy;
		}
		::rtidb::base::ShowTableRows(raw, it, compress_type);
		delete it;
	}
}

void HandleClientSPut(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
	if (parts.size() < 5) {
		std::cout << "Bad put format, eg put tid pid time value" << std::endl;
		return;
	}
	try {
        uint32_t tid = boost::lexical_cast<uint32_t>(parts[1]);
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        ::rtidb::api::TableMeta table_meta;
        bool ok = client->GetTableSchema(tid, pid, table_meta);
        if (!ok) {
            std::cout << "Fail to get table schema" << std::endl;
            return;
        }
        std::string schema = table_meta.schema();
        if (schema.empty()) {
            std::cout << "No schema for table, please use put command" << std::endl;
            return;
        }
        ::rtidb::api::TableStatus table_status;
        if (!client->GetTableStatus(tid, pid, table_status)) {
            std::cout << "Fail to get table status" << std::endl;
            return;
        }
        std::vector<::rtidb::base::ColumnDesc> raw;
        ::rtidb::base::SchemaCodec scodec;
        scodec.Decode(schema, raw);
        std::string buffer;
        uint32_t cnt = parts.size() - 4;
        if (cnt != raw.size()) {
            std::cout << "Input value mismatch schema" << std::endl;
            return;
        }
        std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>> dimensions;
        if (EncodeMultiDimensionData(std::vector<std::string>(parts.begin() + 4, parts.end()), raw, 0, buffer, dimensions) < 0) {
            std::cout << "Encode data error" << std::endl;
            return;
        }
        if (table_status.compress_type() == ::rtidb::api::CompressType::kSnappy) {
            std::string compressed;
            ::snappy::Compress(buffer.c_str(), buffer.length(), &compressed);
            buffer = compressed;
        }
        ok = client->Put(boost::lexical_cast<uint32_t>(parts[1]),
                         boost::lexical_cast<uint32_t>(parts[2]),
                         boost::lexical_cast<uint64_t>(parts[3]),
                         buffer,
                         dimensions[0]);
        if (ok) {
            std::cout << "Put ok" << std::endl;
        }else {
            std::cout << "Put failed" << std::endl; 
        }
    } catch(std::exception const& e) {
        std::cout << e.what() << std::endl;
    } 
}

void HandleClientDelete(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad delete format" << std::endl;
        return;
    }
    try {
        uint32_t tid = boost::lexical_cast<uint32_t>(parts[1]);
        uint32_t pid = boost::lexical_cast<uint32_t>(parts[2]);
        std::string msg;
        std::string idx_name;
        if (parts.size() > 4) {
            idx_name = parts[4];
        }
        if (client->Delete(tid, pid, parts[3], idx_name, msg)) {
            std::cout << "Delete ok" << std::endl;
        } else {
            std::cout << "Delete failed" << std::endl; 
        }
    } catch (std::exception const& e) {
        std::cout<< "Invalid args, tid pid should be uint32_t" << std::endl;
    }
}

void HandleClientBenScan(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    uint64_t et = 0;
    uint32_t tid = 1;
    uint32_t pid = 1;
    uint64_t key_num = 1000000;
    uint32_t times = 10000;
    int num = 100;
    uint32_t limit = 0;
    if (parts.size() >= 3) {
        try {
            tid = ::boost::lexical_cast<uint32_t>(parts[1]);
            pid = ::boost::lexical_cast<uint32_t>(parts[2]);
            if (parts.size() >= 4) {
                key_num = ::boost::lexical_cast<uint64_t>(parts[3]);
            }
            if (parts.size() >= 5) {
                times = ::boost::lexical_cast<uint32_t>(parts[4]);
            }
            if (parts.size() >= 6) {
                num = ::boost::lexical_cast<int>(parts[5]);
            }
            if (parts.size() >= 7) {
                limit = ::boost::lexical_cast<uint32_t>(parts[6]);
            }
        } catch (boost::bad_lexical_cast& e) {
            std::cout << "Bad scan format" << std::endl;
            return;
        }
    }
    uint64_t base = 100000000;
    std::random_device rd;
    std::default_random_engine engine(rd());
    std::uniform_int_distribution<> dis(1, key_num);
    std::string msg;
    while(num > 0) {
        for (uint32_t i = 0; i < times; i++) {
            std::string key = std::to_string(base + dis(engine));
            uint64_t st = ::baidu::common::timer::get_micros() / 1000;
            msg.clear();
            //::rtidb::base::KvIterator* it = client->Scan(tid, pid, key.c_str(), st, et, msg, false);
            ::rtidb::base::KvIterator* it = client->Scan(tid, pid, key.c_str(), st, et, limit, msg);
            delete it;
        }
        client->ShowTp();
        num--;
    }
}

void StartClient() {
    if (FLAGS_endpoint.empty()) {
        std::cout << "Start failed! not set endpoint" << std::endl;
        return;
    }
    if (FLAGS_interactive) {
        std::cout << "Welcome to rtidb with version "<< RTIDB_VERSION_MAJOR
            << "." << RTIDB_VERSION_MEDIUM
            << "." << RTIDB_VERSION_MINOR << "."<<RTIDB_VERSION_BUG << std::endl;
    }
    ::rtidb::client::TabletClient client(FLAGS_endpoint);
    client.Init();
    std::string display_prefix = FLAGS_endpoint + "> ";
    while (true) {
        std::string buffer;
        if (!FLAGS_interactive) {
            buffer = FLAGS_cmd;
        } else {
            char *line = ::rtidb::base::linenoise(display_prefix.c_str());
            if (line == NULL) {
                return;
            }
            if (line[0] != '\0' && line[0] != '/') {
                buffer.assign(line);
                boost::trim(buffer);
                if (!buffer.empty()) {
                    ::rtidb::base::linenoiseHistoryAdd(line);
                }
            }
            ::rtidb::base::linenoiseFree(line);
            if (buffer.empty()) {
                continue;
            }
        }
        std::vector<std::string> parts;
        ::rtidb::base::SplitString(buffer, " ", &parts);
        if (parts.empty()) {
            continue;
        } else if (parts[0] == "put") {
            HandleClientPut(parts, &client);
        } else if (parts[0] == "sput") {
            HandleClientSPut(parts, &client);
        } else if (parts[0] == "create") {
            HandleClientCreateTable(parts, &client);
        } else if (parts[0] == "get") {
            HandleClientGet(parts, &client);
        } else if (parts[0] == "sget") {
            HandleClientSGet(parts, &client);
        } else if (parts[0] == "screate") {
            HandleClientSCreateTable(parts, &client);
        } else if (parts[0] == "scan") {
            HandleClientScan(parts, &client);
        } else if (parts[0] == "sscan") {
            HandleClientSScan(parts, &client);
        } else if (parts[0] == "delete") {
            HandleClientDelete(parts, &client);
        } else if (parts[0] == "count") {
            HandleClientCount(parts, &client);
        } else if (parts[0] == "preview") {
            HandleClientPreview(parts, &client);
        } else if (parts[0] == "showschema") {
            HandleClientShowSchema(parts, &client);
        } else if (parts[0] == "getfollower") {
            HandleClientGetFollower(parts, &client);
        } else if (parts[0] == "benput") {
            HandleClientBenPut(parts, &client);
        } else if (parts[0] == "benscan") {
            HandleClientBenScan(parts, &client);
        } else if (parts[0] == "benget") {
            HandleClientBenGet(parts, &client);
        } else if (parts[0] == "benchmark") {
            HandleClientBenchmark(&client);
        } else if (parts[0] == "drop") {
            HandleClientDropTable(parts, &client);
        } else if (parts[0] == "addreplica") {
            HandleClientAddReplica(parts, &client);
        } else if (parts[0] == "delreplica") {
            HandleClientDelReplica(parts, &client);
        } else if (parts[0] == "makesnapshot") {
            HandleClientMakeSnapshot(parts, &client);
        } else if (parts[0] == "pausesnapshot") {
            HandleClientPauseSnapshot(parts, &client);
        } else if (parts[0] == "recoversnapshot") {
            HandleClientRecoverSnapshot(parts, &client);
        } else if (parts[0] == "sendsnapshot") {
            HandleClientSendSnapshot(parts, &client);
        } else if (parts[0] == "loadtable") {
            HandleClientLoadTable(parts, &client);
        } else if (parts[0] == "changerole") {
            HandleClientChangeRole(parts, &client);
        } else if (parts[0] == "gettablestatus") {
            HandleClientGetTableStatus(parts, &client);
        } else if (parts[0] == "setexpire") {
            HandleClientSetExpire(parts, &client);
        } else if (parts[0] == "setttlclock") {
            HandleClientSetTTLClock(parts, &client);
        } else if (parts[0] == "connectzk") {
            HandleClientConnectZK(parts, &client);
        } else if (parts[0] == "disconnectzk") {
            HandleClientDisConnectZK(parts, &client);
        } else if (parts[0] == "setttl") {
            HandleClientSetTTL(parts, &client);
        } else if (parts[0] == "setlimit") {
            HandleClientSetLimit(parts, &client);
        } else if (parts[0] == "exit" || parts[0] == "quit") {
            std::cout << "bye" << std::endl;
            return;
        } else if (parts[0] == "help" || parts[0] == "man") {
            HandleClientHelp(parts, &client);
        } else {
            std::cout << "unsupported cmd" << std::endl;
        }
        if (!FLAGS_interactive) {
            return;
        }
    }
}

void StartNsClient() {
    std::string endpoint;
    if (FLAGS_interactive) {
        std::cout << "Welcome to rtidb with version "<< RTIDB_VERSION_MAJOR
            << "." << RTIDB_VERSION_MEDIUM
            << "." << RTIDB_VERSION_MINOR << "."<<RTIDB_VERSION_BUG << std::endl;
    }
    std::shared_ptr<ZkClient> zk_client;
    if (!FLAGS_zk_cluster.empty()) {
        zk_client = std::make_shared<ZkClient>(FLAGS_zk_cluster, 1000, "", FLAGS_zk_root_path);
        if (!zk_client->Init()) {
            std::cout << "zk client init failed" << std::endl;
            return;
        }
        std::string node_path = FLAGS_zk_root_path + "/leader";
        std::vector<std::string> children;
        if (!zk_client->GetChildren(node_path, children) || children.empty()) {
            std::cout << "get children failed" << std::endl;
            return;
        }
        std::string leader_path = node_path + "/" + children[0];
        if (!zk_client->GetNodeValue(leader_path, endpoint)) {
            std::cout << "get leader failed" << std::endl;
            return;
        }
        std::cout << "ns leader: " << endpoint << std::endl;
    } else if (!FLAGS_endpoint.empty()) {
        endpoint = FLAGS_endpoint;
    } else {
        std::cout << "Start failed! not set endpoint or zk_cluster" << std::endl;
        return;
    }
    ::rtidb::client::NsClient client(endpoint);
    if (client.Init() < 0) {
        std::cout << "client init failed" << std::endl;
        return;
    }
    std::string display_prefix = endpoint + "> ";
    while (true) {
        std::string buffer;
        if (!FLAGS_interactive) {
            buffer = FLAGS_cmd;
        } else {
	        char *line = ::rtidb::base::linenoise(display_prefix.c_str());
            if (line == NULL) {
                return;
            }
            if (line[0] != '\0' && line[0] != '/') { 
                buffer.assign(line);
                boost::trim(buffer);
                if (!buffer.empty()) {
                    ::rtidb::base::linenoiseHistoryAdd(line);
                }
            }
            ::rtidb::base::linenoiseFree(line);
            if (buffer.empty()) {
                continue;
            }
        }
        std::vector<std::string> parts;
        ::rtidb::base::SplitString(buffer, " ", &parts);
        if (parts.empty()) {
            continue;
        } else if (parts[0] == "showtablet") {
            HandleNSShowTablet(parts, &client);
        } else if (parts[0] == "showns") {
            HandleNSShowNameServer(parts, &client, zk_client);
        } else if (parts[0] == "showopstatus") {
            HandleNSShowOPStatus(parts, &client);
        } else if (parts[0] == "create") {
            HandleNSCreateTable(parts, &client);
        } else if (parts[0] == "put") {
            HandleNSPut(parts, &client);
        } else if (parts[0] == "scan") {
            HandleNSScan(parts, &client);
        } else if (parts[0] == "get") {
            HandleNSGet(parts, &client);
        } else if (parts[0] == "delete") {
            HandleNSDelete(parts, &client);
        } else if (parts[0] == "count") {
            HandleNSCount(parts, &client);
        } else if (parts[0] == "preview") {
            HandleNSPreview(parts, &client);
        } else if (parts[0] == "makesnapshot") {
            HandleNSMakeSnapshot(parts, &client);
        } else if (parts[0] == "addreplica") {
            HandleNSAddReplica(parts, &client);
        } else if (parts[0] == "delreplica") {
            HandleNSDelReplica(parts, &client);
        } else if (parts[0] == "drop") {
            HandleNSClientDropTable(parts, &client);
        } else if (parts[0] == "showtable") {
            HandleNSClientShowTable(parts, &client);
        } else if (parts[0] == "showschema") {
            HandleNSClientShowSchema(parts, &client);
        } else if (parts[0] == "confset") {
            HandleNSClientConfSet(parts, &client);
        } else if (parts[0] == "confget") {
            HandleNSClientConfGet(parts, &client);
        } else if (parts[0] == "changeleader") {
            HandleNSClientChangeLeader(parts, &client);
        } else if (parts[0] == "offlineendpoint") {
            HandleNSClientOfflineEndpoint(parts, &client);
        } else if (parts[0] == "migrate") {
            HandleNSClientMigrate(parts, &client);
        } else if (parts[0] == "recoverendpoint") {
            HandleNSClientRecoverEndpoint(parts, &client);
        } else if (parts[0] == "recovertable") {
            HandleNSClientRecoverTable(parts, &client);
        } else if (parts[0] == "connectzk") {
            HandleNSClientConnectZK(parts, &client);
        } else if (parts[0] == "disconnectzk") {
            HandleNSClientDisConnectZK(parts, &client);
        } else if (parts[0] == "gettablepartition") {
            HandleNSClientGetTablePartition(parts, &client);
        } else if (parts[0] == "settablepartition") {
            HandleNSClientSetTablePartition(parts, &client);
        } else if (parts[0] == "updatetablealive") {
            HandleNSClientUpdateTableAlive(parts, &client);
        } else if (parts[0] == "setttl") {
            HandleNSClientSetTTL(parts, &client);
        } else if (parts[0] == "cancelop") {
            HandleNSClientCancelOP(parts, &client);
        } else if (parts[0] == "exit" || parts[0] == "quit") {
            std::cout << "bye" << std::endl;
            return;
        } else if (parts[0] == "help" || parts[0] == "man") {
            HandleNSClientHelp(parts, &client);
        } else {
            std::cout << "unsupported cmd" << std::endl;
        }
        if (!FLAGS_interactive) {
            return;
        }
    }
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_role == "tablet") {
        StartTablet();
    } else if (FLAGS_role == "client") {
        StartClient();
    } else if (FLAGS_role == "nameserver") {
        StartNameServer();
    } else if (FLAGS_role == "ns_client") {
        StartNsClient();
    } else {
        std::cout << "Start failed! FLAGS_role must be tablet, client, nameserver or ns_client" << std::endl;
    }
    return 0;
}
