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

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>
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
#include "timer.h"
#include "version.h"
#include "proto/tablet.pb.h"
#include "proto/client.pb.h"
#include "proto/name_server.pb.h"
#include "tprinter.h"
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_string(endpoint);
DECLARE_string(scan_endpoint);
DECLARE_int32(thread_pool_size);
DECLARE_int32(scan_thread_pool_size);
DEFINE_string(role, "tablet | nameserver | client | ns_client", "Set the rtidb role for start");
DEFINE_string(cmd, "", "Set the command");
DEFINE_bool(interactive, true, "Set the interactive");

DEFINE_string(log_dir, "", "Config the log dir");
DEFINE_int32(log_file_size, 1024, "Config the log size in MB");
DEFINE_int32(log_file_count, 24, "Config the log count");
DEFINE_string(log_level, "debug", "Set the rtidb log level, eg: debug or info");

static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/){
    s_quit = true;
}

void SetupLog() {
    // Config log 
    if (FLAGS_log_level == "debug") {
        ::baidu::common::SetLogLevel(DEBUG);
    }else {
        ::baidu::common::SetLogLevel(INFO);
    }
    if (!FLAGS_log_dir.empty()) {
        std::string info_file = FLAGS_log_dir + "/rtidb.info.log";
        std::string warning_file = FLAGS_log_dir + "/rtidb.warning.log";
        ::baidu::common::SetLogFile(info_file.c_str());
        ::baidu::common::SetWarningFile(warning_file.c_str());
    }
    ::baidu::common::SetLogCount(FLAGS_log_file_count);
    ::baidu::common::SetLogSize(FLAGS_log_file_size);
}

void StartNameServer() {
    SetupLog();
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);
    ::rtidb::nameserver::NameServerImpl* name_server = new ::rtidb::nameserver::NameServerImpl();
    name_server->Init();
    sofa::pbrpc::Servlet webservice =
                sofa::pbrpc::NewPermanentExtClosure(name_server, &rtidb::nameserver::NameServerImpl::WebService);
    if (!rpc_server.RegisterService(name_server)) {
        LOG(WARNING, "fail to register nameserver rpc service");
        exit(1);
    }
    rpc_server.RegisterWebServlet("/nameserver", webservice);
    if (!rpc_server.Start(FLAGS_endpoint)) {
        LOG(WARNING, "fail to listen port %s", FLAGS_endpoint.c_str());
        exit(1);
    }
    LOG(INFO, "start nameserver on port %s with version %d.%d.%d", FLAGS_endpoint.c_str(),
            RTIDB_VERSION_MAJOR,
            RTIDB_VERSION_MINOR,
            RTIDB_VERSION_BUG);
    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);
    while (!s_quit) {
        sleep(1);
    }
}

void StartTablet() {
    SetupLog();
    sofa::pbrpc::RpcServerOptions scan_options;
    scan_options.work_thread_num = FLAGS_scan_thread_pool_size;
    sofa::pbrpc::RpcServer scan_rpc_server(scan_options);
    ::rtidb::tablet::TabletImpl* tablet = new ::rtidb::tablet::TabletImpl();
    bool ok = tablet->Init();
    if (!ok) {
        LOG(WARNING, "fail to init tablet");
        exit(1);
    }
    sofa::pbrpc::Servlet webservice =
                sofa::pbrpc::NewPermanentExtClosure(tablet, &rtidb::tablet::TabletImpl::WebService);
    if (!scan_rpc_server.RegisterService(tablet)) {
        LOG(WARNING, "fail to register tablet rpc service");
        exit(1);
    }
    scan_rpc_server.RegisterWebServlet("/tablet", webservice);
    if (!scan_rpc_server.Start(FLAGS_scan_endpoint)) {
        LOG(WARNING, "fail to listen port %s", FLAGS_scan_endpoint.c_str());
        exit(1);
    }
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);
    scan_options.work_thread_num = FLAGS_thread_pool_size;
    if (!rpc_server.RegisterService(tablet)) {
        LOG(WARNING, "fail to register tablet rpc service");
        exit(1);
    }
    rpc_server.RegisterWebServlet("/tablet", webservice);
    if (!rpc_server.Start(FLAGS_endpoint)) {
        LOG(WARNING, "fail to listen port %s", FLAGS_endpoint.c_str());
        exit(1);
    }
    LOG(INFO, "start tablet on port %s and scan port %s with version %d.%d.%d", FLAGS_endpoint.c_str(),
            FLAGS_scan_endpoint.c_str(),
            RTIDB_VERSION_MAJOR,
            RTIDB_VERSION_MINOR,
            RTIDB_VERSION_BUG);
    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);
    while (!s_quit) {
        sleep(1);
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
    bool ok = client->ShowTablet(tablets);
    if (!ok) {
        std::cout << "Fail to show tablets" << std::endl;
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

void HandleNSMakeSnapshot(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 3) {
        std::cout << "Bad format" << std::endl;
        return;
    }
    try {
        uint32_t tid = boost::lexical_cast<uint32_t>(parts[2]);
        bool ok = client->MakeSnapshot(parts[1], tid);
        if (!ok) {
            std::cout << "Fail to show tablets" << std::endl;
            return;
        }
    } catch(std::exception const& e) {
        std::cout << "Invalid args. pid should be uint32_t" << std::endl;
    } 
}

void HandleNSCreateTable(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    if (parts.size() < 2) {
        std::cout << "Bad format" << std::endl;
        return;
    }
	::rtidb::client::TableInfo table_info;
	int fd = open(parts[1].c_str(), O_RDONLY);
    if (fd < 0) {
        std::cout << "can not open file " << parts[1] << std::endl;
        return;
    }
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    if (!google::protobuf::TextFormat::Parse(&fileInput, &table_info)) {
        std::cout << "table meta file format error" << std::endl;
        return;
    }

    ::rtidb::nameserver::TableInfo ns_table_info;
    ns_table_info.set_name(table_info.name());
    ns_table_info.set_ttl(table_info.ttl());
    ns_table_info.set_seg_cnt(table_info.seg_cnt());
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
                printf("pid_group[%s] format error.", pid_group.c_str());
                return;
            }
            start_index = boost::lexical_cast<uint32_t>(vec[0]);
            end_index = boost::lexical_cast<uint32_t>(vec[1]);

        }
        for (uint32_t pid = start_index; pid <= end_index; pid++) {
            ::rtidb::nameserver::TablePartition* table_partition = ns_table_info.add_table_partition();
            table_partition->set_endpoint(table_info.table_partition(idx).endpoint());
            table_partition->set_pid(pid);
            table_partition->set_is_leader(table_info.table_partition(idx).is_leader());
            if (table_info.table_partition(idx).is_leader()) {
                if (leader_map.find(pid) != leader_map.end()) {
                    printf("pid %u has two leader\n", pid);
                    return;
                }
                leader_map.insert(std::make_pair(pid, table_info.table_partition(idx).endpoint()));
            } else {
                if (follower_map.find(pid) == follower_map.end()) {
                    follower_map.insert(std::make_pair(pid, std::set<std::string>()));
                }
                if (follower_map[pid].find(table_info.table_partition(idx).endpoint()) != follower_map[pid].end()) {
                    printf("pid %u has same follower on %s\n", pid, table_info.table_partition(idx).endpoint().c_str());
                    return;
                }
                follower_map[pid].insert(table_info.table_partition(idx).endpoint());
            }
        }
    }

    // check follower's leader 
    for (const auto& kv : follower_map) {
        auto iter = leader_map.find(kv.first);
        if (iter == leader_map.end()) {
            printf("pid %u has not leader\n", kv.first);
            return;
        }
        if (kv.second.find(iter->second) != kv.second.end()) {
            printf("pid %u leader and follower at same endpoint %s\n", kv.first, iter->second.c_str());
            return;
        }
    }

	if (!client->CreateTable(ns_table_info)) {
		std::cout << "Fail to create table" << std::endl;
		return;
	}
    std::cout << "Create table ok" << std::endl;
}

void HandleNSShowOPStatus(const std::vector<std::string>& parts, ::rtidb::client::NsClient* client) {
    std::vector<std::string> row;
    row.push_back("op_id");
    row.push_back("op_typee");
    row.push_back("status");
    row.push_back("start_time");
    row.push_back("execute_time");
    row.push_back("end_time");
    row.push_back("cur_task");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    ::rtidb::nameserver::ShowOPStatusResponse response;
    bool ok = client->ShowOPStatus(response);
    if (!ok) {
        std::cout << "Fail to show tablets" << std::endl;
        return;
    }
    for (int idx = 0; idx < response.op_status_size(); idx++) { 
        std::vector<std::string> row;
        row.push_back(std::to_string(response.op_status(idx).op_id()));
        row.push_back(response.op_status(idx).op_type());
        row.push_back(response.op_status(idx).status());
        time_t rawtime = (time_t)response.op_status(idx).start_time();
        tm* timeinfo = localtime(&rawtime);
        char buf[20];
        strftime(buf, 20, "%Y%m%d%H%M%S", timeinfo);
        row.push_back(buf);
        if (response.op_status(idx).end_time() != 0) {
            row.push_back(std::to_string(response.op_status(idx).end_time() - response.op_status(idx).start_time()));
            rawtime = (time_t)response.op_status(idx).end_time();
            timeinfo = localtime(&rawtime);
            buf[0] = '\0';
            strftime(buf, 20, "%Y%m%d%H%M%S", timeinfo);
            row.push_back(buf);
        } else {
            uint64_t cur_time = time(0);
            row.push_back(std::to_string(cur_time - response.op_status(idx).start_time()));
            row.push_back("-");
        }
        row.push_back(response.op_status(idx).task_type());
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleClientGet(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad get format, eg get tid pid key time" << std::endl;
        return;
    }
    try {
        std::string value;
        bool ok = client->Get(boost::lexical_cast<uint32_t>(parts[1]),
                              boost::lexical_cast<uint32_t>(parts[2]),
                              parts[3],
                              boost::lexical_cast<uint64_t>(parts[4]),
                              value);
        if (ok) {
            std::cout << "value :" << value << std::endl;
        }else {
            std::cout << "Get failed" << std::endl; 
        }

    
    } catch(std::exception const& e) {
        std::cout << "Invalid args tid and pid should be uint32_t" << std::endl;
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
    uint32_t size = 400;
    try {
        if (parts.size() >= 3) {
            size = boost::lexical_cast<uint32_t>(parts[2]);
        }
        uint32_t times = 10000;
        if (parts.size() >= 4) {
            times = ::boost::lexical_cast<uint32_t>(parts[3]);
        }
        char val[size];
        for (uint32_t i = 0; i < size; i++) {
            val[i] ='0';
        }
        std::string sval(val);
        for (uint32_t i = 0 ; i < times; i++) {
            std::string key = parts[1] + "test" + boost::lexical_cast<std::string>(i);
            for (uint32_t j = 0; j < 1000; j++) {
                client->Put(1, 1, key, j, sval);
            }
            client->ShowTp();
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
        bool ok = client->CreateTable(parts[1], 
                                      boost::lexical_cast<uint32_t>(parts[2]),
                                      boost::lexical_cast<uint32_t>(parts[3]), 
                                      ttl, is_leader, endpoints, seg_cnt);
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

void AddPrintRow(const ::rtidb::api::TableStatus& table_status, ::baidu::common::TPrinter& tp) {
    std::vector<std::string> row;
    char buf[30];
    snprintf(buf, 30, "%u", table_status.tid());
    row.push_back(buf);
    snprintf(buf, 30, "%u", table_status.pid());
    row.push_back(buf);
    snprintf(buf, 30, "%lu", table_status.offset());
    row.push_back(buf);
    row.push_back(::rtidb::api::TableMode_Name(table_status.mode()));
    row.push_back(::rtidb::api::TableState_Name(table_status.state()));
    snprintf(buf, 30, "%u", table_status.ttl());
    row.push_back(buf);
    tp.AddRow(row);
}

void HandleClientGetTableStatus(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    std::vector<std::string> row;
    row.push_back("tid");
    row.push_back("pid");
    row.push_back("offset");
    row.push_back("mode");
    row.push_back("state");
    row.push_back("ttl");
    ::baidu::common::TPrinter tp(row.size());
    tp.AddRow(row);
    if (parts.size() == 3) {
        ::rtidb::api::TableStatus table_status;
        try {
            if (client->GetTableStatus(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), table_status) == 0) {
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
        if (client->GetTableStatus(response) < 0) {
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

void HandleClientChangeRole(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad changerole format" << std::endl;
        return;
    }
    if (parts[3].compare("leader") == 0) {
        try {
            bool ok = client->ChangeRole(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), true);
            if (ok) {
                std::cout << "ChangeRole ok" << std::endl;
            } else {
                std::cout << "Fail to Change leader" << std::endl;
            }
        } catch (boost::bad_lexical_cast& e) {
            std::cout << "Bad changerole format" << std::endl;
        }
    } else {
        std::cout << "not support to change follower" << std::endl;
    }
}

// the input format like scan tid pid pk st et
void HandleClientScan(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad scan format" << std::endl;
        return;
    }
    try {
        ::rtidb::base::KvIterator* it = client->Scan(boost::lexical_cast<uint32_t>(parts[1]), 
                boost::lexical_cast<uint32_t>(parts[2]),
                parts[3], boost::lexical_cast<uint64_t>(parts[4]), 
                boost::lexical_cast<uint64_t>(parts[5]));
        if (it == NULL) {
            std::cout << "Fail to scan table" << std::endl;
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
    for (uint32_t j = 0; j < run_times; j++) {
        for (uint32_t i = 0; i < 500 * 4; i++) {
            std::string key =boost::lexical_cast<std::string>(ns) + "test" + boost::lexical_cast<std::string>(i);
            ::rtidb::base::KvIterator* it = client->Scan(tid, pid, key, st, et);
            delete it;
        }
        client->ShowTp();
    }
}

void HandleClientBenBatchGet(uint32_t tid, uint32_t pid, uint32_t run_times, uint32_t ns,
        ::rtidb::client::TabletClient* client) {
    for (uint32_t j = 0; j < run_times; j++) {
        std::vector<std::string> keys;
        for (uint32_t i = 0; i < 20; i++) {
            std::string key =boost::lexical_cast<std::string>(ns) + "test" + boost::lexical_cast<std::string>(i);
            keys.push_back(key);
        }
        for (uint32_t k = 0; k < 500 * 4; k++)  {
            ::rtidb::base::KvIterator* kit = client->BatchGet(tid, pid, keys);
            delete kit;
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

    std::cout << "Percentile:Start benchmark batchget size:40" << std::endl;
    HandleClientBenBatchGet(2, 1,  times, 1, client);
    std::cout << "Percentile:Start benchmark batchget size:80" << std::endl;
    HandleClientBenBatchGet(2, 1,  times, 2, client);
    std::cout << "Percentile:Start benchmark batchget size:200" << std::endl;
    HandleClientBenBatchGet(2, 1,  times, 3, client);
    std::cout << "Percentile:Start benchmark batchget size:400" << std::endl;
    HandleClientBenBatchGet(2, 1,  times, 4, client);


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
    if (parts.size() < 6) {
        std::cout << "Bad create format, input like create <name> <tid> <pid> <ttl> <seg_cnt>" << std::endl;
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
        std::vector<std::pair<rtidb::base::ColType, std::string>> columns;
        for (uint32_t i = 6; i < parts.size(); i++) {
            std::vector<std::string> kv;
            ::rtidb::base::SplitString(parts[i], ":", &kv);
            if (kv.size() != 2) {
                continue;
            }
            if (kv[1] == "int32") {
                columns.push_back(std::make_pair(::rtidb::base::ColType::kInt32, kv[0]));
            }else if (kv[1] == "int64") {
                columns.push_back(std::make_pair(::rtidb::base::ColType::kInt64, kv[0]));
            }else if (kv[1] == "uint32") {
                columns.push_back(std::make_pair(::rtidb::base::ColType::kUInt32, kv[0]));
            }else if (kv[1] == "uint64") {
                columns.push_back(std::make_pair(::rtidb::base::ColType::kUInt64, kv[0]));
            }else if (kv[1] == "float") {
                columns.push_back(std::make_pair(::rtidb::base::ColType::kFloat, kv[0]));
            }else if (kv[1] == "double") {
                columns.push_back(std::make_pair(::rtidb::base::ColType::kDouble, kv[0]));
            }else if (kv[1] == "string") {
                columns.push_back(std::make_pair(::rtidb::base::ColType::kString, kv[0]));
            }
        }
        std::string schema;
        ::rtidb::base::SchemaCodec codec;
        codec.Encode(columns, schema);
        bool ok = client->CreateTable(parts[1], 
                                      boost::lexical_cast<uint32_t>(parts[2]),
                                      boost::lexical_cast<uint32_t>(parts[3]), 
                                      ttl,seg_cnt, schema);
        if (!ok) {
            std::cout << "Fail to create table" << std::endl;
        }else {
            std::cout << "Create table ok" << std::endl;
        }

    } catch(std::exception const& e) {
        std::cout << "Invalid args, tid , pid or ttl should be uint32_t" << std::endl;
    }
}

void HandleClientShowSchema(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 3) {
        std::cout <<  "Bad show schema format" << std::endl;
        return;
    }
    std::string schema;
    bool ok = client->GetTableSchema(boost::lexical_cast<uint32_t>(parts[1]),
                                    boost::lexical_cast<uint32_t>(parts[2]), schema);
    if(!ok || schema.empty()) {
        std::cout << "No schema for table" << std::endl;
        return;
    }
    std::vector<std::pair<::rtidb::base::ColType, std::string> > raw;
    ::rtidb::base::SchemaCodec codec;
    codec.Decode(schema, raw);

    ::baidu::common::TPrinter tp(3);
    std::vector<std::string> header;
    header.push_back("index");
    header.push_back("name");
    header.push_back("type");

    tp.AddRow(header);
    for (uint32_t i = 0; i < raw.size(); i++) {
        std::vector<std::string> row;
        row.push_back(boost::lexical_cast<std::string>(i));
        row.push_back(raw[i].second);
        switch (raw[i].first) {
            case ::rtidb::base::ColType::kInt32:
                row.push_back("int32");
                break;
            case ::rtidb::base::ColType::kInt64:
                row.push_back("int64");
                break;
            case ::rtidb::base::ColType::kUInt32:
                row.push_back("uint32");
                break;
            case ::rtidb::base::ColType::kUInt64:
                row.push_back("uint64");
                break;
            case ::rtidb::base::ColType::kDouble:
                row.push_back("double");
                break;
            case ::rtidb::base::ColType::kFloat:
                row.push_back("float");
                break;
            case ::rtidb::base::ColType::kString:
                row.push_back("string");
                break;
            default:
                break;
        }
        tp.AddRow(row);
    }
    tp.Print(true);
}

void HandleClientSScan(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad scan format" << std::endl;
        return;
    }
    try {
        ::rtidb::base::KvIterator* it = client->Scan(boost::lexical_cast<uint32_t>(parts[1]), 
                boost::lexical_cast<uint32_t>(parts[2]),
                parts[3], 
                boost::lexical_cast<uint64_t>(parts[4]), 
                boost::lexical_cast<uint64_t>(parts[5]));
        if (it == NULL) {
            std::cout << "Fail to scan table" << std::endl;
        }else {
            std::string schema;
            bool ok = client->GetTableSchema(boost::lexical_cast<uint32_t>(parts[1]),
                                             boost::lexical_cast<uint32_t>(parts[2]), schema);
            if(!ok) {
                std::cout << "No schema for table ,please use command scan" << std::endl;
                return;
            }
            std::vector<std::pair<::rtidb::base::ColType, std::string> > raw;
            ::rtidb::base::SchemaCodec codec;
            codec.Decode(schema, raw);
            ::baidu::common::TPrinter tp(raw.size() + 2);
            std::vector<std::string> row;
            row.push_back("pk");
            row.push_back("ts");
            for (uint32_t i = 0; i < raw.size(); i++) {
                row.push_back(raw[i].second);
            }
            tp.AddRow(row);
            uint32_t index = 1;
            while (it->Valid()) {
                rtidb::base::FlatArrayIterator fit(it->GetValue().data(), it->GetValue().size());
                std::vector<std::string> vrow;
                vrow.push_back(parts[3]);
                vrow.push_back(boost::lexical_cast<std::string>(it->GetKey()));
                while (fit.Valid()) {
                    std::string col;
                    if (fit.GetType() == ::rtidb::base::ColType::kString) {
                        fit.GetString(&col);
                    }else if (fit.GetType() == ::rtidb::base::ColType::kInt32) {
                        int32_t int32_col = 0;
                        fit.GetInt32(&int32_col);
                        col = boost::lexical_cast<std::string>(int32_col);
                    }else if (fit.GetType() == ::rtidb::base::ColType::kInt64) {
                        int64_t int64_col = 0;
                        fit.GetInt64(&int64_col);
                        col = boost::lexical_cast<std::string>(int64_col);
                    }else if (fit.GetType() == ::rtidb::base::ColType::kUInt32) {
                        uint32_t uint32_col = 0;
                        fit.GetUInt32(&uint32_col);
                        col = boost::lexical_cast<std::string>(uint32_col);
                    }else if (fit.GetType() == ::rtidb::base::ColType::kUInt64) {
                        uint64_t uint64_col = 0;
                        fit.GetUInt64(&uint64_col);
                        col = boost::lexical_cast<std::string>(uint64_col);
                    }else if (fit.GetType() == ::rtidb::base::ColType::kDouble) {
                        double double_col = 0.0d;
                        fit.GetDouble(&double_col);
                        col = boost::lexical_cast<std::string>(double_col);
                    }else if (fit.GetType() == ::rtidb::base::ColType::kFloat) {
                        float float_col = 0.0f;
                        fit.GetFloat(&float_col);
                        col = boost::lexical_cast<std::string>(float_col);
                    }
                    fit.Next();
                    vrow.push_back(col);
                }
                tp.AddRow(vrow);
                std::cout << std::endl;
                index ++;
                it->Next();
            }
            delete it;
            tp.Print(true);
        }

    } catch (std::exception const& e) {
        std::cout<< "Invalid args, tid pid should be uint32_t, st and et should be uint64_t" << std::endl;
    }

}

void HandleClientSPut(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad put format, eg put tid pid key time value" << std::endl;
        return;
    }
    try {
        std::string schema;
        bool ok = client->GetTableSchema(boost::lexical_cast<uint32_t>(parts[1]),
                                         boost::lexical_cast<uint32_t>(parts[2]),
                                         schema);

        if (!ok) {
            std::cout << "Fail to get table schema" << std::endl;
            return;
        }

        if (schema.empty()) {
            std::cout << "No schema for table, please use put command" << std::endl;
            return;
        }

        std::vector<std::pair<::rtidb::base::ColType, std::string> > raw;
        ::rtidb::base::SchemaCodec scodec;
        scodec.Decode(schema, raw);
        std::string buffer;
        uint32_t cnt = parts.size() - 5;
        ::rtidb::base::FlatArrayCodec codec(&buffer, (uint8_t) cnt);
        for (uint32_t i = 5; i < parts.size(); i++) {
            if (i-5 >= raw.size()) {
                std::cout << "Input mismatch schema" << std::endl;
                return;
            }
            if (raw[i - 5].first == ::rtidb::base::ColType::kInt32) {
                codec.Append(boost::lexical_cast<int32_t>(parts[i]));
            }else if (raw[i - 5].first == ::rtidb::base::ColType::kInt64) {
                codec.Append(boost::lexical_cast<int64_t>(parts[i]));
            }else if (raw[i - 5].first == ::rtidb::base::ColType::kUInt32) {
                codec.Append(boost::lexical_cast<uint32_t>(parts[i]));
            }else if (raw[i - 5].first == ::rtidb::base::ColType::kUInt64) {
                codec.Append(boost::lexical_cast<uint64_t>(parts[i]));
            }else if (raw[i - 5].first == ::rtidb::base::ColType::kFloat) {
                codec.Append(boost::lexical_cast<float>(parts[i]));
            }else if (raw[i - 5].first == ::rtidb::base::ColType::kDouble) {
                codec.Append(boost::lexical_cast<double>(parts[i]));
            }else if (raw[i - 5].first == ::rtidb::base::ColType::kString) {
                codec.Append(parts[i]);
            }
        }
        codec.Build();
        ok = client->Put(boost::lexical_cast<uint32_t>(parts[1]),
                            boost::lexical_cast<uint32_t>(parts[2]),
                            parts[3],
                            boost::lexical_cast<uint64_t>(parts[4]),
                            buffer);
        if (ok) {
            std::cout << "Put ok" << std::endl;
        }else {
            std::cout << "Put failed" << std::endl; 
        }
    } catch(std::exception const& e) {
        std::cout << e.what() << std::endl;
    } 
}

void HandleClientBenScan(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    uint64_t st = 999;
    uint64_t et = 0;
    uint32_t tid = 1;
    uint32_t pid = 1;
    uint32_t times = 10;
    if (parts.size() >= 3) {
        try {
            times = ::boost::lexical_cast<uint32_t>(parts[2]);
        } catch (boost::bad_lexical_cast& e) {
            std::cout << "Bad scan format" << std::endl;
            return;
        }
    }

    for (uint32_t i = 0; i < 10; i++) {
        std::string key = parts[1] + "test" + boost::lexical_cast<std::string>(i);
        ::rtidb::base::KvIterator* it = client->Scan(tid, pid, key, st, et);
        delete it;
    }
    client->ShowTp();
    for (uint32_t j = 0; j < times; j++) {
        for (uint32_t i = 0; i < 500; i++) {
            std::string key = parts[1] + "test" + boost::lexical_cast<std::string>(i);
            ::rtidb::base::KvIterator* it = client->Scan(tid, pid, key, st, et);
            delete it;
        }
        client->ShowTp();
    }
}

void StartClient() {
    //::baidu::common::SetLogLevel(DEBUG);
    std::cout << "Welcome to rtidb with version "<< RTIDB_VERSION_MAJOR
        << "." << RTIDB_VERSION_MINOR << "."<<RTIDB_VERSION_BUG << std::endl;
    ::rtidb::client::TabletClient client(FLAGS_endpoint);
    while (!s_quit) {
        std::cout << ">";
        std::string buffer;
        if (!FLAGS_interactive) {
            buffer = FLAGS_cmd;
        }else {
            std::getline(std::cin, buffer);
            if (buffer.empty()) {
                continue;
            }
        }
        std::vector<std::string> parts;
        ::rtidb::base::SplitString(buffer, " ", &parts);
        if (parts[0] == "put") {
            HandleClientPut(parts, &client);
        } else if (parts[0] == "sput") {
            HandleClientSPut(parts, &client);
        } else if (parts[0] == "create") {
            HandleClientCreateTable(parts, &client);
        } else if (parts[0] == "get") {
            HandleClientGet(parts, &client);
        } else if (parts[0] == "screate") {
            HandleClientSCreateTable(parts, &client);
        } else if (parts[0] == "scan") {
            HandleClientScan(parts, &client);
        } else if (parts[0] == "sscan") {
            HandleClientSScan(parts, &client);
        } else if (parts[0] == "showschema") {
            HandleClientShowSchema(parts, &client);
        } else if (parts[0] == "benput") {
            HandleClientBenPut(parts, &client);
        } else if (parts[0] == "benscan") {
            HandleClientBenScan(parts, &client);
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
        } else if (parts[0] == "loadtable") {
            HandleClientLoadTable(parts, &client);
        } else if (parts[0] == "changerole") {
            HandleClientChangeRole(parts, &client);
        } else if (parts[0] == "gettablestatus") {
            HandleClientGetTableStatus(parts, &client);
        } else if (parts[0] == "exit" || parts[0] == "quit") {
            std::cout << "bye" << std::endl;
            return;
        } else {
            std::cout << "unsupported cmd" << std::endl;
        }

        if (!FLAGS_interactive) {
            return;
        }
    }

}

void StartNsClient() {
    
    ::rtidb::client::NsClient client(FLAGS_endpoint);
    client.Init();
    while (!s_quit) {
        std::cout << ">";
        std::string buffer;
        if (!FLAGS_interactive) {
            buffer = FLAGS_cmd;
        }else {
            std::getline(std::cin, buffer);
            if (buffer.empty()) {
                continue;
            }
        }
        std::vector<std::string> parts;
        ::rtidb::base::SplitString(buffer, " ", &parts);
        if (parts[0] == "showtablet") {
            HandleNSShowTablet(parts, &client);
        } else  if (parts[0] == "showopstatus") {
            HandleNSShowOPStatus(parts, &client);
        } else  if (parts[0] == "create") {
            HandleNSCreateTable(parts, &client);
        } else  if (parts[0] == "makesnapshot") {
            HandleNSMakeSnapshot(parts, &client);
        } else if (parts[0] == "exit" || parts[0] == "quit") {
            std::cout << "bye" << std::endl;
            return;
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
    }else if (FLAGS_role == "client") {
        StartClient();
    }else if (FLAGS_role == "nameserver") {
        StartNameServer();
    }else if (FLAGS_role == "ns_client") {
        StartNsClient();
    }

    return 0;
}
