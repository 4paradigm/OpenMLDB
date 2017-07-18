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
#include "logging.h"

#include "tablet/tablet_impl.h"
#include "client/tablet_client.h"
#include "base/strings.h"
#include "base/kv_iterator.h"
#include "timer.h"
#include "version.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DEFINE_string(endpoint, "127.0.0.1:9527", "Config the ip and port that rtidb serves for");
DEFINE_string(role, "tablet | master | client", "Set the rtidb role for start");
DEFINE_string(log_level, "debug | info", "Set the rtidb log level");
DEFINE_string(cmd, "", "Set the command");
DEFINE_bool(interactive, true, "Set the interactive");

static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/){
    s_quit = true;
}

void StartTablet() {
    //TODO(wangtaize) optimalize options
    if (FLAGS_log_level == "debug") {
        ::baidu::common::SetLogLevel(DEBUG);
    }else {
        ::baidu::common::SetLogLevel(INFO);
    }
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);
    ::rtidb::tablet::TabletImpl* tablet = new ::rtidb::tablet::TabletImpl();
    tablet->Init();
    sofa::pbrpc::Servlet webservice =
                sofa::pbrpc::NewPermanentExtClosure(tablet, &rtidb::tablet::TabletImpl::WebService);
    if (!rpc_server.RegisterService(tablet)) {
        LOG(WARNING, "fail to register tablet rpc service");
        exit(1);
    }
    rpc_server.RegisterWebServlet("/tablet", webservice);
    if (!rpc_server.Start(FLAGS_endpoint)) {
        LOG(WARNING, "fail to listen port %s", FLAGS_endpoint.c_str());
        exit(1);
    }
    LOG(INFO, "start tablet on port %s with version %d.%d.%d", FLAGS_endpoint.c_str(),
            RTIDB_VERSION_MAJOR,
            RTIDB_VERSION_MINOR,
            RTIDB_VERSION_BUG);
    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);
    while (!s_quit) {
        sleep(1);
    }
}

// the input format like put 1 1 key time value
void HandleClientPut(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad put format" << std::endl;
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
}

// the input format like create name tid pid ttl leader endpoints 
void HandleClientCreateTable(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad create format" << std::endl;
        return;
    }

    bool leader = true;
    if (parts.size() > 5 && parts[5] == "false") {
        leader = false;
    }

    std::vector<std::string> endpoints;
    for (size_t i = 6; i < parts.size(); i++) {
        endpoints.push_back(parts[i]);
    }

    try {
        uint32_t ttl = 0;
        if (parts.size() > 4) {
            ttl = boost::lexical_cast<uint32_t>(parts[4]);
        }
        bool ok = client->CreateTable(parts[1], 
                                      boost::lexical_cast<uint32_t>(parts[2]),
                                      boost::lexical_cast<uint32_t>(parts[3]), 
                                      ttl, leader, endpoints);
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
    bool ok = client->DropTable(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]));
    if (ok) {
        std::cout << "Drop table ok" << std::endl;
    }else {
        std::cout << "Fail to drop table" << std::endl;
    }
}

void HandleClientAddReplica(const std::vector<std::string> parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 4) {
        std::cout << "Bad addreplica format" << std::endl;
        return;
    }
    bool ok = client->AddReplica(boost::lexical_cast<uint32_t>(parts[1]), boost::lexical_cast<uint32_t>(parts[2]), parts[3]);
    if (ok) {
        std::cout << "AddReplica ok" << std::endl;
    }else {
        std::cout << "Fail to Add Replica" << std::endl;
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
                boost::lexical_cast<uint64_t>(parts[5]),
                false);
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
            ::rtidb::base::KvIterator* it = client->Scan(tid, pid, key, st, et, true);
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

void HandleClientBenScan(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    uint64_t st = 999;
    uint64_t et = 0;
    uint32_t tid = 1;
    uint32_t pid = 1;
    uint32_t times = 10;
    if (parts.size() >= 3) {
        times = ::boost::lexical_cast<uint32_t>(parts[2]);
    }

    for (uint32_t i = 0; i < 10; i++) {
        std::string key = parts[1] + "test" + boost::lexical_cast<std::string>(i);
        ::rtidb::base::KvIterator* it = client->Scan(tid, pid, key, st, et, true);
        delete it;
    }
    client->ShowTp();
    for (uint32_t j = 0; j < times; j++) {
        for (uint32_t i = 0; i < 500; i++) {
            std::string key = parts[1] + "test" + boost::lexical_cast<std::string>(i);
            ::rtidb::base::KvIterator* it = client->Scan(tid, pid, key, st, et, true);
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
        }else if (parts[0] == "create") {
            HandleClientCreateTable(parts, &client);
        }else if (parts[0] == "scan") {
            HandleClientScan(parts, &client);
        }else if (parts[0] == "benput") {
            HandleClientBenPut(parts, &client);
        }else if (parts[0] == "benscan") {
            HandleClientBenScan(parts, &client);
        }else if (parts[0] == "benchmark") {
            HandleClientBenchmark(&client);
        }else if (parts[0] == "drop") {
            HandleClientDropTable(parts, &client);
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
    }
    return 0;
}
