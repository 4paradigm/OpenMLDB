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

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DEFINE_string(endpoint, "127.0.0.1:9527", "Config the ip and port that rtidb serves for");
DEFINE_string(role, "tablet | master | client", "Set the rtidb role for start");
DEFINE_string(log_level, "debug | info", "Set the rtidb log level");

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
    if (!rpc_server.RegisterService(tablet)) {
        LOG(WARNING, "fail to register tablet rpc service");
        exit(1);
    }
    if (!rpc_server.Start(FLAGS_endpoint)) {
        LOG(WARNING, "fail to listen port %s", FLAGS_endpoint.c_str());
        exit(1);
    }
    LOG(INFO, "start tablet on port %s", FLAGS_endpoint.c_str());
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
}

void HandleClientBenPut(std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    char val[400];
    for (int i = 0; i < 400; i++) {
        val[i] ='0';
    }
    std::string sval(val);
    for (uint32_t i = 0 ; i < 10000; i++) {
        std::string key = parts[1] + "test" + boost::lexical_cast<std::string>(i);
        for (uint32_t j = 0; j < 1000; j++) {
            client->Put(1, 1, key, j, sval);
        }
        client->ShowTp();
    }
}

//
// the input format like create name tid pid ttl
void HandleClientCreateTable(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 5) {
        std::cout << "Bad create format" << std::endl;
        return;
    }
    bool ok = client->CreateTable(parts[1], boost::lexical_cast<uint32_t>(parts[2]),
            boost::lexical_cast<uint32_t>(parts[3]), boost::lexical_cast<uint32_t>(parts[4]));
    if (!ok) {
        std::cout << "Fail to create table" << std::endl;
    }else {
        std::cout << "Create table ok" << std::endl;
    }
}

// the input format like scan tid pid pk st et
void HandleClientScan(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    if (parts.size() < 6) {
        std::cout << "Bad scan format" << std::endl;
        return;
    }
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
            it->Next();
            if (print) {
                std::cout << index << "\t" << it->GetKey() << "\t" << it->GetValue().ToString() << std::endl;
            } 
            index ++;
        }
        delete it;
    }
}

void HandleClientBenScan(const std::vector<std::string>& parts, ::rtidb::client::TabletClient* client) {
    uint64_t st = 999;
    uint64_t et = 1;
    uint32_t tid = 1;
    uint32_t pid = 1;
    for (uint32_t i = 0; i < 1000; i++) {
        std::string key = parts[1] + "test" + boost::lexical_cast<std::string>(i);
        ::rtidb::base::KvIterator* it = client->Scan(tid, pid, key, st, et, true);
        delete it;
    }
    client->ShowTp();
}

void StartClient() {
    //::baidu::common::SetLogLevel(DEBUG);
    std::cout << "Welcome to rtidb!" << std::endl;
    ::rtidb::client::TabletClient client(FLAGS_endpoint);
    while (!s_quit) {
        std::cout << ">";
        std::string buffer;
        std::getline(std::cin, buffer);
        if (buffer.empty()) {
            continue;
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
