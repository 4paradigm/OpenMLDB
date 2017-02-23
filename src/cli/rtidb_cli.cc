//
// rtidb_cli.cc
// Copyright 2017 elasticlog <elasticlog01@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#include "rtidb_tablet_server.pb.h"

#include <iostream>
#include <vector>
#include <gflags/gflags.h>
#include <boost/lexical_cast.hpp>
#include "rpc/rpc_client.h"
#include "cli/rtidb_client.h"
#include "util/string_util.h"
#include <signal.h>
#include <unistd.h>

DEFINE_string(ts_endpoint, "127.0.0.1:9527", "config the ip and port that ts serves for");

static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/){
  s_quit = true;
}

void HandlePut(const std::string& input, rtidb::RtiDBClient* client) {
    std::vector<std::string> parts;
    ::rtidb::SplitString(input, " ", &parts);
    if (parts.size() < 4) {
        std::cout << "error format input" << std::endl;
        return;
    }
    client->Put(parts[1], parts[2], parts[3]);
}

void HandleScan(const std::string& input, rtidb::RtiDBClient* client) {
    std::vector<std::string> parts;
    ::rtidb::SplitString(input, " ", &parts);
    if (parts.size() < 4) {
        std::cout << "error format input" << std::endl;
        return;
    }
    client->Scan(parts[1], parts[2], parts[3]);
}

void HandleBench(const std::string& input, rtidb::RtiDBClient* client) {
    std::vector<std::string> parts;
    ::rtidb::SplitString(input, " ", &parts);
    char val[400];
    for (int i = 0; i < 400; i++) {
        val[i] ='0';
    }
    std::string sval(val);
    for (int i = 0; i < 10000000; i++) {
        std::string key = "00000000";
        std::string num = boost::lexical_cast<std::string>(i);
        key.replace(key.size()-num.size(), num.size(), num); 
        client->Put(parts[1], key, sval);
    }
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    rtidb::RtiDBClient* client = new rtidb::RtiDBClient(FLAGS_ts_endpoint);
    client->Init();
    std::cout << "welcome to rtidb cli!" << std::endl;
    while (!s_quit) {
        std::cout << ">";
        std::string buffer;
        std::getline(std::cin, buffer);
        if (buffer.empty()) {
            continue;
        }
        std::string cmd = buffer.substr(0, 3);
        if (cmd == "put") {
            HandlePut(buffer, client);
        }else if (cmd =="ben"){
            HandleBench(buffer, client);
        }else {
            HandleScan(buffer, client);
        }
    }


}
