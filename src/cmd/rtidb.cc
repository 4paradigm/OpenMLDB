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

#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>
#include "logging.h"

#include "tablet/tablet_impl.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DEFINE_string(endpoint, "127.0.0.1:9527", "Config the ip and port that rtidb serves for");
DEFINE_string(role, "tablet | master | client", "Set the rtidb role for start");

static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/){
    s_quit = true;
}

void StartTablet() {
    //TODO(wangtaize) optimalize options
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

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    StartTablet();
    return 0;
}
