//
// name_server.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05
//

#include "name_server_impl.h"

#include "gflags/gflags.h"

DECLARE_int32(zk_session_timeout);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_keep_alive_check_interval);

namespace rtidb {
namespace nameserver {

NameServerImpl::NameServerImpl():mu_(), tablet_client_(),
    zk_client_(NULL), dist_lock(NULL){}

NameServerImpl::~NameServerImpl() {}

int NameServerImpl::Init() {
    return 0;
}

bool NameServerImpl::WebService(const sofa::pbrpc::HTTPRequest& request,
        sofa::pbrpc::HTTPResponse& response) {
    return true;
}

std::string NameServerImpl::GetMaster() {
    // to do. get master point form zk

    std::string endpoint("");
    return endpoint;
}

void NameServerImpl::CreateTable(RpcController* controller, 
        const CreateTableRequest* request, 
        GeneralResponse* response, 
        Closure* done) {
}

}
}
