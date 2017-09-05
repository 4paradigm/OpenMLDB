//
// name_server.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05
//

#include "name_server_impl.h"

namespace rtidb {
namespace server {

NameServerImpl::NameServerImpl() {

}

NameServerImpl::~NameServerImpl() {

}

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
