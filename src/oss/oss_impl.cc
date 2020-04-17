//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-16

#include "oss/oss_impl.h"


namespace rtidb {
namespace oss {

OSSImpl::OSSImpl(): mu_(), zk_client_(NULL), server_(NULL)  {
}

OSSImpl::~OSSImpl() {
    if (zk_client_ != NULL) {
        delete zk_client_;
    }
}

bool OSSImpl::Init() {
    std::lock_guard<std::mutex> lock(mu_);
    return true;
}

bool OSSImpl::RegisterZK() {
    return true;
}

void OSSImpl::CreateTable(RpcController *controller,
                          const CreateTableRequest *request,
                          GeneralResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
}

void OSSImpl::Get(RpcController *controller, const GeneralRequest*request,
                  GetResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
}

void OSSImpl::Put(RpcController *controller, const PutRequest *request,
                  GeneralResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
}

void OSSImpl::Delete(RpcController *controller, const GeneralRequest *request,
                     GeneralResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
}

void OSSImpl::Stats(RpcController *controller, const StatsRequest *request,
                    StatsResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
}

}
}

