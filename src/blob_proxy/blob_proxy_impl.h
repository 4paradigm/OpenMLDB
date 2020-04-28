//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-08

#pragma once

#include <brpc/server.h>
#include <client/client.h>

#include <mutex>

#include "proto/blob_proxy.pb.h"
#include "zk/zk_client.h"

using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;
using ::rtidb::zk::ZkClient;

namespace rtidb {
namespace blobproxy {

class BlobProxyImpl : public ::rtidb::blobproxy::BlobProxy {
 public:
    BlobProxyImpl();

    ~BlobProxyImpl();

    bool Init();

    void Get(RpcController* controller, const HttpRequest* request,
             HttpResponse* response, Closure* done);

    inline void SetServer(brpc::Server* server) { server_ = server; }

 private:
    std::mutex mu_;
    ZkClient* zk_client_;
    brpc::Server* server_;
    BaseClient* client_;
};
}  // namespace blobproxy
}  // namespace rtidb
