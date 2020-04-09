//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-08

#pragma once

#include <brpc/server.h>
#include <client/client.h>
#include <mutex>
#include "proto/http_server.pb.h"
#include "zk/zk_client.h"

using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;
using ::rtidb::zk::ZkClient;

namespace rtidb {
namespace http {

class HttpImpl : public ::rtidb::httpserver::HTTPServer {
 public:
    HttpImpl();

    ~HttpImpl();

    bool Init();

    bool RegisterZk();

    void Get(RpcController* controller,
                    const ::rtidb::httpserver::HttpRequest* request,
                    ::rtidb::httpserver::HttpResponse* response, Closure* done);

    inline void SetServer(brpc::Server* server) { server_ = server; }

 private:
    std::mutex mu_;
    ZkClient* zk_client_;
    brpc::Server* server_;
    BaseClient* client_;
};
}  // namespace http
}  // namespace rtidb
