//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-016

#pragma once

#include <brpc/server.h>
#include <storage/object_store.h>
#include <map>
#include <mutex>
#include <string>
#include <memory>
#include "proto/blob_server.pb.h"
#include "thread_pool.h"
#include "zk/zk_client.h"

namespace rtidb {
namespace blobserver {

using ::baidu::common::ThreadPool;
using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;
using ::rtidb::zk::ZkClient;
using ::rtidb::storage::ObjectStore;

typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<ObjectStore>>> ObjectStores;

class BlobServerImpl : public ::rtidb::blobserver::BlobServer {
 public:
    BlobServerImpl();

    ~BlobServerImpl();

    bool Init();

    void CheckZkClient();

    void CreateTable(RpcController* controller,
                     const CreateTableRequest* request,
                     GeneralResponse* response, Closure* done);
    void Put(RpcController* controller, const PutRequest* request,
             GeneralResponse* response, Closure* done);
    void Get(RpcController* controller, const GeneralRequest* request,
             GetResponse* response, Closure* done);
    void Delete(RpcController* controller, const GeneralRequest* request,
                GeneralResponse* response, Closure* done);
    void Stats(RpcController* controller, const StatsRequest* request,
               StatsResponse* response, Closure* done);

    bool RegisterZK();

    inline void SetServer(brpc::Server* server) { server_ = server; }

 private:
    std::mutex mu_;
    ZkClient* zk_client_;
    brpc::Server* server_;
    ThreadPool keep_alive_pool_;
    std::atomic<bool> follower_;
    ObjectStores object_stores_;
};

}  // namespace oss
}  // namespace rtidb
