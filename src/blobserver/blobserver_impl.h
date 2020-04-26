//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-016

#pragma once

#include <brpc/server.h>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "storage/object_store.h"
#include "zk/zk_client.h"
#include "base/spinlock.h"
#include "proto/blob_server.pb.h"
#include "thread_pool.h"  // NOLINT

namespace rtidb {
namespace blobserver {

using ::baidu::common::ThreadPool;
using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;
using ::rtidb::base::SpinMutex;
using ::rtidb::storage::ObjectStore;
using ::rtidb::zk::ZkClient;

typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<ObjectStore>>>
    ObjectStores;

class BlobServerImpl : public ::rtidb::blobserver::BlobServer {
 public:
    BlobServerImpl();

    ~BlobServerImpl();

    bool Init();

    void CheckZkClient();

    bool RegisterZK();

    inline void SetServer(brpc::Server* server) { server_ = server; }

    std::shared_ptr<ObjectStore> GetStore(uint32_t tid, uint32_t pid);

    std::shared_ptr<ObjectStore> GetStoreUnLock(uint32_t tid, uint32_t pid);

    void CreateTable(RpcController* controller,
                     const CreateTableRequest* request,
                     GeneralResponse* response, Closure* done);
    void Put(RpcController* controller, const PutRequest* request,
             PutResponse* response, Closure* done);
    void Get(RpcController* controller, const GeneralRequest* request,
             GetResponse* response, Closure* done);
    void Delete(RpcController* controller, const GeneralRequest* request,
                GeneralResponse* response, Closure* done);
    void Stats(RpcController* controller, const StatsRequest* request,
               StatsResponse* response, Closure* done);
    void LoadTable(RpcController* controller, const LoadTableRequest* request,
                   GeneralResponse* response, Closure* done);
    void GetStoreStatus(RpcController* controller,
                        const GetStoreStatusRequest* request,
                        GetStoreStatusResponse* response, Closure* done);

 private:
    SpinMutex spin_mutex_;
    ZkClient* zk_client_;
    brpc::Server* server_;
    ThreadPool keep_alive_pool_;
    std::atomic<bool> follower_;
    ObjectStores object_stores_;
    std::vector<std::string> root_paths_;
};

}  // namespace blobserver
}  // namespace rtidb
