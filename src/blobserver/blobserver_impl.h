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

#include "base/spinlock.h"
#include "proto/blob_server.pb.h"
#include "storage/object_store.h"
#include "thread_pool.h"  // NOLINT
#include "zk/zk_client.h"

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
                     CreateTableResponse* response, Closure* done);
    void Put(RpcController* controller, const PutRequest* request,
             PutResponse* response, Closure* done);
    void Get(RpcController* controller, const GetRequest* request,
             GetResponse* response, Closure* done);
    void Delete(RpcController* controller, const DeleteRequest* request,
                DeleteResponse* response, Closure* done);
    void LoadTable(RpcController* controller, const LoadTableRequest* request,
                   LoadTableResponse* response, Closure* done);
    void GetStoreStatus(RpcController* controller,
                        const GetStoreStatusRequest* request,
                        GetStoreStatusResponse* response, Closure* done);
    void DropTable(RpcController* controller, const DropTableRequest* request,
                   DropTableResponse* response, Closure* done);

 private:
    int CreateTable(const TableMeta& meta);

    int WriteTableMeta(const std::string& path, const TableMeta& meta);

    void DropTableInternal(uint32_t tid, uint32_t pid);
    SpinMutex spin_mutex_;
    ZkClient* zk_client_;
    brpc::Server* server_;
    ThreadPool keep_alive_pool_;
    ThreadPool task_pool_;
    std::atomic<bool> follower_;
    ObjectStores object_stores_;
};

}  // namespace blobserver
}  // namespace rtidb
