//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-16

#ifndef SRC_CLIENT_BS_CLIENT_H_
#define SRC_CLIENT_BS_CLIENT_H_

#include <rpc/rpc_client.h>

#include <string>

#include "proto/blob_server.pb.h"

namespace rtidb {
namespace client {

using ::rtidb::blobserver::BlobServer_Stub;
using ::rtidb::blobserver::TableMeta;

class BsClient {
 public:
    explicit BsClient(bool use_rdma, const std::string& endpoint, const std::string& real_endpoint);

    BsClient(bool use_rdma, const std::string& endpoint, const std::string& real_endpoint, bool use_sleep_policy);

    int Init();

    std::string GetEndpoint();

    bool CreateTable(const TableMeta& table_meta, std::string* msg);

    bool LoadTable(uint32_t tid, uint32_t pid, std::string *msg);

    bool Put(uint32_t tid, uint32_t pid, const std::string& value,
             int64_t* key, std::string* msg);

    bool Put(uint32_t tid, uint32_t pid, char* value, int64_t len,
             int64_t* key, std::string* msg);

    bool Put(uint32_t tid, uint32_t pid, int64_t key,
             const std::string& value, std::string* msg);

    bool Get(uint32_t tid, uint32_t pid, int64_t key,
             std::string* value, std::string* msg);

    bool Get(uint32_t tid, uint32_t pid, int64_t key,
             std::string* msg, butil::IOBuf* buff);

    bool Delete(uint32_t tid, uint32_t pid, int64_t key,
                std::string* msg);

    bool DropTable(uint32_t tid, uint32_t pid, std::string* msg);

    bool GetStoreStatus(::rtidb::blobserver::GetStoreStatusResponse* response);

    bool GetStoreStatus(uint32_t tid, uint32_t pid,
                        ::rtidb::blobserver::StoreStatus* status);

 private:
    std::string endpoint_;
    ::rtidb::RpcClient<BlobServer_Stub> client_;
};

}  // namespace client
}  // namespace rtidb

#endif  // SRC_CLIENT_BS_CLIENT_H_
