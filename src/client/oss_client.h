//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-16

#include <rpc/rpc_client.h>
#include "proto/object_storage.pb.h"

namespace rtidb {
namespace client {

using ::rtidb::ObjectStorage::OssTableMeta;
using ::rtidb::ObjectStorage::OSS_Stub;

class OssClient {
public:
   OssClient(const std::string& endpoint);

   OssClient(const std::string& endpoint, bool use_sleep_policy);

   int Init();

   std::string GetEndpoint();

   bool CreateTable(const OssTableMeta& table_meta, std::string* msg);

   bool Put(uint32_t tid, uint32_t pid, const std::string& key, const std::string& value, std::string* msg);

   bool Get(uint32_t tid, uint32_t pid, const std::string& key, std::string* value, std::string* msg);

   bool Delete(uint32_t tid, uint32_t pid, const std::string& key, std::string* msg);

   bool Stats(uint32_t tid, uint32_t pid, uint64_t* count, uint64_t* total_space, uint64_t* avail_space);
private:
   std::string endpoint_;
   ::rtidb::RpcClient<OSS_Stub> client_;
};

}
}