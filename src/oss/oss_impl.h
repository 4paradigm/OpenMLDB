//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-016

#pragma once

#include <brpc/server.h>
#include <mutex>
#include "proto/object_storage.pb.h"
#include "zk/zk_client.h"

namespace rtidb {
namespace oss {

using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;
using ::rtidb::zk::ZkClient;
using ::rtidb::ObjectStorage::CreateTableRequest;
using ::rtidb::ObjectStorage::GeneralResponse;
using ::rtidb::ObjectStorage::PutRequest;
using ::rtidb::ObjectStorage::GeneralRequest;
using ::rtidb::ObjectStorage::GetResponse;
using ::rtidb::ObjectStorage::StatsRequest;
using ::rtidb::ObjectStorage::StatsResponse;


class OSSImpl: public ::rtidb::ObjectStorage::OSS {
public:
   OSSImpl();

   ~OSSImpl();

   bool Init();

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
};

}
}
