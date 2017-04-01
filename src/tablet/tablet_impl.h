//
// tablet_impl.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-01 
// 


#ifndef RTIDB_TABLET_IMPL_H
#define RTIDB_TABLET_IMPL_H

#include <map>
#include "proto/tablet.pb.h"
#include "storage/table.h"
#include "mutex.h"

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::baidu::common::Mutex;
using ::baidu::common::MutexLock;

namespace rtidb {
namespace tablet {

class TabletImpl : public ::rtidb::api::TabletServer {

public:
    TabletImpl();
    ~TabletImpl();

    void Put(RpcController* controller,
             const ::rtidb::api::PutRequest* request,
             ::rtidb::api::PutResponse* response,
             Closure* done);

    void Scan(RpcController* controller,
              const ::rtidb::api::ScanRequest* request,
              ::rtidb::api::ScanResponse* response,
              Closure* done);
private:
    // Get table by table id and Inc reference
    ::rtidb::storage::Table* GetTable(uint32_t tid);
private:
    std::map<uint32_t , ::rtidb::storage::Table*> tables_;
    Mutex mu_;
};


}
}


#endif /* !TABLET_IMPL_H */
