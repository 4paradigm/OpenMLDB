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
#include "thread_pool.h"
#include "tablet/tablet_metric.h"
#include "tablet/table_ha.h"
#include <sofa/pbrpc/pbrpc.h>

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::baidu::common::Mutex;
using ::baidu::common::MutexLock;
using ::baidu::common::ThreadPool;

namespace rtidb {
namespace tablet {

class TabletImpl : public ::rtidb::api::TabletServer {

public:
    TabletImpl();
    ~TabletImpl();
    void Init();
    void Put(RpcController* controller,
             const ::rtidb::api::PutRequest* request,
             ::rtidb::api::PutResponse* response,
             Closure* done);

    void Scan(RpcController* controller,
              const ::rtidb::api::ScanRequest* request,
              ::rtidb::api::ScanResponse* response,
              Closure* done);

    void CreateTable(RpcController* controller,
            const ::rtidb::api::CreateTableRequest* request,
            ::rtidb::api::CreateTableResponse* response,
            Closure* done);

    void DropTable(RpcController* controller,
            const ::rtidb::api::DropTableRequest* request,
            ::rtidb::api::DropTableResponse* response,
            Closure* done);

    void RelMem(RpcController* controller,
            const ::rtidb::api::RelMemRequest* request,
            ::rtidb::api::RelMemResponse* response,
            Closure* done);
    //
    //http api
    // get all table informatiom
    // 
    bool WebService(const sofa::pbrpc::HTTPRequest& request,
            sofa::pbrpc::HTTPResponse& response);

private:
    // Get table by table id and Inc reference
    ::rtidb::storage::Table* GetTable(uint32_t tid);
    
    TableDataHA* GetTableHa(uint32_t tid);

    void GcTable(uint32_t tid);

    void ShowTables(const sofa::pbrpc::HTTPRequest& request,
            sofa::pbrpc::HTTPResponse& response); 

    void ShowMetric(const sofa::pbrpc::HTTPRequest& request,
            sofa::pbrpc::HTTPResponse& response);

    inline bool CheckScanRequest(const rtidb::api::ScanRequest* request);
    
    inline bool CheckCreateRequest(const rtidb::api::CreateTableRequest* request);

private:
    std::map<uint32_t, ::rtidb::storage::Table*> tables_;
    Mutex mu_;
    ThreadPool gc_pool_;
    TabletMetric* metric_;
    std::map<uint32_t, ::rtidb::tablet::TableDataHA*> table_has_;
};


}
}


#endif /* !TABLET_IMPL_H */
