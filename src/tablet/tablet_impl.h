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
#include "replica/log_replicator.h"
#include <sofa/pbrpc/pbrpc.h>

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::baidu::common::Mutex;
using ::baidu::common::MutexLock;
using ::baidu::common::ThreadPool;
using ::rtidb::storage::Table;
using ::rtidb::replica::LogReplicator;
using ::rtidb::replica::ReplicatorRole;

namespace rtidb {
namespace tablet {

typedef std::map<uint32_t, std::map<uint32_t, Table*> > Tables;
typedef std::map<uint32_t, std::map<uint32_t, LogReplicator*> > Replicators;

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

    void AppendEntries(RpcController* controller,
            const ::rtidb::api::AppendEntriesRequest* request,
            ::rtidb::api::AppendEntriesResponse* response,
            Closure* done); 

    //
    //http api
    // get all table informatiom
    // 
    bool WebService(const sofa::pbrpc::HTTPRequest& request,
            sofa::pbrpc::HTTPResponse& response);
private:
    // Get table by table id and Inc reference
    ::rtidb::storage::Table* GetTable(uint32_t tid, uint32_t pid);

    ::rtidb::replica::LogReplicator* GetReplicator(uint32_t tid, uint32_t pid);

    void GcTable(uint32_t tid, uint32_t pid);

    void ShowTables(const sofa::pbrpc::HTTPRequest& request,
            sofa::pbrpc::HTTPResponse& response); 

    void ShowMetric(const sofa::pbrpc::HTTPRequest& request,
            sofa::pbrpc::HTTPResponse& response);

    void ShowMemPool(const sofa::pbrpc::HTTPRequest& request,
        sofa::pbrpc::HTTPResponse& response);

    inline bool CheckScanRequest(const rtidb::api::ScanRequest* request);

    inline bool CheckCreateRequest(const rtidb::api::CreateTableRequest* request);

    void CreateTableInternal(const ::rtidb::api::CreateTableRequest* request,
            ::rtidb::api::CreateTableResponse* response);

    bool ApplyLogToTable(uint32_t tid, uint32_t pid, const ::rtidb::api::LogEntry& log); 

private:
    Tables tables_;
    Mutex mu_;
    ThreadPool gc_pool_;
    TabletMetric* metric_;
    Replicators replicators_;
};


}
}


#endif /* !TABLET_IMPL_H */
