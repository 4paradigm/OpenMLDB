//
// tablet_impl.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-01 
// 


#ifndef RTIDB_TABLET_IMPL_H
#define RTIDB_TABLET_IMPL_H

#include "mutex.h"
#include "proto/tablet.pb.h"
#include "replica/log_replicator.h"
#include "storage/snapshot.h"
#include "storage/table.h"
#include "tablet/tablet_metric.h"
#include "thread_pool.h"
#include "zk/zk_client.h"
#include <map>
#include <list>
#include <sofa/pbrpc/pbrpc.h>

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::baidu::common::Mutex;
using ::baidu::common::MutexLock;
using ::baidu::common::ThreadPool;
using ::rtidb::storage::Table;
using ::rtidb::storage::Snapshot;
using ::rtidb::replica::LogReplicator;
using ::rtidb::replica::ReplicatorRole;
using ::rtidb::zk::ZkClient;

namespace rtidb {
namespace tablet {

typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<Table> > > Tables;
typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<LogReplicator> > > Replicators;
typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<Snapshot> > > Snapshots;

class TabletImpl : public ::rtidb::api::TabletServer {

public:
    TabletImpl();

    ~TabletImpl();

    bool Init();

    void Put(RpcController* controller,
             const ::rtidb::api::PutRequest* request,
             ::rtidb::api::PutResponse* response,
             Closure* done);

    void Scan(RpcController* controller,
              const ::rtidb::api::ScanRequest* request,
              ::rtidb::api::ScanResponse* response,
              Closure* done);

    void BatchGet(RpcController* controller,
              const ::rtidb::api::BatchGetRequest* request,
              ::rtidb::api::BatchGetResponse* response,
              Closure* done);

    void CreateTable(RpcController* controller,
            const ::rtidb::api::CreateTableRequest* request,
            ::rtidb::api::CreateTableResponse* response,
            Closure* done);

    void LoadTable(RpcController* controller,
            const ::rtidb::api::LoadTableRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void DropTable(RpcController* controller,
            const ::rtidb::api::DropTableRequest* request,
            ::rtidb::api::DropTableResponse* response,
            Closure* done);

    void AddReplica(RpcController* controller, 
            const ::rtidb::api::ReplicaRequest* request,
            ::rtidb::api::AddReplicaResponse* response,
            Closure* done);

    void DelReplica(RpcController* controller, 
            const ::rtidb::api::ReplicaRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void AppendEntries(RpcController* controller,
            const ::rtidb::api::AppendEntriesRequest* request,
            ::rtidb::api::AppendEntriesResponse* response,
            Closure* done); 

    void GetTableStatus(RpcController* controller,
            const ::rtidb::api::GetTableStatusRequest* request,
            ::rtidb::api::GetTableStatusResponse* response,
            Closure* done);

    void ChangeRole(RpcController* controller,
            const ::rtidb::api::ChangeRoleRequest* request,
            ::rtidb::api::ChangeRoleResponse* response,
            Closure* done);

    void MakeSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);
           
    void PauseSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void RecoverSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void GetTaskStatus(RpcController* controller,
            const ::rtidb::api::TaskStatusRequest* request,
            ::rtidb::api::TaskStatusResponse* response,
            Closure* done);

    void DeleteOPTask(RpcController* controller,
            const ::rtidb::api::DeleteTaskRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);
    //
    //http api
    // get all table informatiom
    // 
    bool WebService(const sofa::pbrpc::HTTPRequest& request,
            sofa::pbrpc::HTTPResponse& response);
private:
    // Get table by table id , no need external synchronization
    std::shared_ptr<Table> GetTable(uint32_t tid, uint32_t pid);
    // Get table by table id , and Need external synchronization  
    std::shared_ptr<Table> GetTableUnLock(uint32_t tid, uint32_t pid);

    std::shared_ptr<LogReplicator> GetReplicator(uint32_t tid, uint32_t pid);
    std::shared_ptr<LogReplicator> GetReplicatorUnLock(uint32_t tid, uint32_t pid);
    std::shared_ptr<Snapshot> GetSnapshot(uint32_t tid, uint32_t pid);
    std::shared_ptr<Snapshot> GetSnapshotUnLock(uint32_t tid, uint32_t pid);
    void GcTable(uint32_t tid, uint32_t pid);

    void ShowTables(const sofa::pbrpc::HTTPRequest& request,
            sofa::pbrpc::HTTPResponse& response); 

    void ShowMetric(const sofa::pbrpc::HTTPRequest& request,
            sofa::pbrpc::HTTPResponse& response);

    void ShowMemPool(const sofa::pbrpc::HTTPRequest& request,
        sofa::pbrpc::HTTPResponse& response);

    inline bool CheckScanRequest(const rtidb::api::ScanRequest* request);

    inline bool CheckTableMeta(const rtidb::api::TableMeta* table_meta);

    int CreateTableInternal(const ::rtidb::api::TableMeta* table_meta,
            std::string& msg);

    bool ApplyLogToTable(uint32_t tid, uint32_t pid, const ::rtidb::api::LogEntry& log); 

    void MakeSnapshotInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task);

    void SchedMakeSnapshot();

    int ChangeToLeader(uint32_t tid, uint32_t pid, 
                       const std::vector<std::string>& replicas);

    void CheckZkClient();

    int32_t DeleteTableInternal(uint32_t tid, uint32_t pid);

    int WriteTableMeta(const std::string& path, const ::rtidb::api::TableMeta* table_meta);

    int UpdateTableMeta(const std::string& path, ::rtidb::api::TableMeta* table_meta);

    void AddTask(std::shared_ptr<::rtidb::api::TaskInfo> task);

    std::shared_ptr<::rtidb::api::TaskInfo> FindTask(
            uint64_t op_id, ::rtidb::api::TaskType task_type);

private:
    Tables tables_;
    Mutex mu_;
    ThreadPool gc_pool_;
    TabletMetric* metric_;
    Replicators replicators_;
    Snapshots snapshots_;
    ZkClient* zk_client_;
    ThreadPool keep_alive_pool_;
    ThreadPool task_pool_;
    std::list<std::shared_ptr<::rtidb::api::TaskInfo>> task_list_;
};


}
}


#endif /* !TABLET_IMPL_H */
