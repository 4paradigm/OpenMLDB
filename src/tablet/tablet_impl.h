//
// tablet_impl.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-01 
// 


#ifndef RTIDB_TABLET_IMPL_H
#define RTIDB_TABLET_IMPL_H

#include "proto/tablet.pb.h"
#include "replica/log_replicator.h"
#include "storage/snapshot.h"
#include "storage/table.h"
#include "thread_pool.h"
#include "base/set.h"
#include "zk/zk_client.h"
#include <map>
#include <list>
#include <brpc/server.h>
#include <brpc/stream.h>
#include <mutex>

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
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

class StreamReceiver : public brpc::StreamInputHandler {
public:
	StreamReceiver(const std::string& file_name, uint32_t tid, uint32_t pid);
	virtual ~StreamReceiver();
    int Init();
    virtual int on_received_messages(brpc::StreamId id,
                    butil::IOBuf *const messages[],
                    size_t size) override;
    virtual void on_idle_timeout(brpc::StreamId id) override;
    virtual void on_closed(brpc::StreamId id) override;
private:
	std::string file_name_;
    uint32_t tid_;
    uint32_t pid_;
    uint64_t size_;
	FILE* file_;
	static ::rtidb::base::set<std::string> stream_receiver_set_;
};

class TabletImpl : public ::rtidb::api::TabletServer {

public:
    TabletImpl();

    ~TabletImpl();

    bool Init();

    void Put(RpcController* controller,
             const ::rtidb::api::PutRequest* request,
             ::rtidb::api::PutResponse* response,
             Closure* done);

    void Get(RpcController* controller,
             const ::rtidb::api::GetRequest* request,
             ::rtidb::api::GetResponse* response,
             Closure* done);

    void Scan(RpcController* controller,
              const ::rtidb::api::ScanRequest* request,
              ::rtidb::api::ScanResponse* response,
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

    void SendSnapshot(RpcController* controller,
            const ::rtidb::api::SendSnapshotRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void CreateStream(RpcController* controller,
            const ::rtidb::api::CreateStreamRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void GetTaskStatus(RpcController* controller,
            const ::rtidb::api::TaskStatusRequest* request,
            ::rtidb::api::TaskStatusResponse* response,
            Closure* done);

    void GetTableSchema(RpcController* controller,
            const ::rtidb::api::GetTableSchemaRequest* request,
            ::rtidb::api::GetTableSchemaResponse* response,
            Closure* done);

    void DeleteOPTask(RpcController* controller,
            const ::rtidb::api::DeleteTaskRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void SetExpire(RpcController* controller,
            const ::rtidb::api::SetExpireRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void SetTTLClock(RpcController* controller,
            const ::rtidb::api::SetTTLClockRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);
    
    void ShowMemPool(RpcController* controller,
            const ::rtidb::api::HttpRequest* request,
            ::rtidb::api::HttpResponse* response,
            Closure* done);

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

    inline bool CheckScanRequest(const rtidb::api::ScanRequest* request);

    inline bool CheckTableMeta(const rtidb::api::TableMeta* table_meta);

    int CreateTableInternal(const ::rtidb::api::TableMeta* table_meta, std::string& msg);

    void MakeSnapshotInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task);

    void SendSnapshotInternal(const std::string& endpoint, uint32_t tid, uint32_t pid,
                        std::shared_ptr<::rtidb::api::TaskInfo> task);

    int SendFile(const std::string& endpoint, uint32_t tid, uint32_t pid, const std::string& file_name);

    int StreamWrite(brpc::StreamId stream, char* buffer, size_t len, uint64_t limit_time);

    void SchedMakeSnapshot();

    int ChangeToLeader(uint32_t tid, uint32_t pid, 
                       const std::vector<std::string>& replicas, uint64_t term);

    void CheckZkClient();

    int32_t DeleteTableInternal(uint32_t tid, uint32_t pid);

    int WriteTableMeta(const std::string& path, const ::rtidb::api::TableMeta* table_meta);

    int UpdateTableMeta(const std::string& path, ::rtidb::api::TableMeta* table_meta);

    int AddOPTask(const ::rtidb::api::TaskInfo& task_info, ::rtidb::api::TaskType task_type,
                    std::shared_ptr<::rtidb::api::TaskInfo>& task_ptr);

    std::shared_ptr<::rtidb::api::TaskInfo> FindTask(
            uint64_t op_id, ::rtidb::api::TaskType task_type);

    int32_t CheckDimessionPut(const ::rtidb::api::PutRequest* request,
                              std::shared_ptr<Table>& table);

private:
    Tables tables_;
    std::mutex mu_;
    ThreadPool gc_pool_;
    Replicators replicators_;
    Snapshots snapshots_;
    ZkClient* zk_client_;
    ThreadPool keep_alive_pool_;
    ThreadPool task_pool_;
    std::map<uint64_t, std::list<std::shared_ptr<::rtidb::api::TaskInfo>>> task_map_;
    std::set<std::string> sync_snapshot_set_;
};


}
}


#endif /* !TABLET_IMPL_H */
