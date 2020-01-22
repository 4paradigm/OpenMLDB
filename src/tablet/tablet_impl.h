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
#include "storage/mem_table_snapshot.h"
#include "storage/disk_table_snapshot.h"
#include "storage/mem_table.h"
#include "storage/disk_table.h"
#include "tablet/file_receiver.h"
#include "thread_pool.h"
#include "base/set.h"
#include "zk/zk_client.h"
#include <map>
#include <list>
#include <brpc/server.h>
#include <mutex>
#include "base/spinlock.h"

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::baidu::common::ThreadPool;
using ::rtidb::storage::Table;
using ::rtidb::storage::MemTable;
using ::rtidb::storage::DiskTable;
using ::rtidb::storage::Snapshot;
using ::rtidb::replica::LogReplicator;
using ::rtidb::replica::ReplicatorRole;
using ::rtidb::zk::ZkClient;
using ::rtidb::base::SpinMutex;

const uint32_t INVALID_REMOTE_TID = UINT32_MAX;

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

    bool RegisterZK();

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

    void Delete(RpcController* controller,
              const ::rtidb::api::DeleteRequest* request,
              ::rtidb::api::GeneralResponse* response,
              Closure* done);

    void Count(RpcController* controller,
              const ::rtidb::api::CountRequest* request,
              ::rtidb::api::CountResponse* response,
              Closure* done);

    void Traverse(RpcController* controller,
              const ::rtidb::api::TraverseRequest* request,
              ::rtidb::api::TraverseResponse* response,
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

    void SetConcurrency(RpcController* ctrl,
            const ::rtidb::api::SetConcurrencyRequest* request,
            ::rtidb::api::SetConcurrencyResponse* response,
            Closure* done);

    void DelReplica(RpcController* controller, 
            const ::rtidb::api::ReplicaRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void AppendEntries(RpcController* controller,
            const ::rtidb::api::AppendEntriesRequest* request,
            ::rtidb::api::AppendEntriesResponse* response,
            Closure* done); 

    void UpdateTableMetaForAddField(RpcController* controller,
            const ::rtidb::api::UpdateTableMetaForAddFieldRequest* request,
            ::rtidb::api::GeneralResponse* response,
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

    void SendData(RpcController* controller,
            const ::rtidb::api::SendDataRequest* request,
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
    
    void UpdateTTL(RpcController* controller, 
            const ::rtidb::api::UpdateTTLRequest* request,
            ::rtidb::api::UpdateTTLResponse* response,
            Closure* done);

    void ExecuteGc(RpcController* controller, 
            const ::rtidb::api::ExecuteGcRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void ShowMemPool(RpcController* controller,
            const ::rtidb::api::HttpRequest* request,
            ::rtidb::api::HttpResponse* response,
            Closure* done);

    void GetAllSnapshotOffset(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::SnapOffsetResponse* response,
            Closure* done);

    void GetTermPair(RpcController* controller,
            const ::rtidb::api::GetTermPairRequest* request,
            ::rtidb::api::GetTermPairResponse* response,
            Closure* done);

    void GetTableFollower(RpcController* controller,
            const ::rtidb::api::GetTableFollowerRequest* request,
            ::rtidb::api::GetTableFollowerResponse* response,
            Closure* done);

    void GetManifest(RpcController* controller,
            const ::rtidb::api::GetManifestRequest* request,
            ::rtidb::api::GetManifestResponse* response,
            Closure* done);

    void ConnectZK(RpcController* controller,
            const ::rtidb::api::ConnectZKRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void DisConnectZK(RpcController* controller,
            const ::rtidb::api::DisConnectZKRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void DeleteBinlog(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void CheckFile(RpcController* controller,
            const ::rtidb::api::CheckFileRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void SetMode(RpcController* controller,
            const ::rtidb::api::SetModeRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    void AlignTable(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

    inline void SetServer(brpc::Server* server) {
        server_ = server;
    }

    // get on value from specified ttl type index
    int32_t GetIndex(uint64_t expire_time, uint64_t expire_cnt,
                          ::rtidb::api::TTLType ttl_type,
                          ::rtidb::storage::TableIterator* it,
                          const ::rtidb::api::GetRequest* request,
                          std::string* value,
                          uint64_t* ts);

    // scan specified ttl type index
    int32_t ScanIndex(uint64_t expire_time, uint64_t expire_cnt,
                          ::rtidb::api::TTLType ttl_type,
                          ::rtidb::storage::TableIterator* it,
                          const ::rtidb::api::ScanRequest* request,
                          std::string* pairs,
                          uint32_t* count);

    int32_t CountIndex(uint64_t expire_time, uint64_t expire_cnt,
                          ::rtidb::api::TTLType ttl_type,
                          ::rtidb::storage::TableIterator* it,
                          const ::rtidb::api::CountRequest* request,
                          uint32_t* count);
private:

    bool CreateMultiDir(const std::vector<std::string>& dirs);
    // Get table by table id , no need external synchronization
    std::shared_ptr<Table> GetTable(uint32_t tid, uint32_t pid);
    // Get table by table id , and Need external synchronization  
    std::shared_ptr<Table> GetTableUnLock(uint32_t tid, uint32_t pid);
    //std::shared_ptr<DiskTable> GetDiskTable(uint32_t tid, uint32_t pid);
    //std::shared_ptr<DiskTable> GetDiskTableUnLock(uint32_t tid, uint32_t pid);

    std::shared_ptr<LogReplicator> GetReplicator(uint32_t tid, uint32_t pid);
    std::shared_ptr<LogReplicator> GetReplicatorUnLock(uint32_t tid, uint32_t pid);
    std::shared_ptr<Snapshot> GetSnapshot(uint32_t tid, uint32_t pid);
    std::shared_ptr<Snapshot> GetSnapshotUnLock(uint32_t tid, uint32_t pid);
    void GcTable(uint32_t tid, uint32_t pid, bool execute_once);

    int CheckTableMeta(const rtidb::api::TableMeta* table_meta, std::string& msg);

    int CreateTableInternal(const ::rtidb::api::TableMeta* table_meta, std::string& msg);

    int CreateDiskTableInternal(const ::rtidb::api::TableMeta* table_meta, bool is_load, std::string& msg);

    void MakeSnapshotInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task);

    void SendSnapshotInternal(const std::string& endpoint, uint32_t tid, uint32_t pid,
                        uint32_t remote_tid, std::shared_ptr<::rtidb::api::TaskInfo> task);

    void SchedMakeSnapshot();

    void SchedMakeDiskTableSnapshot();

    void GetDiskused();

    void CheckZkClient();

    int32_t DeleteTableInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task_ptr);

    int LoadTableInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task_ptr);
    int LoadDiskTableInternal(uint32_t tid, uint32_t pid, const ::rtidb::api::TableMeta& table_meta,
                std::shared_ptr<::rtidb::api::TaskInfo> task_ptr);

    int WriteTableMeta(const std::string& path, const ::rtidb::api::TableMeta* table_meta);

    int UpdateTableMeta(const std::string& path, ::rtidb::api::TableMeta* table_meta, bool for_add_column);

    int UpdateTableMeta(const std::string& path, ::rtidb::api::TableMeta* table_meta);

    int AddOPTask(const ::rtidb::api::TaskInfo& task_info, ::rtidb::api::TaskType task_type,
            std::shared_ptr<::rtidb::api::TaskInfo>& task_ptr);

    std::shared_ptr<::rtidb::api::TaskInfo> FindTask(
            uint64_t op_id, ::rtidb::api::TaskType task_type);

    int AddOPMultiTask(const ::rtidb::api::TaskInfo& task_info, ::rtidb::api::TaskType task_type,
            std::shared_ptr<::rtidb::api::TaskInfo>& task_ptr);

    std::shared_ptr<::rtidb::api::TaskInfo> FindMultiTask(const ::rtidb::api::TaskInfo& task_info); 

   int CheckDimessionPut(const ::rtidb::api::PutRequest* request, uint32_t idx_cnt);
    
    // sync log data from page cache to disk 
    void SchedSyncDisk(uint32_t tid, uint32_t pid);

    // sched replicator to delete binlog
    void SchedDelBinlog(uint32_t tid, uint32_t pid);

    bool CheckGetDone(::rtidb::api::GetType type, 
                      uint64_t ts, uint64_t target_ts); 

    bool ChooseDBRootPath(uint32_t tid, uint32_t pid,
            const ::rtidb::common::StorageMode& mode,
            std::string& path);

    bool ChooseRecycleBinRootPath(uint32_t tid, uint32_t pid,
            const ::rtidb::common::StorageMode& mode,
            std::string& path);

    bool ChooseTableRootPath(uint32_t tid, uint32_t pid,
            const ::rtidb::common::StorageMode& mode,
            std::string& path);

    bool GetTableRootSize(uint32_t tid, uint32_t pid, const
            ::rtidb::common::StorageMode& mode, uint64_t& size);

    int32_t GetSnapshotOffset(uint32_t tid, uint32_t pid,
              common::StorageMode& sm, std::string& msg,
              uint64_t& term, uint64_t& offset);

    bool SeekWithCount(::rtidb::storage::TableIterator* it, const uint64_t time,
            const ::rtidb::api::GetType& type, uint32_t max_cnt, uint32_t& cnt);

    bool Seek(::rtidb::storage::TableIterator* it, const uint64_t time,
            const ::rtidb::api::GetType& type);

    void DelRecycle(const std::string &path);
    
    void SchedDelRecycle();

private:
    Tables tables_;
    std::mutex mu_;
    SpinMutex spin_mutex_;
    ThreadPool gc_pool_;
    Replicators replicators_;
    Snapshots snapshots_;
    ZkClient* zk_client_;
    ThreadPool keep_alive_pool_;
    ThreadPool task_pool_;
    ThreadPool io_pool_;
    ThreadPool snapshot_pool_;
    std::map<uint64_t, std::list<std::shared_ptr<::rtidb::api::TaskInfo>>> task_map_;
    std::set<std::string> sync_snapshot_set_;
    std::map<std::string, std::shared_ptr<FileReceiver>> file_receiver_map_;
    brpc::Server* server_;
    std::map<::rtidb::common::StorageMode, std::vector<std::string>> mode_root_paths_;
    std::map<::rtidb::common::StorageMode, std::vector<std::string>> mode_recycle_root_paths_;
    std::atomic<bool> follower_;
};

}
}


#endif /* !TABLET_IMPL_H */
