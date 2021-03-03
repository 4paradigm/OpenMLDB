//
// tablet_impl.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize
// Date 2017-04-01
//

#ifndef SRC_TABLET_TABLET_IMPL_H_
#define SRC_TABLET_TABLET_IMPL_H_

#include <brpc/server.h>
#include <utility>
#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <string>
#include <vector>

#include "base/set.h"
#include "base/spinlock.h"
#include "catalog/tablet_catalog.h"
#include "proto/tablet.pb.h"
#include "replica/log_replicator.h"
#include "storage/disk_table.h"
#include "storage/disk_table_snapshot.h"
#include "storage/mem_table.h"
#include "storage/mem_table_snapshot.h"
#include "tablet/combine_iterator.h"
#include "tablet/file_receiver.h"
#include "thread_pool.h"  // NOLINT
#include "vm/engine.h"
#include "zk/zk_client.h"
#include "catalog/schema_adapter.h"

using ::baidu::common::ThreadPool;
using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;
using ::rtidb::base::SpinMutex;
using ::rtidb::replica::LogReplicator;
using ::rtidb::replica::ReplicatorRole;
using ::rtidb::storage::DiskTable;
using ::rtidb::storage::IndexDef;
using ::rtidb::storage::MemTable;
using ::rtidb::storage::Snapshot;
using ::rtidb::storage::Table;
using ::rtidb::zk::ZkClient;
using Schema = ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>;

const uint32_t INVALID_REMOTE_TID = UINT32_MAX;

namespace rtidb {
namespace tablet {

typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<Table>>> Tables;
typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<LogReplicator>>>
    Replicators;
typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<Snapshot>>>
    Snapshots;

// tablet cache entry for sql procedure
struct SQLProcedureCacheEntry {
    std::shared_ptr<fesql::sdk::ProcedureInfo> procedure_info;
    std::shared_ptr<fesql::vm::CompileInfo> request_info;
    std::shared_ptr<fesql::vm::CompileInfo> batch_request_info;

    SQLProcedureCacheEntry(const std::shared_ptr<fesql::sdk::ProcedureInfo> pinfo,
                           std::shared_ptr<fesql::vm::CompileInfo> rinfo,
                           std::shared_ptr<fesql::vm::CompileInfo> brinfo)
      : procedure_info(pinfo), request_info(rinfo), batch_request_info(brinfo) {}
};
class SpCache : public fesql::vm::CompileInfoCache {
 public:
    SpCache() : db_sp_map_() {}
    ~SpCache() {}
    void InsertSQLProcedureCacheEntry(const std::string& db, const std::string& sp_name,
                                      std::shared_ptr<fesql::sdk::ProcedureInfo> procedure_info,
                                      std::shared_ptr<fesql::vm::CompileInfo> request_info,
                                      std::shared_ptr<fesql::vm::CompileInfo> batch_request_info) {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        auto& sp_map_of_db = db_sp_map_[db];
        sp_map_of_db.insert(
            std::make_pair(sp_name, SQLProcedureCacheEntry(procedure_info, request_info, batch_request_info)));
    }

    void DropSQLProcedureCacheEntry(const std::string& db, const std::string& sp_name) {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        db_sp_map_[db].erase(sp_name);
        return;
    }
    const bool ProcedureExist(const std::string& db, const std::string& sp_name) {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        auto& sp_map_of_db = db_sp_map_[db];
        auto sp_it = sp_map_of_db.find(sp_name);
        return sp_it != sp_map_of_db.end();
    }
    std::shared_ptr<fesql::vm::CompileInfo> GetRequestInfo(const std::string& db, const std::string& sp_name,
                                                           fesql::base::Status& status) override {  // NOLINT
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        auto db_it = db_sp_map_.find(db);
        if (db_it == db_sp_map_.end()) {
            status = fesql::base::Status(fesql::common::kProcedureNotFound,
                                         "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<fesql::vm::CompileInfo>();
        }
        auto sp_it = db_it->second.find(sp_name);
        if (sp_it == db_it->second.end()) {
            status = fesql::base::Status(fesql::common::kProcedureNotFound,
                                         "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<fesql::vm::CompileInfo>();
        }

        if (!sp_it->second.request_info) {
            status = fesql::base::Status(fesql::common::kProcedureNotFound,
                                         "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<fesql::vm::CompileInfo>();
        }
        return sp_it->second.request_info;
    }
    std::shared_ptr<fesql::vm::CompileInfo> GetBatchRequestInfo(const std::string& db, const std::string& sp_name,
                                                                fesql::base::Status& status) override {  // NOLINT
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        auto db_it = db_sp_map_.find(db);
        if (db_it == db_sp_map_.end()) {
            status = fesql::base::Status(fesql::common::kProcedureNotFound,
                                         "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<fesql::vm::CompileInfo>();
        }
        auto sp_it = db_it->second.find(sp_name);
        if (sp_it == db_it->second.end()) {
            status = fesql::base::Status(fesql::common::kProcedureNotFound,
                                         "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<fesql::vm::CompileInfo>();
        }
        if (!sp_it->second.batch_request_info) {
            status = fesql::base::Status(fesql::common::kProcedureNotFound,
                                         "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<fesql::vm::CompileInfo>();
        }
        return sp_it->second.batch_request_info;
    }

 private:
    std::map<std::string, std::map<std::string, SQLProcedureCacheEntry>> db_sp_map_;
    SpinMutex spin_mutex_;
};
class TabletImpl : public ::rtidb::api::TabletServer {
 public:
    TabletImpl();

    ~TabletImpl();

    bool Init(const std::string& real_endpoint);
    bool Init(const std::string& zk_cluster, const std::string& zk_path,
            const std::string& endpoint, const std::string& real_endpoint);

    bool RegisterZK();

    void Put(RpcController* controller, const ::rtidb::api::PutRequest* request,
             ::rtidb::api::PutResponse* response, Closure* done);

    void Get(RpcController* controller, const ::rtidb::api::GetRequest* request,
             ::rtidb::api::GetResponse* response, Closure* done);

    void Scan(RpcController* controller,
              const ::rtidb::api::ScanRequest* request,
              ::rtidb::api::ScanResponse* response, Closure* done);

    void Delete(RpcController* controller,
                const ::rtidb::api::DeleteRequest* request,
                ::rtidb::api::GeneralResponse* response, Closure* done);

    void Count(RpcController* controller,
               const ::rtidb::api::CountRequest* request,
               ::rtidb::api::CountResponse* response, Closure* done);

    void Traverse(RpcController* controller,
                  const ::rtidb::api::TraverseRequest* request,
                  ::rtidb::api::TraverseResponse* response, Closure* done);

    void CreateTable(RpcController* controller,
                     const ::rtidb::api::CreateTableRequest* request,
                     ::rtidb::api::CreateTableResponse* response,
                     Closure* done);

    void LoadTable(RpcController* controller,
                   const ::rtidb::api::LoadTableRequest* request,
                   ::rtidb::api::GeneralResponse* response, Closure* done);

    void DropTable(RpcController* controller,
                   const ::rtidb::api::DropTableRequest* request,
                   ::rtidb::api::DropTableResponse* response, Closure* done);

    void AddReplica(RpcController* controller,
                    const ::rtidb::api::ReplicaRequest* request,
                    ::rtidb::api::AddReplicaResponse* response, Closure* done);

    void SetConcurrency(RpcController* ctrl,
                        const ::rtidb::api::SetConcurrencyRequest* request,
                        ::rtidb::api::SetConcurrencyResponse* response,
                        Closure* done);

    void DelReplica(RpcController* controller,
                    const ::rtidb::api::ReplicaRequest* request,
                    ::rtidb::api::GeneralResponse* response, Closure* done);

    void AppendEntries(RpcController* controller,
                       const ::rtidb::api::AppendEntriesRequest* request,
                       ::rtidb::api::AppendEntriesResponse* response,
                       Closure* done);

    void UpdateTableMetaForAddField(
        RpcController* controller,
        const ::rtidb::api::UpdateTableMetaForAddFieldRequest* request,
        ::rtidb::api::GeneralResponse* response, Closure* done);

    void GetTableStatus(RpcController* controller,
                        const ::rtidb::api::GetTableStatusRequest* request,
                        ::rtidb::api::GetTableStatusResponse* response,
                        Closure* done);

    void ChangeRole(RpcController* controller,
                    const ::rtidb::api::ChangeRoleRequest* request,
                    ::rtidb::api::ChangeRoleResponse* response, Closure* done);

    void MakeSnapshot(RpcController* controller,
                      const ::rtidb::api::GeneralRequest* request,
                      ::rtidb::api::GeneralResponse* response, Closure* done);

    void PauseSnapshot(RpcController* controller,
                       const ::rtidb::api::GeneralRequest* request,
                       ::rtidb::api::GeneralResponse* response, Closure* done);

    void RecoverSnapshot(RpcController* controller,
                         const ::rtidb::api::GeneralRequest* request,
                         ::rtidb::api::GeneralResponse* response,
                         Closure* done);

    void SendSnapshot(RpcController* controller,
                      const ::rtidb::api::SendSnapshotRequest* request,
                      ::rtidb::api::GeneralResponse* response, Closure* done);

    void SendData(RpcController* controller,
                  const ::rtidb::api::SendDataRequest* request,
                  ::rtidb::api::GeneralResponse* response, Closure* done);

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
                      ::rtidb::api::GeneralResponse* response, Closure* done);

    void SetExpire(RpcController* controller,
                   const ::rtidb::api::SetExpireRequest* request,
                   ::rtidb::api::GeneralResponse* response, Closure* done);

    void UpdateTTL(RpcController* controller,
                   const ::rtidb::api::UpdateTTLRequest* request,
                   ::rtidb::api::UpdateTTLResponse* response, Closure* done);

    void ExecuteGc(RpcController* controller,
                   const ::rtidb::api::ExecuteGcRequest* request,
                   ::rtidb::api::GeneralResponse* response, Closure* done);

    void ShowMemPool(RpcController* controller,
                     const ::rtidb::api::HttpRequest* request,
                     ::rtidb::api::HttpResponse* response, Closure* done);

    void GetAllSnapshotOffset(
        RpcController* controller, const ::rtidb::api::EmptyRequest* request,
        ::rtidb::api::TableSnapshotOffsetResponse* response, Closure* done);

    void GetTermPair(RpcController* controller,
                     const ::rtidb::api::GetTermPairRequest* request,
                     ::rtidb::api::GetTermPairResponse* response,
                     Closure* done);

    void GetCatalog(RpcController* controller,
                     const ::rtidb::api::GetCatalogRequest* request,
                     ::rtidb::api::GetCatalogResponse* response,
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
                   ::rtidb::api::GeneralResponse* response, Closure* done);

    void DisConnectZK(RpcController* controller,
                      const ::rtidb::api::DisConnectZKRequest* request,
                      ::rtidb::api::GeneralResponse* response, Closure* done);

    void DeleteBinlog(RpcController* controller,
                      const ::rtidb::api::GeneralRequest* request,
                      ::rtidb::api::GeneralResponse* response, Closure* done);

    void CheckFile(RpcController* controller,
                   const ::rtidb::api::CheckFileRequest* request,
                   ::rtidb::api::GeneralResponse* response, Closure* done);

    void SetMode(RpcController* controller,
                 const ::rtidb::api::SetModeRequest* request,
                 ::rtidb::api::GeneralResponse* response, Closure* done);

    void DeleteIndex(RpcController* controller,
                     const ::rtidb::api::DeleteIndexRequest* request,
                     ::rtidb::api::GeneralResponse* response, Closure* done);

    void DumpIndexData(RpcController* controller,
                       const ::rtidb::api::DumpIndexDataRequest* request,
                       ::rtidb::api::GeneralResponse* response, Closure* done);

    void LoadIndexData(RpcController* controller,
                       const ::rtidb::api::LoadIndexDataRequest* request,
                       ::rtidb::api::GeneralResponse* response, Closure* done);

    void ExtractIndexData(RpcController* controller,
                          const ::rtidb::api::ExtractIndexDataRequest* request,
                          ::rtidb::api::GeneralResponse* response,
                          Closure* done);

    void AddIndex(RpcController* controller,
                  const ::rtidb::api::AddIndexRequest* request,
                  ::rtidb::api::GeneralResponse* response, Closure* done);

    void SendIndexData(RpcController* controller,
                       const ::rtidb::api::SendIndexDataRequest* request,
                       ::rtidb::api::GeneralResponse* response, Closure* done);

    void Query(RpcController* controller,
               const rtidb::api::QueryRequest* request,
               rtidb::api::QueryResponse* response, Closure* done);

    void SubQuery(RpcController* controller,
               const rtidb::api::QueryRequest* request,
               rtidb::api::QueryResponse* response, Closure* done);

    void SQLBatchRequestQuery(RpcController* controller,
                              const rtidb::api::SQLBatchRequestQueryRequest* request,
                              rtidb::api::SQLBatchRequestQueryResponse* response,
                              Closure* done);
    void SubBatchRequestQuery(RpcController* controller,
                              const rtidb::api::SQLBatchRequestQueryRequest* request,
                              rtidb::api::SQLBatchRequestQueryResponse* response,
                              Closure* done);
    void CancelOP(RpcController* controller,
                  const rtidb::api::CancelOPRequest* request,
                  rtidb::api::GeneralResponse* response, Closure* done);

    void UpdateRealEndpointMap(RpcController* controller,
            const rtidb::api::UpdateRealEndpointMapRequest* request,
            rtidb::api::GeneralResponse* response, Closure* done);

    inline void SetServer(brpc::Server* server) { server_ = server; }

    // get on value from specified ttl type index
    int32_t GetIndex(const ::rtidb::api::GetRequest* request,
                     const ::rtidb::api::TableMeta& meta,
                     const std::map<int32_t, std::shared_ptr<Schema>>& vers_schema,
                     CombineIterator* combine_it, std::string* value,
                     uint64_t* ts);

    // scan specified ttl type index
    int32_t ScanIndex(const ::rtidb::api::ScanRequest* request,
                      const ::rtidb::api::TableMeta& meta,
                      const std::map<int32_t, std::shared_ptr<Schema>>& vers_schema,
                      CombineIterator* combine_it, std::string* pairs,
                      uint32_t* count);

    int32_t ScanIndex(const ::rtidb::api::ScanRequest* request,
                      const ::rtidb::api::TableMeta& meta,
                      const std::map<int32_t, std::shared_ptr<Schema>>& vers_schema,
                      CombineIterator* combine_it, butil::IOBuf* buf,
                      uint32_t* count);

    int32_t CountIndex(uint64_t expire_time, uint64_t expire_cnt,
                       ::rtidb::storage::TTLType ttl_type,
                       ::rtidb::storage::TableIterator* it,
                       const ::rtidb::api::CountRequest* request,
                       uint32_t* count);

    std::shared_ptr<Table> GetTable(uint32_t tid, uint32_t pid);

    void CreateProcedure(RpcController* controller,
            const rtidb::api::CreateProcedureRequest* request,
            rtidb::api::GeneralResponse* response, Closure* done);

    void DropProcedure(RpcController* controller,
            const ::rtidb::api::DropProcedureRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done);

 private:
    bool CreateMultiDir(const std::vector<std::string>& dirs);
    // Get table by table id , no need external synchronization
    // Get table by table id , and Need external synchronization
    std::shared_ptr<Table> GetTableUnLock(uint32_t tid, uint32_t pid);
    // std::shared_ptr<DiskTable> GetDiskTable(uint32_t tid, uint32_t pid);
    // std::shared_ptr<DiskTable> GetDiskTableUnLock(uint32_t tid, uint32_t
    // pid);

    std::shared_ptr<LogReplicator> GetReplicator(uint32_t tid, uint32_t pid);

    std::shared_ptr<LogReplicator> GetReplicatorUnLock(uint32_t tid,
                                                       uint32_t pid);
    std::shared_ptr<Snapshot> GetSnapshot(uint32_t tid, uint32_t pid);

    std::shared_ptr<Snapshot> GetSnapshotUnLock(uint32_t tid, uint32_t pid);

    void GcTable(uint32_t tid, uint32_t pid, bool execute_once);

    void GcTableSnapshot(uint32_t tid, uint32_t pid);

    int CheckTableMeta(const rtidb::api::TableMeta* table_meta,
                       std::string& msg);  // NOLINT

    int CreateTableInternal(const ::rtidb::api::TableMeta* table_meta,
                            std::string& msg);  // NOLINT

    int CreateDiskTableInternal(const ::rtidb::api::TableMeta* table_meta,
                                bool is_load, std::string& msg);  // NOLINT

    void MakeSnapshotInternal(uint32_t tid, uint32_t pid, uint64_t end_offset,
                              std::shared_ptr<::rtidb::api::TaskInfo> task);

    void SendSnapshotInternal(const std::string& endpoint, uint32_t tid,
                              uint32_t pid, uint32_t remote_tid,
                              std::shared_ptr<::rtidb::api::TaskInfo> task);

    void DumpIndexDataInternal(
        std::shared_ptr<::rtidb::storage::Table> table,
        std::shared_ptr<::rtidb::storage::MemTableSnapshot> memtable_snapshot,
        uint32_t partition_num,
        ::rtidb::common::ColumnKey& column_key,  // NOLINT
        uint32_t idx, std::shared_ptr<::rtidb::api::TaskInfo> task);

    void SendIndexDataInternal(
        std::shared_ptr<::rtidb::storage::Table> table,
        const std::map<uint32_t, std::string>& pid_endpoint_map,
        std::shared_ptr<::rtidb::api::TaskInfo> task);

    void LoadIndexDataInternal(uint32_t tid, uint32_t pid, uint32_t cur_pid,
                               uint32_t partition_num, uint64_t last_time,
                               std::shared_ptr<::rtidb::api::TaskInfo> task);

    void ExtractIndexDataInternal(
        std::shared_ptr<::rtidb::storage::Table> table,
        std::shared_ptr<::rtidb::storage::MemTableSnapshot> memtable_snapshot,
        ::rtidb::common::ColumnKey& column_key, uint32_t idx,  // NOLINT
        uint32_t partition_num, std::shared_ptr<::rtidb::api::TaskInfo> task);

    void SchedMakeSnapshot();

    void SchedMakeDiskTableSnapshot();

    void GetDiskused();

    void CheckZkClient();

    void RefreshTableInfo();

    int32_t DeleteTableInternal(
        uint32_t tid, uint32_t pid,
        std::shared_ptr<::rtidb::api::TaskInfo> task_ptr);

    int LoadTableInternal(uint32_t tid, uint32_t pid,
                          std::shared_ptr<::rtidb::api::TaskInfo> task_ptr);
    int LoadDiskTableInternal(uint32_t tid, uint32_t pid,
                              const ::rtidb::api::TableMeta& table_meta,
                              std::shared_ptr<::rtidb::api::TaskInfo> task_ptr);
    int WriteTableMeta(const std::string& path,
                       const ::rtidb::api::TableMeta* table_meta);

    int UpdateTableMeta(const std::string& path,
                        ::rtidb::api::TableMeta* table_meta,
                        bool for_add_column);

    int UpdateTableMeta(const std::string& path,
                        ::rtidb::api::TableMeta* table_meta);

    int AddOPTask(const ::rtidb::api::TaskInfo& task_info,
                  ::rtidb::api::TaskType task_type,
                  std::shared_ptr<::rtidb::api::TaskInfo>& task_ptr);  // NOLINT

    void SetTaskStatus(
        std::shared_ptr<::rtidb::api::TaskInfo>& task_ptr,  // NOLINT
        ::rtidb::api::TaskStatus status);

    int GetTaskStatus(
        std::shared_ptr<::rtidb::api::TaskInfo>& task_ptr,  // NOLINT
        ::rtidb::api::TaskStatus* status);

    std::shared_ptr<::rtidb::api::TaskInfo> FindTask(
        uint64_t op_id, ::rtidb::api::TaskType task_type);

    int AddOPMultiTask(
        const ::rtidb::api::TaskInfo& task_info,
        ::rtidb::api::TaskType task_type,
        std::shared_ptr<::rtidb::api::TaskInfo>& task_ptr);  // NOLINT

    std::shared_ptr<::rtidb::api::TaskInfo> FindMultiTask(
        const ::rtidb::api::TaskInfo& task_info);

    int CheckDimessionPut(const ::rtidb::api::PutRequest* request,
                          uint32_t idx_cnt);

    // sync log data from page cache to disk
    void SchedSyncDisk(uint32_t tid, uint32_t pid);

    // sched replicator to delete binlog
    void SchedDelBinlog(uint32_t tid, uint32_t pid);

    bool CheckGetDone(::rtidb::api::GetType type, uint64_t ts,
                      uint64_t target_ts);

    bool ChooseDBRootPath(uint32_t tid, uint32_t pid,
                          const ::rtidb::common::StorageMode& mode,
                          std::string& path);  // NOLINT

    bool ChooseRecycleBinRootPath(uint32_t tid, uint32_t pid,
                                  const ::rtidb::common::StorageMode& mode,
                                  std::string& path);  // NOLINT

    bool ChooseTableRootPath(uint32_t tid, uint32_t pid,
                             const ::rtidb::common::StorageMode& mode,
                             std::string& path);  // NOLINT

    bool GetTableRootSize(uint32_t tid, uint32_t pid,
                          const ::rtidb::common::StorageMode& mode,
                          uint64_t& size);  // NOLINT

    int32_t GetSnapshotOffset(uint32_t tid, uint32_t pid,
                              common::StorageMode sm,
                              std::string& msg,                   // NOLINT
                              uint64_t& term, uint64_t& offset);  // NOLINT

    void DelRecycle(const std::string& path);

    void SchedDelRecycle();

    bool GetRealEp(uint64_t tid, uint64_t pid,
            std::map<std::string, std::string>* real_ep_map);

    void ProcessQuery(RpcController* controller,
                      const rtidb::api::QueryRequest* request,
                      ::rtidb::api::QueryResponse* response,
                      butil::IOBuf* buf);
    void ProcessBatchRequestQuery(RpcController* controller,
        const rtidb::api::SQLBatchRequestQueryRequest* request,
                                  rtidb::api::SQLBatchRequestQueryResponse* response,
                                  butil::IOBuf& buf);  // NOLINT

 private:
    void RunRequestQuery(RpcController* controller,
        const rtidb::api::QueryRequest& request,
        ::fesql::vm::RequestRunSession& session, // NOLINT 
        rtidb::api::QueryResponse& response, butil::IOBuf& buf); // NOLINT

    void CreateProcedure(const std::shared_ptr<fesql::sdk::ProcedureInfo> sp_info);

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
    std::map<uint64_t, std::list<std::shared_ptr<::rtidb::api::TaskInfo>>>
        task_map_;
    std::set<std::string> sync_snapshot_set_;
    std::map<std::string, std::shared_ptr<FileReceiver>> file_receiver_map_;
    brpc::Server* server_;
    std::map<::rtidb::common::StorageMode, std::vector<std::string>>
        mode_root_paths_;
    std::map<::rtidb::common::StorageMode, std::vector<std::string>>
        mode_recycle_root_paths_;
    std::atomic<bool> follower_;
    std::shared_ptr<std::map<std::string, std::string>> real_ep_map_;
    // thread safe
    std::shared_ptr<::rtidb::catalog::TabletCatalog> catalog_;
    // thread safe
    ::fesql::vm::Engine engine_;
    std::shared_ptr<::fesql::vm::LocalTablet> local_tablet_;
    std::string zk_cluster_;
    std::string zk_path_;
    std::string endpoint_;
    std::shared_ptr<SpCache> sp_cache_;
    std::string notify_path_;
    std::string sp_root_path_;
};

}  // namespace tablet
}  // namespace rtidb

#endif  // SRC_TABLET_TABLET_IMPL_H_
