/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_TABLET_TABLET_IMPL_H_
#define SRC_TABLET_TABLET_IMPL_H_

#include <brpc/server.h>

#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/set.h"
#include "base/spinlock.h"
#include "catalog/schema_adapter.h"
#include "catalog/tablet_catalog.h"
#include "common/thread_pool.h"
#include "proto/tablet.pb.h"
#include "replica/log_replicator.h"
#include "storage/mem_table.h"
#include "storage/mem_table_snapshot.h"
#include "tablet/combine_iterator.h"
#include "tablet/file_receiver.h"
#include "vm/engine.h"
#include "zk/zk_client.h"

using ::baidu::common::ThreadPool;
using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;
using ::openmldb::base::SpinMutex;
using ::openmldb::replica::LogReplicator;
using ::openmldb::replica::ReplicatorRole;
using ::openmldb::storage::IndexDef;
using ::openmldb::storage::MemTable;
using ::openmldb::storage::Snapshot;
using ::openmldb::storage::Table;
using ::openmldb::zk::ZkClient;
using Schema = ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>;

const uint32_t INVALID_REMOTE_TID = UINT32_MAX;

namespace openmldb {
namespace tablet {

typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<Table>>> Tables;
typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<LogReplicator>>> Replicators;
typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<Snapshot>>> Snapshots;

// tablet cache entry for sql procedure
struct SQLProcedureCacheEntry {
    std::shared_ptr<hybridse::sdk::ProcedureInfo> procedure_info;
    std::shared_ptr<hybridse::vm::CompileInfo> request_info;
    std::shared_ptr<hybridse::vm::CompileInfo> batch_request_info;

    SQLProcedureCacheEntry(const std::shared_ptr<hybridse::sdk::ProcedureInfo> pinfo,
                           std::shared_ptr<hybridse::vm::CompileInfo> rinfo,
                           std::shared_ptr<hybridse::vm::CompileInfo> brinfo)
        : procedure_info(pinfo), request_info(rinfo), batch_request_info(brinfo) {}
};
class SpCache : public hybridse::vm::CompileInfoCache {
 public:
    SpCache() : db_sp_map_() {}
    ~SpCache() {}
    void InsertSQLProcedureCacheEntry(const std::string& db, const std::string& sp_name,
                                      std::shared_ptr<hybridse::sdk::ProcedureInfo> procedure_info,
                                      std::shared_ptr<hybridse::vm::CompileInfo> request_info,
                                      std::shared_ptr<hybridse::vm::CompileInfo> batch_request_info) {
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
    std::shared_ptr<hybridse::vm::CompileInfo> GetRequestInfo(const std::string& db, const std::string& sp_name,
                                                              hybridse::base::Status& status) override {  // NOLINT
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        auto db_it = db_sp_map_.find(db);
        if (db_it == db_sp_map_.end()) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }
        auto sp_it = db_it->second.find(sp_name);
        if (sp_it == db_it->second.end()) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }

        if (!sp_it->second.request_info) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }
        return sp_it->second.request_info;
    }
    std::shared_ptr<hybridse::vm::CompileInfo> GetBatchRequestInfo(const std::string& db, const std::string& sp_name,
                                                                   hybridse::base::Status& status) override {  // NOLINT
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        auto db_it = db_sp_map_.find(db);
        if (db_it == db_sp_map_.end()) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }
        auto sp_it = db_it->second.find(sp_name);
        if (sp_it == db_it->second.end()) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }
        if (!sp_it->second.batch_request_info) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }
        return sp_it->second.batch_request_info;
    }

 private:
    std::map<std::string, std::map<std::string, SQLProcedureCacheEntry>> db_sp_map_;
    SpinMutex spin_mutex_;
};
class TabletImpl : public ::openmldb::api::TabletServer {
 public:
    TabletImpl();

    ~TabletImpl();

    bool Init(const std::string& real_endpoint);
    bool Init(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint,
              const std::string& real_endpoint);

    bool RegisterZK();

    void Put(RpcController* controller, const ::openmldb::api::PutRequest* request,
             ::openmldb::api::PutResponse* response, Closure* done);

    void Get(RpcController* controller, const ::openmldb::api::GetRequest* request,
             ::openmldb::api::GetResponse* response, Closure* done);

    void Scan(RpcController* controller, const ::openmldb::api::ScanRequest* request,
              ::openmldb::api::ScanResponse* response, Closure* done);

    void Delete(RpcController* controller, const ::openmldb::api::DeleteRequest* request,
                ::openmldb::api::GeneralResponse* response, Closure* done);

    void Count(RpcController* controller, const ::openmldb::api::CountRequest* request,
               ::openmldb::api::CountResponse* response, Closure* done);

    void Traverse(RpcController* controller, const ::openmldb::api::TraverseRequest* request,
                  ::openmldb::api::TraverseResponse* response, Closure* done);

    void CreateTable(RpcController* controller, const ::openmldb::api::CreateTableRequest* request,
                     ::openmldb::api::CreateTableResponse* response, Closure* done);

    void LoadTable(RpcController* controller, const ::openmldb::api::LoadTableRequest* request,
                   ::openmldb::api::GeneralResponse* response, Closure* done);

    void DropTable(RpcController* controller, const ::openmldb::api::DropTableRequest* request,
                   ::openmldb::api::DropTableResponse* response, Closure* done);

    void Refresh(RpcController* controller, const ::openmldb::api::RefreshRequest* request,
                   ::openmldb::api::GeneralResponse* response, Closure* done);

    void AddReplica(RpcController* controller, const ::openmldb::api::ReplicaRequest* request,
                    ::openmldb::api::AddReplicaResponse* response, Closure* done);

    void DelReplica(RpcController* controller, const ::openmldb::api::ReplicaRequest* request,
                    ::openmldb::api::GeneralResponse* response, Closure* done);

    void AppendEntries(RpcController* controller, const ::openmldb::api::AppendEntriesRequest* request,
                       ::openmldb::api::AppendEntriesResponse* response, Closure* done);

    void UpdateTableMetaForAddField(RpcController* controller,
                                    const ::openmldb::api::UpdateTableMetaForAddFieldRequest* request,
                                    ::openmldb::api::GeneralResponse* response, Closure* done);

    void GetTableStatus(RpcController* controller, const ::openmldb::api::GetTableStatusRequest* request,
                        ::openmldb::api::GetTableStatusResponse* response, Closure* done);

    void ChangeRole(RpcController* controller, const ::openmldb::api::ChangeRoleRequest* request,
                    ::openmldb::api::ChangeRoleResponse* response, Closure* done);

    void MakeSnapshot(RpcController* controller, const ::openmldb::api::GeneralRequest* request,
                      ::openmldb::api::GeneralResponse* response, Closure* done);

    void PauseSnapshot(RpcController* controller, const ::openmldb::api::GeneralRequest* request,
                       ::openmldb::api::GeneralResponse* response, Closure* done);

    void RecoverSnapshot(RpcController* controller, const ::openmldb::api::GeneralRequest* request,
                         ::openmldb::api::GeneralResponse* response, Closure* done);

    void SendSnapshot(RpcController* controller, const ::openmldb::api::SendSnapshotRequest* request,
                      ::openmldb::api::GeneralResponse* response, Closure* done);

    void SendData(RpcController* controller, const ::openmldb::api::SendDataRequest* request,
                  ::openmldb::api::GeneralResponse* response, Closure* done);

    void GetTaskStatus(RpcController* controller, const ::openmldb::api::TaskStatusRequest* request,
                       ::openmldb::api::TaskStatusResponse* response, Closure* done);

    void GetTableSchema(RpcController* controller, const ::openmldb::api::GetTableSchemaRequest* request,
                        ::openmldb::api::GetTableSchemaResponse* response, Closure* done);

    void DeleteOPTask(RpcController* controller, const ::openmldb::api::DeleteTaskRequest* request,
                      ::openmldb::api::GeneralResponse* response, Closure* done);

    void SetExpire(RpcController* controller, const ::openmldb::api::SetExpireRequest* request,
                   ::openmldb::api::GeneralResponse* response, Closure* done);

    void UpdateTTL(RpcController* controller, const ::openmldb::api::UpdateTTLRequest* request,
                   ::openmldb::api::UpdateTTLResponse* response, Closure* done);

    void ExecuteGc(RpcController* controller, const ::openmldb::api::ExecuteGcRequest* request,
                   ::openmldb::api::GeneralResponse* response, Closure* done);

    void ShowMemPool(RpcController* controller, const ::openmldb::api::HttpRequest* request,
                     ::openmldb::api::HttpResponse* response, Closure* done);

    void GetAllSnapshotOffset(RpcController* controller, const ::openmldb::api::EmptyRequest* request,
                              ::openmldb::api::TableSnapshotOffsetResponse* response, Closure* done);

    void GetTermPair(RpcController* controller, const ::openmldb::api::GetTermPairRequest* request,
                     ::openmldb::api::GetTermPairResponse* response, Closure* done);

    void GetCatalog(RpcController* controller, const ::openmldb::api::GetCatalogRequest* request,
                    ::openmldb::api::GetCatalogResponse* response, Closure* done);

    void GetTableFollower(RpcController* controller, const ::openmldb::api::GetTableFollowerRequest* request,
                          ::openmldb::api::GetTableFollowerResponse* response, Closure* done);

    void GetManifest(RpcController* controller, const ::openmldb::api::GetManifestRequest* request,
                     ::openmldb::api::GetManifestResponse* response, Closure* done);

    void ConnectZK(RpcController* controller, const ::openmldb::api::ConnectZKRequest* request,
                   ::openmldb::api::GeneralResponse* response, Closure* done);

    void DisConnectZK(RpcController* controller, const ::openmldb::api::DisConnectZKRequest* request,
                      ::openmldb::api::GeneralResponse* response, Closure* done);

    void DeleteBinlog(RpcController* controller, const ::openmldb::api::GeneralRequest* request,
                      ::openmldb::api::GeneralResponse* response, Closure* done);

    void CheckFile(RpcController* controller, const ::openmldb::api::CheckFileRequest* request,
                   ::openmldb::api::GeneralResponse* response, Closure* done);

    void SetMode(RpcController* controller, const ::openmldb::api::SetModeRequest* request,
                 ::openmldb::api::GeneralResponse* response, Closure* done);

    void DeleteIndex(RpcController* controller, const ::openmldb::api::DeleteIndexRequest* request,
                     ::openmldb::api::GeneralResponse* response, Closure* done);

    void DumpIndexData(RpcController* controller, const ::openmldb::api::DumpIndexDataRequest* request,
                       ::openmldb::api::GeneralResponse* response, Closure* done);

    void LoadIndexData(RpcController* controller, const ::openmldb::api::LoadIndexDataRequest* request,
                       ::openmldb::api::GeneralResponse* response, Closure* done);

    void ExtractIndexData(RpcController* controller, const ::openmldb::api::ExtractIndexDataRequest* request,
                          ::openmldb::api::GeneralResponse* response, Closure* done);

    void AddIndex(RpcController* controller, const ::openmldb::api::AddIndexRequest* request,
                  ::openmldb::api::GeneralResponse* response, Closure* done);

    void SendIndexData(RpcController* controller, const ::openmldb::api::SendIndexDataRequest* request,
                       ::openmldb::api::GeneralResponse* response, Closure* done);

    void Query(RpcController* controller, const openmldb::api::QueryRequest* request,
               openmldb::api::QueryResponse* response, Closure* done);

    void SubQuery(RpcController* controller, const openmldb::api::QueryRequest* request,
                  openmldb::api::QueryResponse* response, Closure* done);

    void SQLBatchRequestQuery(RpcController* controller, const openmldb::api::SQLBatchRequestQueryRequest* request,
                              openmldb::api::SQLBatchRequestQueryResponse* response, Closure* done);
    void SubBatchRequestQuery(RpcController* controller, const openmldb::api::SQLBatchRequestQueryRequest* request,
                              openmldb::api::SQLBatchRequestQueryResponse* response, Closure* done);
    void CancelOP(RpcController* controller, const openmldb::api::CancelOPRequest* request,
                  openmldb::api::GeneralResponse* response, Closure* done);

    void UpdateRealEndpointMap(RpcController* controller, const openmldb::api::UpdateRealEndpointMapRequest* request,
                               openmldb::api::GeneralResponse* response, Closure* done);

    inline void SetServer(brpc::Server* server) { server_ = server; }

    // get on value from specified ttl type index
    int32_t GetIndex(const ::openmldb::api::GetRequest* request, const ::openmldb::api::TableMeta& meta,
                     const std::map<int32_t, std::shared_ptr<Schema>>& vers_schema, CombineIterator* combine_it,
                     std::string* value, uint64_t* ts);

    // scan specified ttl type index
    int32_t ScanIndex(const ::openmldb::api::ScanRequest* request, const ::openmldb::api::TableMeta& meta,
                      const std::map<int32_t, std::shared_ptr<Schema>>& vers_schema, CombineIterator* combine_it,
                      std::string* pairs, uint32_t* count);

    int32_t ScanIndex(const ::openmldb::api::ScanRequest* request, const ::openmldb::api::TableMeta& meta,
                      const std::map<int32_t, std::shared_ptr<Schema>>& vers_schema, CombineIterator* combine_it,
                      butil::IOBuf* buf, uint32_t* count);

    int32_t CountIndex(uint64_t expire_time, uint64_t expire_cnt, ::openmldb::storage::TTLType ttl_type,
                       ::openmldb::storage::TableIterator* it, const ::openmldb::api::CountRequest* request,
                       uint32_t* count);

    std::shared_ptr<Table> GetTable(uint32_t tid, uint32_t pid);

    void CreateProcedure(RpcController* controller, const openmldb::api::CreateProcedureRequest* request,
                         openmldb::api::GeneralResponse* response, Closure* done);

    void DropProcedure(RpcController* controller, const ::openmldb::api::DropProcedureRequest* request,
                       ::openmldb::api::GeneralResponse* response, Closure* done);

 private:
    bool CreateMultiDir(const std::vector<std::string>& dirs);
    // Get table by table id , no need external synchronization
    // Get table by table id , and Need external synchronization
    std::shared_ptr<Table> GetTableUnLock(uint32_t tid, uint32_t pid);

    std::shared_ptr<LogReplicator> GetReplicator(uint32_t tid, uint32_t pid);

    std::shared_ptr<LogReplicator> GetReplicatorUnLock(uint32_t tid, uint32_t pid);
    std::shared_ptr<Snapshot> GetSnapshot(uint32_t tid, uint32_t pid);

    std::shared_ptr<Snapshot> GetSnapshotUnLock(uint32_t tid, uint32_t pid);

    void GcTable(uint32_t tid, uint32_t pid, bool execute_once);

    void GcTableSnapshot(uint32_t tid, uint32_t pid);

    int CheckTableMeta(const openmldb::api::TableMeta* table_meta,
                       std::string& msg);  // NOLINT

    int CreateTableInternal(const ::openmldb::api::TableMeta* table_meta,
                            std::string& msg);  // NOLINT

    void MakeSnapshotInternal(uint32_t tid, uint32_t pid, uint64_t end_offset,
                              std::shared_ptr<::openmldb::api::TaskInfo> task);

    void SendSnapshotInternal(const std::string& endpoint, uint32_t tid, uint32_t pid, uint32_t remote_tid,
                              std::shared_ptr<::openmldb::api::TaskInfo> task);

    void DumpIndexDataInternal(std::shared_ptr<::openmldb::storage::Table> table,
                               std::shared_ptr<::openmldb::storage::MemTableSnapshot> memtable_snapshot,
                               uint32_t partition_num,
                               ::openmldb::common::ColumnKey& column_key,  // NOLINT
                               uint32_t idx, std::shared_ptr<::openmldb::api::TaskInfo> task);

    void SendIndexDataInternal(std::shared_ptr<::openmldb::storage::Table> table,
                               const std::map<uint32_t, std::string>& pid_endpoint_map,
                               std::shared_ptr<::openmldb::api::TaskInfo> task);

    void LoadIndexDataInternal(uint32_t tid, uint32_t pid, uint32_t cur_pid, uint32_t partition_num, uint64_t last_time,
                               std::shared_ptr<::openmldb::api::TaskInfo> task);

    void ExtractIndexDataInternal(std::shared_ptr<::openmldb::storage::Table> table,
                                  std::shared_ptr<::openmldb::storage::MemTableSnapshot> memtable_snapshot,
                                  ::openmldb::common::ColumnKey& column_key, uint32_t idx,  // NOLINT
                                  uint32_t partition_num, std::shared_ptr<::openmldb::api::TaskInfo> task);

    void SchedMakeSnapshot();

    void GetDiskused();

    void CheckZkClient();

    void RefreshTableInfo();

    bool RefreshSingleTable(uint32_t tid);

    int32_t DeleteTableInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::openmldb::api::TaskInfo> task_ptr);

    int LoadTableInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::openmldb::api::TaskInfo> task_ptr);
    int WriteTableMeta(const std::string& path, const ::openmldb::api::TableMeta* table_meta);

    int UpdateTableMeta(const std::string& path, ::openmldb::api::TableMeta* table_meta, bool for_add_column);

    int UpdateTableMeta(const std::string& path, ::openmldb::api::TableMeta* table_meta);

    int AddOPTask(const ::openmldb::api::TaskInfo& task_info, ::openmldb::api::TaskType task_type,
                  std::shared_ptr<::openmldb::api::TaskInfo>& task_ptr);  // NOLINT

    void SetTaskStatus(std::shared_ptr<::openmldb::api::TaskInfo>& task_ptr,  // NOLINT
                       ::openmldb::api::TaskStatus status);

    int GetTaskStatus(std::shared_ptr<::openmldb::api::TaskInfo>& task_ptr,  // NOLINT
                      ::openmldb::api::TaskStatus* status);

    std::shared_ptr<::openmldb::api::TaskInfo> FindTask(uint64_t op_id, ::openmldb::api::TaskType task_type);

    int AddOPMultiTask(const ::openmldb::api::TaskInfo& task_info, ::openmldb::api::TaskType task_type,
                       std::shared_ptr<::openmldb::api::TaskInfo>& task_ptr);  // NOLINT

    std::shared_ptr<::openmldb::api::TaskInfo> FindMultiTask(const ::openmldb::api::TaskInfo& task_info);

    int CheckDimessionPut(const ::openmldb::api::PutRequest* request, uint32_t idx_cnt);

    // sync log data from page cache to disk
    void SchedSyncDisk(uint32_t tid, uint32_t pid);

    // sched replicator to delete binlog
    void SchedDelBinlog(uint32_t tid, uint32_t pid);

    bool CheckGetDone(::openmldb::api::GetType type, uint64_t ts, uint64_t target_ts);

    bool ChooseDBRootPath(uint32_t tid, uint32_t pid,
                          std::string& path);  // NOLINT

    bool ChooseRecycleBinRootPath(uint32_t tid, uint32_t pid,
                                  std::string& path);  // NOLINT

    bool ChooseTableRootPath(uint32_t tid, uint32_t pid,
                             std::string& path);  // NOLINT

    bool GetTableRootSize(uint32_t tid, uint32_t pid,
                          uint64_t& size);  // NOLINT

    int32_t GetSnapshotOffset(uint32_t tid, uint32_t pid,
                              std::string& msg,                   // NOLINT
                              uint64_t& term, uint64_t& offset);  // NOLINT

    void DelRecycle(const std::string& path);

    void SchedDelRecycle();

    bool GetRealEp(uint64_t tid, uint64_t pid, std::map<std::string, std::string>* real_ep_map);

    void ProcessQuery(RpcController* controller, const openmldb::api::QueryRequest* request,
                      ::openmldb::api::QueryResponse* response, butil::IOBuf* buf);
    void ProcessBatchRequestQuery(RpcController* controller, const openmldb::api::SQLBatchRequestQueryRequest* request,
                                  openmldb::api::SQLBatchRequestQueryResponse* response,
                                  butil::IOBuf& buf);  // NOLINT

 private:
    void RunRequestQuery(RpcController* controller, const openmldb::api::QueryRequest& request,
                         ::hybridse::vm::RequestRunSession& session,                  // NOLINT
                         openmldb::api::QueryResponse& response, butil::IOBuf& buf);  // NOLINT

    void CreateProcedure(const std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info);

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
    std::map<uint64_t, std::list<std::shared_ptr<::openmldb::api::TaskInfo>>> task_map_;
    std::set<std::string> sync_snapshot_set_;
    std::map<std::string, std::shared_ptr<FileReceiver>> file_receiver_map_;
    brpc::Server* server_;
    std::vector<std::string> mode_root_paths_;
    std::vector<std::string> mode_recycle_root_paths_;
    std::atomic<bool> follower_;
    std::shared_ptr<std::map<std::string, std::string>> real_ep_map_;
    // thread safe
    std::shared_ptr<::openmldb::catalog::TabletCatalog> catalog_;
    // thread safe
    std::unique_ptr<::hybridse::vm::Engine> engine_;
    std::shared_ptr<::hybridse::vm::LocalTablet> local_tablet_;
    std::string zk_cluster_;
    std::string zk_path_;
    std::string endpoint_;
    std::shared_ptr<SpCache> sp_cache_;
    std::string notify_path_;
    std::string sp_root_path_;
};

}  // namespace tablet
}  // namespace openmldb

#endif  // SRC_TABLET_TABLET_IMPL_H_
