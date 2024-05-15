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

#ifndef SRC_CLIENT_TABLET_CLIENT_H_
#define SRC_CLIENT_TABLET_CLIENT_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/ddl_parser.h"
#include "base/kv_iterator.h"
#include "base/status.h"
#include "brpc/channel.h"
#include "client/client.h"
#include "codec/schema_codec.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "sdk/option.h"

namespace openmldb {

// forward decl
namespace sdk {
class SQLRequestRowBatch;
}  // namespace sdk

namespace client {
using ::openmldb::api::TaskInfo;
const uint32_t INVALID_REMOTE_TID = UINT32_MAX;

class TabletClient : public Client {
 public:
    TabletClient(const std::string& endpoint, const std::string& real_endpoint,
                 const openmldb::authn::AuthToken auth_token = openmldb::authn::ServiceToken{"default"});

    TabletClient(const std::string& endpoint, const std::string& real_endpoint, bool use_sleep_policy,
                 const openmldb::authn::AuthToken auth_token = openmldb::authn::ServiceToken{"default"});

    ~TabletClient();

    int Init() override;

    base::Status CreateTable(const ::openmldb::api::TableMeta& table_meta);

    base::Status TruncateTable(uint32_t tid, uint32_t pid);

    bool UpdateTableMetaForAddField(uint32_t tid, const std::vector<openmldb::common::ColumnDesc>& cols,
                                    const openmldb::common::VersionPair& pair,
                                    std::string& msg);  // NOLINT

    bool Query(const std::string& db, const std::string& sql,
               const std::vector<openmldb::type::DataType>& parameter_types, const std::string& parameter_row,
               brpc::Controller* cntl, ::openmldb::api::QueryResponse* response, const bool is_debug = false);

    bool Query(const std::string& db, const std::string& sql, const std::string& row, brpc::Controller* cntl,
               ::openmldb::api::QueryResponse* response, const bool is_debug = false);

    bool SQLBatchRequestQuery(const std::string& db, const std::string& sql,
                              std::shared_ptr<::openmldb::sdk::SQLRequestRowBatch>, brpc::Controller* cntl,
                              ::openmldb::api::SQLBatchRequestQueryResponse* response, const bool is_debug = false);

    base::Status Put(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time, const std::string& value);

    base::Status Put(uint32_t tid, uint32_t pid, uint64_t time, const std::string& value,
             const std::vector<std::pair<std::string, uint32_t>>& dimensions,
             int memory_usage_limit = 0, bool put_if_absent = false);

    base::Status Put(uint32_t tid, uint32_t pid, uint64_t time, const base::Slice& value,
            ::google::protobuf::RepeatedPtrField<::openmldb::api::Dimension>* dimensions,
            int memory_usage_limit = 0, bool put_if_absent = false);

    bool Get(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time, std::string& value,  // NOLINT
             uint64_t& ts,                                                                          // NOLINT
             std::string& msg);  // NOLINT

    bool Get(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time, const std::string& idx_name,
             std::string& value, uint64_t& ts, std::string& msg);  // NOLINT

    bool Delete(uint32_t tid, uint32_t pid, const std::string& pk, const std::string& idx_name,
                std::string& msg);  // NOLINT

    base::Status Delete(uint32_t tid, uint32_t pid, const sdk::DeleteOption& option, uint64_t timeout_ms);

    bool Count(uint32_t tid, uint32_t pid, const std::string& pk, const std::string& idx_name, bool filter_expired_data,
               uint64_t& value, std::string& msg);  // NOLINT

    std::shared_ptr<openmldb::base::ScanKvIterator> Scan(uint32_t tid, uint32_t pid, const std::string& pk,
                                                         const std::string& idx_name, uint64_t stime, uint64_t etime,
                                                         uint32_t limit, uint32_t skip_record_num,
                                                         std::string& msg);  // NOLINT

    std::shared_ptr<openmldb::base::ScanKvIterator> Scan(uint32_t tid, uint32_t pid, const std::string& pk,
                                                         const std::string& idx_name, uint64_t stime, uint64_t etime,
                                                         uint32_t limit, std::string& msg);  // NOLINT

    bool Scan(const ::openmldb::api::ScanRequest& request, brpc::Controller* cntl,
              ::openmldb::api::ScanResponse* response);

    bool AsyncScan(const ::openmldb::api::ScanRequest& request,
                   openmldb::RpcCallback<openmldb::api::ScanResponse>* callback);

    bool GetTableSchema(uint32_t tid, uint32_t pid,
                        ::openmldb::api::TableMeta& table_meta);  // NOLINT

    bool DropTable(uint32_t id, uint32_t pid, std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool AddReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
                    std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool AddReplica(uint32_t tid, uint32_t pid, const std::string& endpoint, uint32_t remote_tid,
                    std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool DelReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
                    std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool MakeSnapshot(uint32_t tid, uint32_t pid, uint64_t offset,
                      std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool SendSnapshot(uint32_t tid, uint32_t remote_tid, uint32_t pid, const std::string& endpoint,
                      std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool PauseSnapshot(uint32_t tid, uint32_t pid, std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool RecoverSnapshot(uint32_t tid, uint32_t pid, std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    base::Status LoadTable(const std::string& name, uint32_t id, uint32_t pid, uint64_t ttl, uint32_t seg_cnt);

    base::Status LoadTable(const std::string& name, uint32_t id, uint32_t pid, uint64_t ttl, bool leader,
                           uint32_t seg_cnt, std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    // for ns WrapTaskFun, must return bool
    bool LoadTable(const ::openmldb::api::TableMeta& table_meta, std::shared_ptr<TaskInfo> task_info);

    bool ChangeRole(uint32_t tid, uint32_t pid, bool leader, uint64_t term);

    bool ChangeRole(uint32_t tid, uint32_t pid, bool leader, const std::vector<std::string>& endpoints, uint64_t term,
                    const std::vector<::openmldb::common::EndpointAndTid>* et = nullptr);

    bool UpdateTTL(uint32_t tid, uint32_t pid, const ::openmldb::type::TTLType& type, uint64_t abs_ttl,
                   uint64_t lat_ttl, const std::string& index_name);

    bool DeleteBinlog(uint32_t tid, uint32_t pid, ::openmldb::common::StorageMode storage_mode);

    bool GetTaskStatus(::openmldb::api::TaskStatusResponse& response);  // NOLINT

    bool DeleteOPTask(const std::vector<uint64_t>& op_id_vec);

    bool GetTermPair(uint32_t tid, uint32_t pid,
                     ::openmldb::common::StorageMode storage_mode,  // NOLINT
                     uint64_t& term,                                // NOLINT
                     uint64_t& offset, bool& has_table,             // NOLINT
                     bool& is_leader);                              // NOLINT

    bool GetManifest(uint32_t tid, uint32_t pid, ::openmldb::common::StorageMode storage_mode,
                     ::openmldb::api::Manifest& manifest);  // NOLINT

    base::Status GetTableStatus(::openmldb::api::GetTableStatusResponse& response);  // NOLINT
    base::Status GetTableStatus(uint32_t tid, uint32_t pid,
                                ::openmldb::api::TableStatus& table_status);  // NOLINT
    base::Status GetTableStatus(uint32_t tid, uint32_t pid, bool need_schema,
                                ::openmldb::api::TableStatus& table_status);  // NOLINT

    bool FollowOfNoOne(uint32_t tid, uint32_t pid, uint64_t term,
                       uint64_t& offset);  // NOLINT

    base::Status GetTableFollower(uint32_t tid, uint32_t pid,
                                  uint64_t& offset,                            // NOLINT
                                  std::map<std::string, uint64_t>& info_map);  // NOLINT

    bool GetAllSnapshotOffset(std::map<uint32_t, std::map<uint32_t, uint64_t>>& tid_pid_offset);  // NOLINT

    bool SetExpire(uint32_t tid, uint32_t pid, bool is_expire);
    bool ConnectZK();
    bool DisConnectZK();

    std::shared_ptr<openmldb::base::TraverseKvIterator> Traverse(uint32_t tid, uint32_t pid,
                                                                 const std::string& idx_name, const std::string& pk,
                                                                 uint64_t ts, uint32_t limit, bool skip_current_pk,
                                                                 uint32_t ts_pos, uint32_t& count);  // NOLINT

    bool SetMode(bool mode);

    bool DeleteIndex(uint32_t tid, uint32_t pid, const std::string& idx_name, std::string* msg);

    bool AddIndex(uint32_t tid, uint32_t pid, const ::openmldb::common::ColumnKey& column_key,
                  std::shared_ptr<TaskInfo> task_info);

    bool AddMultiIndex(uint32_t tid, uint32_t pid, const std::vector<::openmldb::common::ColumnKey>& column_keys,
                       std::shared_ptr<TaskInfo> task_info);

    bool GetCatalog(uint64_t* version);

    bool SendIndexData(uint32_t tid, uint32_t pid, const std::map<uint32_t, std::string>& pid_endpoint_map,
                       std::shared_ptr<TaskInfo> task_info);

    bool LoadIndexData(uint32_t tid, uint32_t pid, uint32_t partition_num, std::shared_ptr<TaskInfo> task_info);

    bool ExtractIndexData(uint32_t tid, uint32_t pid, uint32_t partition_num,
                          const std::vector<::openmldb::common::ColumnKey>& column_key, uint64_t offset, bool dump_data,
                          std::shared_ptr<TaskInfo> task_info);

    bool CancelOP(const uint64_t op_id);

    bool UpdateRealEndpointMap(const std::map<std::string, std::string>& map);

    base::Status CreateProcedure(const openmldb::api::CreateProcedureRequest& sp_request);

    bool CallProcedure(const std::string& db, const std::string& sp_name, const base::Slice& row,
                       brpc::Controller* cntl, openmldb::api::QueryResponse* response, bool is_debug,
                       uint64_t timeout_ms);

    bool CallSQLBatchRequestProcedure(const std::string& db, const std::string& sp_name,
                                      std::shared_ptr<::openmldb::sdk::SQLRequestRowBatch>, brpc::Controller* cntl,
                                      openmldb::api::SQLBatchRequestQueryResponse* response, bool is_debug,
                                      uint64_t timeout_ms);

    base::Status CallSQLBatchRequestProcedure(const std::string& db, const std::string& sp_name,
                                              const base::Slice& meta, const base::Slice& data, bool is_debug,
                                              uint64_t timeout_ms, brpc::Controller* cntl,
                                              openmldb::api::SQLBatchRequestQueryResponse* response);

    bool DropProcedure(const std::string& db_name, const std::string& sp_name);

    bool Refresh(uint32_t tid);

    bool SubQuery(const ::openmldb::api::QueryRequest& request,
                  openmldb::RpcCallback<openmldb::api::QueryResponse>* callback);

    bool SubBatchRequestQuery(const ::openmldb::api::SQLBatchRequestQueryRequest& request,
                              openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback);

    bool CreateFunction(const ::openmldb::common::ExternalFun& fun, std::string* msg);

    bool DropFunction(const ::openmldb::common::ExternalFun& fun, std::string* msg);

    bool CallProcedure(const std::string& db, const std::string& sp_name, const base::Slice& row, uint64_t timeout_ms,
                       bool is_debug, openmldb::RpcCallback<openmldb::api::QueryResponse>* callback);

    bool CallSQLBatchRequestProcedure(const std::string& db, const std::string& sp_name,
                                      std::shared_ptr<::openmldb::sdk::SQLRequestRowBatch> row_batch, bool is_debug,
                                      uint64_t timeout_ms,
                                      openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback);

    base::Status CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name, const base::Slice& meta, const base::Slice& data,
        bool is_debug, uint64_t timeout_ms,
        openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback);

    bool CreateAggregator(const ::openmldb::api::TableMeta& base_table_meta, uint32_t aggr_tid, uint32_t aggr_pid,
                          uint32_t index_pos, const ::openmldb::base::LongWindowInfo& window_info);

    bool GetAndFlushDeployStats(::openmldb::api::DeployStatsResponse* res);

    bool FlushPrivileges();

 private:
    base::Status LoadTableInternal(const ::openmldb::api::TableMeta& table_meta, std::shared_ptr<TaskInfo> task_info);

 private:
    ::openmldb::RpcClient<::openmldb::api::TabletServer_Stub> client_;
};

}  // namespace client
}  // namespace openmldb

#endif  // SRC_CLIENT_TABLET_CLIENT_H_
