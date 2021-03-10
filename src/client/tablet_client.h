//
// tablet_client.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize
// Date 2017-04-02
//

#ifndef SRC_CLIENT_TABLET_CLIENT_H_
#define SRC_CLIENT_TABLET_CLIENT_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/kv_iterator.h"
#include "brpc/channel.h"
#include "codec/schema_codec.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"

using Schema = ::google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>;
using Cond_Column = ::google::protobuf::RepeatedPtrField<rtidb::api::Columns>;

namespace rtidb {

// forward decl
namespace sdk {
class SQLRequestRowBatch;
}  // namespace sdk

const uint32_t INVALID_TID = UINT32_MAX;
namespace client {
using ::rtidb::api::TaskInfo;
const uint32_t INVALID_REMOTE_TID = UINT32_MAX;

class TabletClient {
 public:
    explicit TabletClient(const std::string& endpoint, const std::string& real_endpoint);

    TabletClient(const std::string& endpoint, const std::string& real_endpoint, bool use_sleep_policy);

    ~TabletClient();

    int Init();

    std::string GetEndpoint();

    const std::string& GetRealEndpoint() const;

    bool CreateTable(const std::string& name, uint32_t tid, uint32_t pid,
                     uint64_t abs_ttl, uint64_t lat_ttl, bool leader,
                     const std::vector<std::string>& endpoints,
                     const ::rtidb::api::TTLType& type, uint32_t seg_cnt,
                     uint64_t term,
                     const ::rtidb::api::CompressType compress_type);

    bool CreateTable(const std::string& name, uint32_t tid, uint32_t pid,
                     uint64_t abs_ttl, uint64_t lat_ttl, uint32_t seg_cnt,
                     const std::vector<::rtidb::codec::ColumnDesc>& columns,
                     const ::rtidb::api::TTLType& type, bool leader,
                     const std::vector<std::string>& endpoints,
                     uint64_t term = 0,
                     const ::rtidb::api::CompressType compress_type =
                         ::rtidb::api::CompressType::kNoCompress);

    bool CreateTable(const ::rtidb::api::TableMeta& table_meta);

    bool UpdateTableMetaForAddField(
        uint32_t tid, const std::vector<rtidb::common::ColumnDesc>& cols,
        const rtidb::common::VersionPair& pair, const std::string& schema,
        std::string& msg);  // NOLINT

    bool Query(const std::string& db, const std::string& sql,
               brpc::Controller* cntl, ::rtidb::api::QueryResponse* response,
               const bool is_debug = false);

    bool Query(const std::string& db, const std::string& sql,
               const std::string& row, brpc::Controller* cntl,
               ::rtidb::api::QueryResponse* response,
               const bool is_debug = false);

    bool SQLBatchRequestQuery(const std::string& db, const std::string& sql,
                              std::shared_ptr<::rtidb::sdk::SQLRequestRowBatch>,
                              brpc::Controller* cntl,
                              ::rtidb::api::SQLBatchRequestQueryResponse* response,
                              const bool is_debug = false);

    bool Put(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time,
             const std::string& value, uint32_t format_version = 0);

    bool Put(uint32_t tid, uint32_t pid, const char* pk, uint64_t time,
             const char* value, uint32_t size, uint32_t format_version = 0);

    bool Put(uint32_t tid, uint32_t pid, uint64_t time,
             const std::string& value,
             const std::vector<std::pair<std::string, uint32_t>>& dimensions);

    bool Put(uint32_t tid, uint32_t pid, uint64_t time,
             const std::string& value,
             const std::vector<std::pair<std::string, uint32_t>>& dimensions,
             uint32_t format_version);

    bool Put(uint32_t tid, uint32_t pid,
             const std::vector<std::pair<std::string, uint32_t>>& dimensions,
             const std::vector<uint64_t>& ts_dimensions,
             const std::string& value);

    bool Put(uint32_t tid, uint32_t pid,
             const std::vector<std::pair<std::string, uint32_t>>& dimensions,
             const std::vector<uint64_t>& ts_dimensions,
             const std::string& value, uint32_t format_version);

    bool Get(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time,
             std::string& value, uint64_t& ts, std::string& msg);  // NOLINT

    bool Get(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time,
             const std::string& idx_name, std::string& value,  // NOLINT
             uint64_t& ts,                                     // NOLINT
             std::string& msg);                                // NOLINT

    bool Get(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time,
             const std::string& idx_name, const std::string& ts_name,
             std::string& value, uint64_t& ts, std::string& msg);  // NOLINT

    bool Delete(uint32_t tid, uint32_t pid, const std::string& pk,
                const std::string& idx_name, std::string& msg);  // NOLINT

    bool Count(uint32_t tid, uint32_t pid, const std::string& pk,
               const std::string& idx_name, bool filter_expired_data,
               uint64_t& value, std::string& msg);  // NOLINT

    bool Count(uint32_t tid, uint32_t pid, const std::string& pk,
               const std::string& idx_name, const std::string& ts_name,
               bool filter_expired_data, uint64_t& value,  // NOLINT
               std::string& msg);                          // NOLINT

    ::rtidb::base::KvIterator* Scan(uint32_t tid, uint32_t pid,
                                    const std::string& pk, uint64_t stime,
                                    uint64_t etime, uint32_t limit,
                                    uint32_t atleast,
                                    std::string& msg);  // NOLINT

    ::rtidb::base::KvIterator* Scan(uint32_t tid, uint32_t pid,
                                    const std::string& pk, uint64_t stime,
                                    uint64_t etime, const std::string& idx_name,
                                    const std::string& ts_name, uint32_t limit,
                                    uint32_t atleast,
                                    std::string& msg);  // NOLINT

    ::rtidb::base::KvIterator* Scan(uint32_t tid, uint32_t pid,
                                    const std::string& pk, uint64_t stime,
                                    uint64_t etime, const std::string& idx_name,
                                    uint32_t limit, uint32_t atleast,
                                    std::string& msg);  // NOLINT

    ::rtidb::base::KvIterator* Scan(uint32_t tid, uint32_t pid, const char* pk,
                                    uint64_t stime, uint64_t etime,
                                    std::string& msg,     // NOLINT
                                    bool showm = false);  // NOLINT

    bool Scan(const ::rtidb::api::ScanRequest& request,
             brpc::Controller* cntl,
             ::rtidb::api::ScanResponse* response);

    bool AsyncScan(const ::rtidb::api::ScanRequest& request,
                   rtidb::RpcCallback<rtidb::api::ScanResponse>* callback);

    bool GetTableSchema(uint32_t tid, uint32_t pid,
                        ::rtidb::api::TableMeta& table_meta);  // NOLINT

    bool DropTable(
        uint32_t id, uint32_t pid,
        std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool AddReplica(
        uint32_t tid, uint32_t pid, const std::string& endpoint,
        std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool AddReplica(
        uint32_t tid, uint32_t pid, const std::string& endpoint,
        uint32_t remote_tid,
        std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool DelReplica(
        uint32_t tid, uint32_t pid, const std::string& endpoint,
        std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool MakeSnapshot(
        uint32_t tid, uint32_t pid, uint64_t offset,
        std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool SendSnapshot(
        uint32_t tid, uint32_t remote_tid, uint32_t pid,
        const std::string& endpoint,
        std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool PauseSnapshot(
        uint32_t tid, uint32_t pid,
        std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool RecoverSnapshot(
        uint32_t tid, uint32_t pid,
        std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool LoadTable(const std::string& name, uint32_t id, uint32_t pid,
                   uint64_t ttl, uint32_t seg_cnt);

    bool LoadTable(
        const std::string& name, uint32_t id, uint32_t pid, uint64_t ttl,
        bool leader, uint32_t seg_cnt,
        ::rtidb::common::StorageMode storage_mode,
        std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool LoadTable(const ::rtidb::api::TableMeta& table_meta,
                   std::shared_ptr<TaskInfo> task_info);

    bool LoadTable(uint32_t tid, uint32_t pid,
                   ::rtidb::common::StorageMode storage_mode, std::string* msg);
    bool ChangeRole(uint32_t tid, uint32_t pid, bool leader, uint64_t term);

    bool ChangeRole(
        uint32_t tid, uint32_t pid, bool leader,
        const std::vector<std::string>& endpoints, uint64_t term,
        const std::vector<::rtidb::common::EndpointAndTid>* et = nullptr);

    bool UpdateTTL(uint32_t tid, uint32_t pid,
                   const ::rtidb::api::TTLType& type, uint64_t abs_ttl,
                   uint64_t lat_ttl, const std::string& ts_name);
    bool SetMaxConcurrency(const std::string& key, int32_t max_concurrency);
    bool DeleteBinlog(uint32_t tid, uint32_t pid,
                      ::rtidb::common::StorageMode storage_mode);

    bool GetTaskStatus(::rtidb::api::TaskStatusResponse& response);  // NOLINT

    bool DeleteOPTask(const std::vector<uint64_t>& op_id_vec);

    bool GetTermPair(uint32_t tid, uint32_t pid,
                     ::rtidb::common::StorageMode storage_mode,
                     uint64_t& term,                     // NOLINT
                     uint64_t& offset, bool& has_table,  // NOLINT
                     bool& is_leader);                   // NOLINT

    bool GetManifest(uint32_t tid, uint32_t pid,
                     ::rtidb::common::StorageMode storage_mode,
                     ::rtidb::api::Manifest& manifest);  // NOLINT

    bool GetTableStatus(
        ::rtidb::api::GetTableStatusResponse& response);  // NOLINT
    bool GetTableStatus(uint32_t tid, uint32_t pid,
                        ::rtidb::api::TableStatus& table_status);  // NOLINT
    bool GetTableStatus(uint32_t tid, uint32_t pid, bool need_schema,
                        ::rtidb::api::TableStatus& table_status);  // NOLINT

    bool FollowOfNoOne(uint32_t tid, uint32_t pid, uint64_t term,
                       uint64_t& offset);  // NOLINT

    bool GetTableFollower(uint32_t tid, uint32_t pid,
                          uint64_t& offset,                           // NOLINT
                          std::map<std::string, uint64_t>& info_map,  // NOLINT
                          std::string& msg);                          // NOLINT

    bool GetAllSnapshotOffset(std::map<uint32_t, std::map<uint32_t, uint64_t>>&
                                  tid_pid_offset);  // NOLINT

    bool SetExpire(uint32_t tid, uint32_t pid, bool is_expire);
    bool ConnectZK();
    bool DisConnectZK();

    ::rtidb::base::KvIterator* Traverse(uint32_t tid, uint32_t pid,
                                        const std::string& idx_name,
                                        const std::string& pk, uint64_t ts,
                                        uint32_t limit,
                                        uint32_t& count);  // NOLINT

    void ShowTp();

    bool SetMode(bool mode);

    bool DeleteIndex(uint32_t tid, uint32_t pid, const std::string& idx_name,
                     std::string* msg);

    bool AddIndex(uint32_t tid, uint32_t pid,
                  const ::rtidb::common::ColumnKey& column_key,
                  std::shared_ptr<TaskInfo> task_info);

    bool DumpIndexData(uint32_t tid, uint32_t pid, uint32_t partition_num,
                       const ::rtidb::common::ColumnKey& column_key,
                       uint32_t idx, std::shared_ptr<TaskInfo> task_info);

    bool GetCatalog(uint64_t* version);

    bool SendIndexData(uint32_t tid, uint32_t pid,
                       const std::map<uint32_t, std::string>& pid_endpoint_map,
                       std::shared_ptr<TaskInfo> task_info);

    bool LoadIndexData(uint32_t tid, uint32_t pid, uint32_t partition_num,
                       std::shared_ptr<TaskInfo> task_info);

    bool ExtractIndexData(uint32_t tid, uint32_t pid, uint32_t partition_num,
                          const ::rtidb::common::ColumnKey& column_key,
                          uint32_t idx, std::shared_ptr<TaskInfo> task_info);

    bool CancelOP(const uint64_t op_id);

    bool UpdateRealEndpointMap(const std::map<std::string, std::string>& map);

    bool CreateProcedure(const rtidb::api::CreateProcedureRequest& sp_request,
            std::string& msg); // NOLINT

    bool CallProcedure(const std::string& db, const std::string& sp_name,
            const std::string& row, brpc::Controller* cntl,
            rtidb::api::QueryResponse* response,
            bool is_debug, uint64_t timeout_ms);

    bool CallSQLBatchRequestProcedure(const std::string& db, const std::string& sp_name,
            std::shared_ptr<::rtidb::sdk::SQLRequestRowBatch>,
            brpc::Controller* cntl,
            rtidb::api::SQLBatchRequestQueryResponse* response,
            bool is_debug, uint64_t timeout_ms);

    bool DropProcedure(const std::string& db_name, const std::string& sp_name);

    bool SubQuery(const ::rtidb::api::QueryRequest& request,
            rtidb::RpcCallback<rtidb::api::QueryResponse>* callback);

    bool SubBatchRequestQuery(const ::rtidb::api::SQLBatchRequestQueryRequest& request,
                              rtidb::RpcCallback<rtidb::api::SQLBatchRequestQueryResponse>* callback);
    bool CallProcedure(const std::string& db, const std::string& sp_name,
            const std::string& row, uint64_t timeout_ms, bool is_debug,
            rtidb::RpcCallback<rtidb::api::QueryResponse>* callback);

    bool CallSQLBatchRequestProcedure(
            const std::string& db, const std::string& sp_name,
            std::shared_ptr<::rtidb::sdk::SQLRequestRowBatch> row_batch,
            bool is_debug, uint64_t timeout_ms,
            rtidb::RpcCallback<rtidb::api::SQLBatchRequestQueryResponse>* callback);

 private:
    std::string endpoint_;
    std::string real_endpoint_;
    ::rtidb::RpcClient<::rtidb::api::TabletServer_Stub> client_;
    std::vector<uint64_t> percentile_;
};

}  // namespace client
}  // namespace rtidb

#endif  // SRC_CLIENT_TABLET_CLIENT_H_
