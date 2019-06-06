//
// tablet_client.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-02 
// 


#ifndef RTIDB_TABLET_CLIENT_H
#define RTIDB_TABLET_CLIENT_H

#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "base/kv_iterator.h"
#include "base/schema_codec.h"

namespace rtidb {
namespace client {
using ::rtidb::api::TaskInfo;

class TabletClient {

public:
    TabletClient(const std::string& endpoint);

    TabletClient(const std::string& endpoint, bool use_sleep_policy);

    ~TabletClient();

    int Init();

    std::string GetEndpoint();

    bool CreateTable(const std::string& name,
                     uint32_t tid, uint32_t pid, uint64_t ttl,
                     bool leader, 
                     const std::vector<std::string>& endpoints,
                     const ::rtidb::api::TTLType& type,
                     uint32_t seg_cnt, uint64_t term, 
                     const ::rtidb::api::CompressType compress_type);


    bool CreateTable(const std::string& name, 
                     uint32_t tid, uint32_t pid,
                     uint64_t ttl, uint32_t seg_cnt,
                     const std::vector<::rtidb::base::ColumnDesc>& columns,
                     const ::rtidb::api::TTLType& type,
                     bool leader, const std::vector<std::string>& endpoints,
                     uint64_t term = 0, const ::rtidb::api::CompressType compress_type = ::rtidb::api::CompressType::kNoCompress);
                    
    bool CreateTable(const ::rtidb::api::TableMeta& table_meta);                

    bool Put(uint32_t tid,
             uint32_t pid,
             const std::string& pk, 
             uint64_t time,
             const std::string& value);

    bool Put(uint32_t tid,
             uint32_t pid,
             const char* pk,
             uint64_t time,
             const char* value,
             uint32_t size);

    bool Put(uint32_t tid,
             uint32_t pid,
             uint64_t time,
             const std::string& value,
             const std::vector<std::pair<std::string, uint32_t> >& dimensions);

    bool Put(uint32_t tid,
             uint32_t pid,
             const std::vector<std::pair<std::string, uint32_t>>& dimensions,
             const std::vector<uint64_t>& ts_dimensions,
             const std::string& value);

    bool Get(uint32_t tid, 
             uint32_t pid,
             const std::string& pk,
             uint64_t time,
             std::string& value,
             uint64_t& ts,
             std::string& msg);

    bool Get(uint32_t tid, 
             uint32_t pid,
             const std::string& pk,
             uint64_t time,
             const std::string& idx_name,
             std::string& value,
             uint64_t& ts,
             std::string& msg);

    bool Delete(uint32_t tid, uint32_t pid, 
             const std::string& pk, const std::string& idx_name,
             std::string& msg);

    bool Count(uint32_t tid, uint32_t pid, const std::string& pk,
             const std::string& idx_name, bool filter_expired_data, 
             uint64_t& value, std::string& msg);

    ::rtidb::base::KvIterator* Scan(uint32_t tid,
             uint32_t pid,
             const std::string& pk,
             uint64_t stime,
             uint64_t etime,
             uint32_t limit,
             std::string& msg);
    
    ::rtidb::base::KvIterator* Scan(uint32_t tid,
                                 uint32_t pid,
                                 const std::string& pk,
                                 uint64_t stime,
                                 uint64_t etime,
                                 const std::string& idx_name,
                                 uint32_t limit,
                                 std::string& msg);

    ::rtidb::base::KvIterator* Scan(uint32_t tid,
             uint32_t pid,
             const char* pk,
             uint64_t stime,
             uint64_t etime,
             std::string& msg,
             bool showm = false);

    bool GetTableSchema(uint32_t tid, uint32_t pid, 
                        std::string& schema);

    bool DropTable(uint32_t id, uint32_t pid,
                std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool AddReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
                std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool DelReplica(uint32_t tid, uint32_t pid, const std::string& endpoint,
                std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool MakeSnapshot(uint32_t tid, uint32_t pid, 
                std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool SendSnapshot(uint32_t tid, uint32_t pid, const std::string& endpoint, 
                std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool PauseSnapshot(uint32_t tid, uint32_t pid, 
                std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool RecoverSnapshot(uint32_t tid, uint32_t pid,
                std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool LoadTable(const std::string& name, uint32_t id, uint32_t pid, uint64_t ttl, uint32_t seg_cnt);

    bool LoadTable(const std::string& name, uint32_t id, uint32_t pid, uint64_t ttl,
               bool leader, const std::vector<std::string>& endpoints, uint32_t seg_cnt,
               std::shared_ptr<TaskInfo> task_info = std::shared_ptr<TaskInfo>());

    bool ChangeRole(uint32_t tid, uint32_t pid, bool leader, uint64_t term = 0);

    bool ChangeRole(uint32_t tid, uint32_t pid, bool leader, 
                    const std::vector<std::string>& endpoints, uint64_t term = 0);

    bool UpdateTTL(uint32_t tid, uint32_t pid, 
                   const ::rtidb::api::TTLType& type,
                   uint64_t ttl, const std::string& ts_name);
    bool SetMaxConcurrency(const std::string& key, int32_t max_concurrency);
    bool DeleteBinlog(uint32_t tid, uint32_t pid);

    bool GetTaskStatus(::rtidb::api::TaskStatusResponse& response);               

    bool DeleteOPTask(const std::vector<uint64_t>& op_id_vec);

    bool GetTermPair(uint32_t tid, uint32_t pid, uint64_t& term, uint64_t& offset, bool& has_table, bool& is_leader);

    bool GetManifest(uint32_t tid, uint32_t pid, ::rtidb::api::Manifest& manifest);

    bool GetTableStatus(::rtidb::api::GetTableStatusResponse& response);
    bool GetTableStatus(uint32_t tid, uint32_t pid,
                    ::rtidb::api::TableStatus& table_status);
    bool GetTableStatus(uint32_t tid, uint32_t pid, bool need_schema,
                    ::rtidb::api::TableStatus& table_status);

    bool FollowOfNoOne(uint32_t tid, uint32_t pid, uint64_t term, uint64_t& offset);

    bool GetTableFollower(uint32_t tid, uint32_t pid, uint64_t& offset, 
                    std::map<std::string, uint64_t>& info_map, std::string& msg);
    
    bool SetExpire(uint32_t tid, uint32_t pid, bool is_expire);
    bool SetTTLClock(uint32_t tid, uint32_t pid, uint64_t timestamp);
    bool ConnectZK();
    bool DisConnectZK();

    ::rtidb::base::KvIterator* Traverse(uint32_t tid, uint32_t pid, const std::string& idx_name, 
                const std::string& pk, uint64_t ts, uint32_t limit, uint32_t& count);

    void ShowTp();

private:
    std::string endpoint_;
    ::rtidb::RpcClient<::rtidb::api::TabletServer_Stub> client_;
    std::vector<uint64_t> percentile_;
};


}
}


#endif /* !RTIDB_TABLET_CLIENT_H */
