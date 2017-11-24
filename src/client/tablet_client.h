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

class TabletClient {

public:
    TabletClient(const std::string& endpoint);

    ~TabletClient();

    int Init();

    std::string GetEndpoint();

    bool CreateTable(const std::string& name, 
                     uint32_t id, 
                     uint32_t pid,
                     uint64_t ttl,
                     uint32_t seg_cnt=16);

    bool CreateTable(const std::string& name,
                     uint32_t tid, uint32_t pid, uint64_t ttl,
                     bool leader, const std::vector<std::string>& endpoints,
                     uint32_t seg_cnt=16);

    bool CreateTable(const std::string& name, uint32_t tid, uint32_t pid,
                     uint64_t ttl, uint32_t seg_cnt, 
                     const std::vector<::rtidb::base::ColumnDesc>& columns);

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

    bool Get(uint32_t tid, 
             uint32_t pid,
             const std::string& pk,
             uint64_t time,
             std::string& value);

    ::rtidb::base::KvIterator* Scan(uint32_t tid,
             uint32_t pid,
             const std::string& pk,
             uint64_t stime,
             uint64_t etime);
    
    ::rtidb::base::KvIterator* Scan(uint32_t tid,
                                 uint32_t pid,
                                 const std::string& pk,
                                 uint64_t stime,
                                 uint64_t etime,
                                 const std::string& idx_name);

    ::rtidb::base::KvIterator* Scan(uint32_t tid,
             uint32_t pid,
             const char* pk,
             uint64_t stime,
             uint64_t etime,
             bool showm = false);

    bool GetTableSchema(uint32_t tid, uint32_t pid, 
                        std::string& schema);

    bool DropTable(uint32_t id, uint32_t pid);

    bool AddReplica(uint32_t tid, uint32_t pid, const std::string& endpoint);

    bool DelReplica(uint32_t tid, uint32_t pid, const std::string& endpoint);

    bool MakeSnapshot(uint32_t tid, uint32_t pid);
    bool MakeSnapshotNS(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task_info);

    bool PauseSnapshot(uint32_t tid, uint32_t pid);

    bool RecoverSnapshot(uint32_t tid, uint32_t pid);

    bool LoadTable(const std::string& name, uint32_t id, uint32_t pid, uint64_t ttl, uint32_t seg_cnt);

    bool LoadTable(const std::string& name, uint32_t id, uint32_t pid, uint64_t ttl,
               bool leader, const std::vector<std::string>& endpoints, uint32_t seg_cnt);

    bool ChangeRole(uint32_t tid, uint32_t pid, bool leader);

    bool ChangeRole(uint32_t tid, uint32_t pid, bool leader, 
                    const std::vector<std::string>& endpoints);

    bool GetTaskStatus(::rtidb::api::TaskStatusResponse& response);               

    bool DeleteOPTask(const std::vector<uint64_t>& op_id_vec);

    int GetTableStatus(::rtidb::api::GetTableStatusResponse& response);
    int GetTableStatus(uint32_t tid, uint32_t pid,
                    ::rtidb::api::TableStatus& table_status);
    
    bool SetExpire(uint32_t tid, uint32_t pid, bool is_expire);
    bool SetTTLClock(uint32_t tid, uint32_t pid, uint64_t timestamp);
    
    void ShowTp();

private:
    std::string endpoint_;
    ::rtidb::RpcClient<::rtidb::api::TabletServer_Stub> client_;
    std::vector<uint64_t> percentile_;
};


}
}


#endif /* !RTIDB_TABLET_CLIENT_H */
