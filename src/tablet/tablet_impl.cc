//
// tablet_impl.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-01
//

#include "tablet/tablet_impl.h"
#include "tablet/file_sender.h"

#include "config.h"
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <gflags/gflags.h>
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#ifdef TCMALLOC_ENABLE 
#include "gperftools/malloc_extension.h"
#endif
#include "base/codec.h"
#include "base/strings.h"
#include "base/file_util.h"
#include "storage/segment.h"
#include "logging.h"
#include "timer.h"
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <thread>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;
using ::rtidb::storage::Table;
using ::rtidb::storage::DataBlock;

DECLARE_int32(gc_interval);
DECLARE_int32(disk_gc_interval);
DECLARE_int32(gc_pool_size);
DECLARE_int32(statdb_ttl);
DECLARE_uint32(scan_max_bytes_size);
DECLARE_uint32(scan_reserve_size);
DECLARE_double(mem_release_rate);
DECLARE_string(db_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_bool(binlog_notify_on_put);
DECLARE_int32(task_pool_size);
DECLARE_int32(io_pool_size);
DECLARE_int32(make_snapshot_time);
DECLARE_int32(make_snapshot_check_interval);
DECLARE_string(recycle_bin_root_path);
DECLARE_string(recycle_ssd_bin_root_path);
DECLARE_string(recycle_hdd_bin_root_path);
DECLARE_int32(make_snapshot_threshold_offset);

// cluster config
DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

DECLARE_int32(binlog_sync_to_disk_interval);
DECLARE_int32(binlog_delete_interval);
DECLARE_uint32(absolute_ttl_max);
DECLARE_uint32(latest_ttl_max);

namespace rtidb {
namespace tablet {
const static std::string SERVER_CONCURRENCY_KEY = "server";

TabletImpl::TabletImpl():tables_(),mu_(), gc_pool_(FLAGS_gc_pool_size),
    replicators_(), snapshots_(), zk_client_(NULL),
    keep_alive_pool_(1), task_pool_(FLAGS_task_pool_size),
    io_pool_(FLAGS_io_pool_size), snapshot_pool_(1), server_(NULL){}

TabletImpl::~TabletImpl() {
    task_pool_.Stop(true);
    keep_alive_pool_.Stop(true);
    gc_pool_.Stop(true);
    io_pool_.Stop(true);
    snapshot_pool_.Stop(true);
}

bool TabletImpl::Init() {
    std::lock_guard<std::mutex> lock(mu_);
    if (!FLAGS_zk_cluster.empty()) {
        zk_client_ = new ZkClient(FLAGS_zk_cluster, FLAGS_zk_session_timeout,
                FLAGS_endpoint, FLAGS_zk_root_path);
        bool ok = zk_client_->Init();
        if (!ok) {
            PDLOG(WARNING, "fail to init zookeeper with cluster %s", FLAGS_zk_cluster.c_str());
            return false;
        }
    }else {
        PDLOG(INFO, "zk cluster disabled");
    }
    bool ok = ::rtidb::base::MkdirRecur(FLAGS_recycle_bin_root_path);
    if (!ok) {
        PDLOG(WARNING, "fail to create recycle bin path %s", FLAGS_recycle_bin_root_path.c_str());
        return false;
    }
    if (FLAGS_make_snapshot_time < 0 || FLAGS_make_snapshot_time > 23) {
        PDLOG(WARNING, "make_snapshot_time[%d] is illegal.", FLAGS_make_snapshot_time);
        return false;
    }
    snapshot_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
#ifdef TCMALLOC_ENABLE
    MallocExtension* tcmalloc = MallocExtension::instance();
    tcmalloc->SetMemoryReleaseRate(FLAGS_mem_release_rate);
#endif 
    return true;
}

void TabletImpl::UpdateTTL(RpcController* ctrl,
        const ::rtidb::api::UpdateTTLRequest* request,
        ::rtidb::api::UpdateTTLResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);         
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());

    if (!table) {
        PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(),
                request->pid());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }

    if (request->type() != table->GetTTLType()) {
        response->set_code(112);
        response->set_msg("ttl type mismatch");
        PDLOG(WARNING, "ttl type mismatch. tid %u, pid %u", request->tid(), request->pid());
        return;
    }

    uint64_t ttl = request->value();
    if ((table->GetTTLType() == ::rtidb::api::kAbsoluteTime && ttl > FLAGS_absolute_ttl_max) ||
            (table->GetTTLType() == ::rtidb::api::kLatestTime && ttl > FLAGS_latest_ttl_max)) {
        response->set_code(132);
        uint32_t max_ttl = table->GetTTLType() == ::rtidb::api::kAbsoluteTime ? FLAGS_absolute_ttl_max : FLAGS_latest_ttl_max;
        response->set_msg("ttl is greater than conf value. max ttl is " + std::to_string(max_ttl));
        PDLOG(WARNING, "ttl is greater than conf value. ttl[%lu] ttl_type[%s] max ttl[%u]", 
                        ttl, ::rtidb::api::TTLType_Name(table->GetTTLType()).c_str(), max_ttl);
        return;
    }
    if (request->has_ts_name() && request->ts_name().size() > 0) {
        auto iter = table->GetTSMapping().find(request->ts_name());
        if (iter == table->GetTSMapping().end()) {
            PDLOG(WARNING, "ts name %s not found in table tid %u, pid %u", request->ts_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(137);
            response->set_msg("ts name not found");
            return;
        }
        table->SetTTL(iter->second, ttl);
        PDLOG(INFO, "update table #tid %d #pid %d ttl to %lu, ts_name %u",
                request->tid(), request->pid(), request->value(), request->ts_name().c_str());
    } else {
        table->SetTTL(ttl);
        PDLOG(INFO, "update table #tid %d #pid %d ttl to %lu", request->tid(), request->pid(), request->value());
    }
    response->set_code(0);
    response->set_msg("ok");
}

bool TabletImpl::RegisterZK() {
    if (!FLAGS_zk_cluster.empty()) {
        if (!zk_client_->Register(true)) {
            PDLOG(WARNING, "fail to register tablet with endpoint %s", FLAGS_endpoint.c_str());
            return false;
        }
        PDLOG(INFO, "tablet with endpoint %s register to zk cluster %s ok", FLAGS_endpoint.c_str(), FLAGS_zk_cluster.c_str());
        keep_alive_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&TabletImpl::CheckZkClient, this));
    }
    return true;
}

bool TabletImpl::CheckGetDone(::rtidb::api::GetType type, uint64_t ts,  uint64_t target_ts) {
    switch (type) {
        case rtidb::api::GetType::kSubKeyEq:
            if (ts == target_ts) {
                return true;
            }
            break;
        case rtidb::api::GetType::kSubKeyLe:
            if (ts <= target_ts) {
                return true;
            }
            break;
        case rtidb::api::GetType::kSubKeyLt:
            if (ts < target_ts) {
                return true;
            }
            break;
        case rtidb::api::GetType::kSubKeyGe:
            if (ts >= target_ts) {
                return true;
            }
            break;
        case rtidb::api::GetType::kSubKeyGt:
            if (ts > target_ts) {
                return true;
            }
    }
    return false;
}

int32_t TabletImpl::GetTimeIndex(uint64_t expire_ts,
                                 ::rtidb::storage::TableIterator* it,
                                 uint64_t st,
                                 const rtidb::api::GetType& st_type,
                                 uint64_t et,
                                 const rtidb::api::GetType& et_type,
                                 std::string* value,
                                 uint64_t* ts) {

    if (it == NULL || value == NULL || ts == NULL) {
        PDLOG(WARNING, "invalid args");
        return -1;
    }
    if (st_type == ::rtidb::api::kSubKeyEq
            && et_type == ::rtidb::api::kSubKeyEq
            && st != et) return -1;

    uint64_t end_time = std::max(et, expire_ts);
    ::rtidb::api::GetType real_et_type = et_type;
    if (et < expire_ts && et_type == ::rtidb::api::GetType::kSubKeyGt) {
        real_et_type = ::rtidb::api::GetType::kSubKeyGe; 
    }
    if (st > 0) {
        if (st < end_time) {
            PDLOG(WARNING, "invalid args for st %lu less than et %lu or expire time %lu", st, et, expire_ts);
            return -1;
        }
        switch (st_type) {
            case ::rtidb::api::GetType::kSubKeyEq:
                it->Seek(st);
                if (it->Valid() && it->GetKey() == st) {
                    ::rtidb::base::Slice it_value = it->GetValue();
                    value->assign(it_value.data(), it_value.size());
                    *ts = it->GetKey();
                    return 0;
                }else {
                    return 1;
                }
            case ::rtidb::api::GetType::kSubKeyLe:
                it->Seek(st);
                break;
            case ::rtidb::api::GetType::kSubKeyLt:
                //NOTE the st is million second
                it->Seek(st - 1);
                break;
            // adopt for legacy
            case ::rtidb::api::GetType::kSubKeyGt:
                it->SeekToFirst();
                if (it->Valid() && it->GetKey() > st) {
                    ::rtidb::base::Slice it_value = it->GetValue();
                    value->assign(it_value.data(), it_value.size());
                    *ts = it->GetKey();
                    return 0;
                }else {
                    return 1;
                }
            // adopt for legacy
            case ::rtidb::api::GetType::kSubKeyGe:
                it->SeekToFirst();
                if (it->Valid() && it->GetKey() >= st) {
                    ::rtidb::base::Slice it_value = it->GetValue();
                    value->assign(it_value.data(), it_value.size());
                    *ts = it->GetKey();
                    return 0;
                }else {
                    return 1;
                }
            default:
                PDLOG(WARNING, "invalid st type %s", ::rtidb::api::GetType_Name(st_type).c_str());
                return -2;
        }
    }else {
        it->SeekToFirst(); 
    }

    if (it->Valid()) {
        bool jump_out = false;
        switch(real_et_type) {
            case ::rtidb::api::GetType::kSubKeyEq:
                if (it->GetKey() != end_time) {
                    jump_out = true;
                }
                break;
            case ::rtidb::api::GetType::kSubKeyGt:
                if (it->GetKey() <= end_time) {
                    jump_out = true;
                }
                break;
            case ::rtidb::api::GetType::kSubKeyGe:
                if (it->GetKey() < end_time) {
                    jump_out = true;
                }
                break;
            default:
                PDLOG(WARNING, "invalid et type %s", ::rtidb::api::GetType_Name(et_type).c_str());
                return -2;
        }
        if (jump_out) return 1;
        ::rtidb::base::Slice it_value = it->GetValue();
        value->assign(it_value.data(), it_value.size());
        *ts = it->GetKey();
        return 0;
    }
    return 1;
}


int32_t TabletImpl::GetLatestIndex(uint64_t ttl,
                               ::rtidb::storage::TableIterator* it,
                               uint64_t st,
                               const rtidb::api::GetType& st_type,
                               uint64_t et,
                               const rtidb::api::GetType& et_type,
                               std::string* value,
                               uint64_t* ts) {

    if (it == NULL || value == NULL || ts == NULL) {
        PDLOG(WARNING, "invalid args");
        return -1;
    }

    if (st_type == ::rtidb::api::kSubKeyEq
        && et_type == ::rtidb::api::kSubKeyEq
        && st != et) return -1;

    if (st < et) {
        PDLOG(WARNING, "invalid args");
        return -1;
    }

    uint32_t it_count = 0;
    // go to start point
    it->SeekToFirst();
    if (st > 0) {
        while (it->Valid() && (it_count < ttl || ttl == 0)) {
            it_count++;
            bool jump_out = false;
            switch (st_type) {
                case ::rtidb::api::GetType::kSubKeyEq:
                    if (it->GetKey() <= st) {
                        if (it->GetKey() == st)  {
                            ::rtidb::base::Slice it_value = it->GetValue();
                            value->assign(it_value.data(), it_value.size());
                            *ts = it->GetKey();
                            return 0;
                        }else {
                            return 1;
                        }
                    }
                    break;
                case ::rtidb::api::GetType::kSubKeyLe:
                    if (it->GetKey() <= st) {
                        jump_out = true;
                    }
                    break;

                case ::rtidb::api::GetType::kSubKeyLt:
                    if (it->GetKey() < st) {
                        jump_out = true;
                    }
                    break;
                // adopt for the legacy
                case ::rtidb::api::GetType::kSubKeyGe:
                    if (it->GetKey() >= st) {
                        ::rtidb::base::Slice it_value = it->GetValue();
                        value->assign(it_value.data(), it_value.size());
                        *ts = it->GetKey();
                        return 0;
                    }else {
                        return 1;
                    }
                // adopt for the legacy
                case ::rtidb::api::GetType::kSubKeyGt:
                    if (it->GetKey() > st) {
                        ::rtidb::base::Slice it_value = it->GetValue();
                        value->assign(it_value.data(), it_value.size());
                        *ts = it->GetKey();
                        return 0;
                    }else {
                        return 1;
                    }
                default:
                    PDLOG(WARNING, "invalid st type %s", ::rtidb::api::GetType_Name(st_type).c_str());
                    return -2;
            }
            if (!jump_out) {
                it->Next();
            }else {
                break;
            }
        }
    }
    if (it->Valid() && (it_count < ttl || ttl == 0)) {
        it_count++;
        bool jump_out = false;
        switch(et_type) {
            case ::rtidb::api::GetType::kSubKeyEq:
                if (it->GetKey() != et) {
                    jump_out = true;
                }
                break;

            case ::rtidb::api::GetType::kSubKeyGt:
                if (it->GetKey() <= et) {
                    jump_out = true;
                }
                break;

            case ::rtidb::api::GetType::kSubKeyGe:
                if (it->GetKey() < et) {
                    jump_out = true;
                }
                break;

            default:
                PDLOG(WARNING, "invalid et type %s", ::rtidb::api::GetType_Name(et_type).c_str());
                return -2;
        }
        if (jump_out) return 1;
        ::rtidb::base::Slice it_value = it->GetValue();
        value->assign(it_value.data(), it_value.size());
        *ts = it->GetKey();
        return 0;
    }
    // not found
    return 1;
}


void TabletImpl::Get(RpcController* controller,
             const ::rtidb::api::GetRequest* request,
             ::rtidb::api::GetResponse* response,
             Closure* done) {
    brpc::ClosureGuard done_guard(done);         
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }

    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table is loading. tid %u, pid %u", 
                      request->tid(), request->pid());
        response->set_code(104);
        response->set_msg("table is loading");
        return;
    }

    if (table->GetStorageMode() != ::rtidb::common::StorageMode::kMemory) {
        GetFromDiskTable(table, request, response);
    }

    uint32_t index = 0;
    int ts_index = -1;
    if (request->has_idx_name() && request->idx_name().size() > 0) {
        std::map<std::string, uint32_t>::iterator iit = table->GetMapping().find(request->idx_name());
        if (iit == table->GetMapping().end()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(108);
            response->set_msg("idx name not found");
            return;
        }
        index = iit->second;
    }
    if (request->has_ts_name() && request->ts_name().size() > 0) {
        auto iter = table->GetTSMapping().find(request->ts_name());
        if (iter == table->GetTSMapping().end()) {
            PDLOG(WARNING, "ts name %s not found in table tid %u, pid %u", request->ts_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(137);
            response->set_msg("ts name not found");
            return;
        }
        ts_index = iter->second;
    }    

    ::rtidb::storage::Ticket ticket;
    ::rtidb::storage::TableIterator* it = NULL;
    if (ts_index >= 0) {
        it = table->NewIterator(index, ts_index, request->key(), ticket);
    } else {
        it = table->NewIterator(index, request->key(), ticket);
    }

    if (it == NULL) {
        response->set_code(109);
        response->set_msg("key not found");
        return;
    }
    uint64_t ttl = ts_index < 0 ? table->GetTTL(index) : table->GetTTL(index, ts_index);
    std::string* value = response->mutable_value(); 
    uint64_t ts = 0;
    int32_t code = 0;
    switch(table->GetTTLType()) {
        case ::rtidb::api::TTLType::kLatestTime:
            code = GetLatestIndex(ttl, it,
                        request->ts(), request->type(),
                        request->et(), request->et_type(),
                        value, &ts);
            break;

        default:
            uint64_t expire_ts = table->GetExpireTime(ttl);
            code = GetTimeIndex(expire_ts, it,
                    request->ts(), request->type(),
                    request->et(), request->et_type(),
                    value, &ts);
            break;
    }
    delete it;
    response->set_ts(ts);
    response->set_code(code);
    switch(code) {
        case 1:
            response->set_code(109);
            response->set_msg("key not found");
            return;
        case 0:
            return;
        case -1:
            response->set_msg("invalid args");
            response->set_code(307);
            return;
        case -2:
            response->set_code(307);
            response->set_msg("st/et sub key type is invalid");
            return;
        default:
            return;
    }
}

void TabletImpl::GetFromDiskTable(std::shared_ptr<Table> disk_table,
             const ::rtidb::api::GetRequest* request,
             ::rtidb::api::GetResponse* response) {
    ::rtidb::storage::TableIterator* it = NULL;
    ::rtidb::storage::Ticket ticket;
    if (request->has_idx_name() && request->idx_name().size() > 0) {
        std::map<std::string, uint32_t>::iterator iit = disk_table->GetMapping().find(request->idx_name());
        if (iit == disk_table->GetMapping().end()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(108);
            response->set_msg("idx name not found");
            return;
        }
        it = disk_table->NewIterator(iit->second, request->key(), ticket);
    } else {
        it = disk_table->NewIterator(request->key(), ticket);
    }
    ::rtidb::api::GetType get_type = ::rtidb::api::GetType::kSubKeyEq;
    if (request->has_type()) {
        get_type = request->type();
    }
    bool has_found = true;
    // filter with time
    if (request->ts() > 0) {
        if (disk_table->GetTTLType() == ::rtidb::api::TTLType::kLatestTime) {
            uint64_t keep_cnt = disk_table->GetTTL();
            it->SeekToFirst();
            while (it->Valid()) {
                if (CheckGetDone(get_type, it->GetKey(), request->ts())) {
                    break;
                }
                keep_cnt--;
                if (keep_cnt == 0) {
                    has_found = false;
                    break;
                }
                it->Next();
            }
        } else if (request->ts() > disk_table->GetExpireTime(disk_table->GetTTL())) {
            it->Seek(request->ts());
            if (it->Valid() && it->GetKey() != request->ts()) {
                has_found = false;
            }
        }

    } else {
        it->SeekToFirst();
        if (it->Valid() && disk_table->GetTTLType() == ::rtidb::api::TTLType::kAbsoluteTime) {
            if (it->GetKey() <= disk_table->GetExpireTime(disk_table->GetTTL())) {
                has_found = false;
            }
        }
    }
    if (it->Valid() && has_found) {
        response->set_code(0);
        response->set_msg("ok");
        response->set_key(request->key());
        response->set_ts(it->GetKey());
        response->set_value(it->GetValue().data(), it->GetValue().size());
    } else {
        response->set_code(109);
        response->set_msg("key not found");
        PDLOG(DEBUG, "not found key %s ts %lu ", request->key().c_str(),
                request->ts());
    }        
    delete it;
}

void TabletImpl::Put(RpcController* controller,
        const ::rtidb::api::PutRequest* request,
        ::rtidb::api::PutResponse* response,
        Closure* done) {
    if (request->time() == 0 && request->ts_dimensions_size() == 0) {
        response->set_code(114);
        response->set_msg("ts must be greater than zero");
        done->Run();
        return;
    }

    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(100);
        response->set_msg("table is not exist");
        done->Run();
        return;
    }    
    if (!table->IsLeader()) {
        response->set_code(103);
        response->set_msg("table is follower");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table is loading. tid %u, pid %u", 
                      request->tid(), request->pid());
        response->set_code(104);
        response->set_msg("table is loading");
        done->Run();
        return;
    }
    bool ok = false;
    if (request->dimensions_size() > 0) {
        int32_t ret_code = CheckDimessionPut(request, table->GetIdxCnt());
        if (ret_code != 0) {
            response->set_code(115);
            response->set_msg("invalid dimension parameter");
            done->Run();
            return;
        }
        if (request->ts_dimensions_size() > 0) {
            ok = table->Put(request->dimensions(), request->ts_dimensions(), request->value());
        } else {
            ok = table->Put(request->time(), request->value(), request->dimensions());
        }
    } else {
        ok = table->Put(request->pk(), 
                   request->time(), 
                   request->value().c_str(),
                   request->value().size());
    }
    if (!ok) {
        response->set_code(116);
        response->set_msg("put failed");
        done->Run();
        return;
    }
    response->set_code(0);
    std::shared_ptr<LogReplicator> replicator;
    do {
        replicator = GetReplicator(request->tid(), request->pid());
        if (!replicator) {
            PDLOG(WARNING, "fail to find table tid %u pid %u leader's log replicator", request->tid(),
                    request->pid());
            break;
        }
        ::rtidb::api::LogEntry entry;
        entry.set_pk(request->pk());
        entry.set_ts(request->time());
        entry.set_value(request->value());
        entry.set_term(replicator->GetLeaderTerm());
        if (request->dimensions_size() > 0) {
            entry.mutable_dimensions()->CopyFrom(request->dimensions());
        }
        if (request->ts_dimensions_size() > 0) {
            entry.mutable_ts_dimensions()->CopyFrom(request->ts_dimensions());
        }
        replicator->AppendEntry(entry);
    } while(false);
    done->Run();
    if (replicator) {
        if (FLAGS_binlog_notify_on_put) {
            replicator->Notify(); 
        }
    }
}

int TabletImpl::CheckTableMeta(const rtidb::api::TableMeta* table_meta, std::string& msg) {
    msg.clear();
    if (table_meta->name().size() <= 0) {
        msg = "table name is empty";
        return -1;
    }
    if (table_meta->tid() <= 0) {
        msg = "tid is zero";
        return -1;
    }
    ::rtidb::api::TTLType type = table_meta->ttl_type();
    uint64_t ttl = table_meta->ttl();
    if ((type == ::rtidb::api::kAbsoluteTime && ttl > FLAGS_absolute_ttl_max) ||
            (type == ::rtidb::api::kLatestTime && ttl > FLAGS_latest_ttl_max)) {
        uint32_t max_ttl = type == ::rtidb::api::kAbsoluteTime ? FLAGS_absolute_ttl_max : FLAGS_latest_ttl_max;
        msg = "ttl is greater than conf value. max ttl is " + std::to_string(max_ttl);
        return -1;
    }
    std::map<std::string, std::string> column_map;
    std::set<std::string> ts_set;
    if (table_meta->column_desc_size() > 0) {
        for (const auto& column_desc : table_meta->column_desc()) {
            if (column_map.find(column_desc.name()) != column_map.end()) {
                msg = "has repeated column name " + column_desc.name();
                return -1;
            }
            if (column_desc.is_ts_col()) {
                if (column_desc.add_ts_idx()) {
                    msg = "can not set add_ts_idx and is_ts_col together. column name " + column_desc.name();
                    return -1;
                }
                if (column_desc.type() != "int64" && column_desc.type() != "uint64" && 
                        column_desc.type() != "timestamp") {
                    msg = "ttl column type must be int64, uint64, timestamp";
                    return -1;
                }
                if (column_desc.has_ttl()) {
                    ttl = column_desc.ttl();
                    if ((type == ::rtidb::api::kAbsoluteTime && ttl > FLAGS_absolute_ttl_max) ||
                            (type == ::rtidb::api::kLatestTime && ttl > FLAGS_latest_ttl_max)) {
                        uint32_t max_ttl = type == ::rtidb::api::kAbsoluteTime ? FLAGS_absolute_ttl_max : FLAGS_latest_ttl_max;
                        msg = "ttl is greater than conf value. max ttl is " + std::to_string(max_ttl);
                        return -1;
                    }
                }
                ts_set.insert(column_desc.name());
            }
            if (column_desc.add_ts_idx() && ((column_desc.type() == "float") || (column_desc.type() == "double"))) {
                msg = "float or double column can not be index";
                return -1;
            }
            column_map.insert(std::make_pair(column_desc.name(), column_desc.type()));
        }
    }
    std::set<std::string> index_set;
    if (table_meta->column_key_size() > 0) {
        for (const auto& column_key : table_meta->column_key()) {
            if (index_set.find(column_key.index_name()) != index_set.end()) {
                msg = "has repeated index name " + column_key.index_name();
                return -1;
            }
            index_set.insert(column_key.index_name());
            bool has_col = false;
            for (const auto& column_name : column_key.col_name()) {
                has_col = true;
                auto iter = column_map.find(column_name);
                if (iter == column_map.end()) {
                    msg = "not found column name " + column_name;
                    return -1;
                }
                if ((iter->second == "float") || (iter->second == "double")) {
                    msg = "float or double column can not be index" + column_name;
                    return -1;
                }
                if (ts_set.find(column_name) != ts_set.end()) {
                    msg = "column name in column key can not set ts col. column name " + column_name;
                    return -1;
                }
            }
            if (!has_col) {
                auto iter = column_map.find(column_key.index_name());
                if (iter == column_map.end()) {
                    msg = "index must member of columns when column key col name is empty";
                    return -1;
                } else {
                    if ((iter->second == "float") || (iter->second == "double")) {
                        msg = "indxe name column type can not float or column";
                        return -1;
                    }
                }
            }
            std::set<std::string> ts_name_set;
            for (const auto& ts_name : column_key.ts_name()) {
                if (ts_set.find(ts_name) == ts_set.end()) {
                    msg = "not found ts_name " + ts_name;
                    return -1;
                }
                if (ts_name_set.find(ts_name) != ts_name_set.end()) {
                    msg = "has repeated ts_name " + ts_name;
                    return -1;
                }
                ts_name_set.insert(ts_name);
            }
            if (ts_set.size() > 1 && column_key.ts_name_size() == 0) {
                msg = "ts column num more than one, must set ts name";
                return -1;
            }
        }
    } else if (ts_set.size() > 1) {
        msg = "column_key should be set when has two or more ts columns";
        return -1;
    }
    return 0;
}

int32_t TabletImpl::ScanTimeIndex(uint64_t expire_ts, 
                                 ::rtidb::storage::TableIterator* it,
                                 uint32_t limit,
                                 uint64_t st,
                                 const rtidb::api::GetType& st_type,
                                 uint64_t et,
                                 const rtidb::api::GetType& et_type,
                                 std::string* pairs,
                                 uint32_t* count,
                                 bool remove_duplicated_record) {

    if (it == NULL || pairs == NULL || count == NULL) {
        PDLOG(WARNING, "invalid args");
        return -1;
    }

    uint64_t end_time = std::max(et, expire_ts);
    rtidb::api::GetType real_type = et_type;
    if (et < expire_ts && et_type == ::rtidb::api::GetType::kSubKeyGt) {
        real_type = ::rtidb::api::GetType::kSubKeyGe;
    }
    if (st > 0) {
        if (st < end_time) {
            PDLOG(WARNING, "invalid args for st %lu less than et %lu or expire time %lu", st, et, expire_ts);
            return -1;
        }
        switch (st_type) {
            case ::rtidb::api::GetType::kSubKeyEq:
            case ::rtidb::api::GetType::kSubKeyLe:
                it->Seek(st);
                break;
            case ::rtidb::api::GetType::kSubKeyLt:
                //NOTE the st is million second
                it->Seek(st - 1);
                break;
            default:
                PDLOG(WARNING, "invalid st type %s", ::rtidb::api::GetType_Name(st_type).c_str());
                return -2;
        }
    }else {
        it->SeekToFirst(); 
    }

    uint64_t last_time = 0;
    std::vector<std::pair<uint64_t, ::rtidb::base::Slice>> tmp;
    tmp.reserve(FLAGS_scan_reserve_size);

    uint32_t total_block_size = 0;
    while (it->Valid()) {
        if (limit > 0 && tmp.size() >= limit) {
            break;
        }
        // skip duplicate record 
        if (remove_duplicated_record 
            && tmp.size() > 0 && last_time == it->GetKey()) {
            it->Next();
            continue;
        }
        bool jump_out = false;
        switch(real_type) {
            case ::rtidb::api::GetType::kSubKeyEq:
                if (it->GetKey() != end_time) {
                    jump_out = true;
                }
                break;
            case ::rtidb::api::GetType::kSubKeyGt:
                if (it->GetKey() <= end_time) {
                    jump_out = true;
                }
                break;
            case ::rtidb::api::GetType::kSubKeyGe:
                if (it->GetKey() < end_time) {
                    jump_out = true;
                }
                break;
            default:
                PDLOG(WARNING, "invalid et type %s", ::rtidb::api::GetType_Name(et_type).c_str());
                return -2;
        }
        if (jump_out) break;
        last_time = it->GetKey();
        ::rtidb::base::Slice it_value = it->GetValue();
        tmp.push_back(std::make_pair(it->GetKey(), it_value));
        total_block_size += it_value.size();
        if (total_block_size > FLAGS_scan_max_bytes_size) {
            PDLOG(WARNING, "reach the max byte size");
            return -3;
        }
        it->Next();
    }
    int32_t ok = ::rtidb::base::EncodeRows(tmp, total_block_size, pairs);
    if (ok == -1) {
        PDLOG(WARNING, "fail to encode rows");
        return -4;
    }
    *count = tmp.size();
    return 0;
}

int32_t TabletImpl::ScanLatestIndex(uint64_t ttl,
                                    ::rtidb::storage::TableIterator* it,
                                    uint32_t limit,
                                    uint64_t st,
                                    const rtidb::api::GetType& st_type,
                                    uint64_t et,
                                    const rtidb::api::GetType& et_type,
                                    std::string* pairs,
                                    uint32_t* count) {
    if (it == NULL || pairs == NULL || count == NULL) {
        PDLOG(WARNING, "invalid args");
        return -1;
    }
    uint32_t it_count = 0;
    // go to start point
    it->SeekToFirst();
    if (st > 0) {
        while (it->Valid() && (it_count < ttl || ttl == 0)) {
            it_count++;
            bool jump_out = false;
            switch (st_type) {
                case ::rtidb::api::GetType::kSubKeyEq:
                case ::rtidb::api::GetType::kSubKeyLe:
                    if (it->GetKey() <= st) {
                        jump_out = true;
                    }
                    break;

                case ::rtidb::api::GetType::kSubKeyLt:
                    if (it->GetKey() < st) {
                        jump_out = true;
                    }
                    break;

                default:
                    PDLOG(WARNING, "invalid st type %s", ::rtidb::api::GetType_Name(st_type).c_str());
                    return -2;
            }
            if (!jump_out) {
                it->Next();
            }else {
                break;
            }
        }
    }
    std::vector<std::pair<uint64_t, ::rtidb::base::Slice>> tmp;
    tmp.reserve(FLAGS_scan_reserve_size);
    uint32_t total_block_size = 0;
    while (it->Valid() && (it_count < ttl || ttl == 0)) {
        it_count++;
        if (limit > 0 && tmp.size() >= limit) break;
        bool jump_out = false;
        switch(et_type) {
            case ::rtidb::api::GetType::kSubKeyEq:
                if (it->GetKey() != et) {
                    jump_out = true;
                }
                break;

            case ::rtidb::api::GetType::kSubKeyGt:
                if (it->GetKey() <= et) {
                    jump_out = true;
                }
                break;

            case ::rtidb::api::GetType::kSubKeyGe:
                if (it->GetKey() < et) {
                    jump_out = true;
                }
                break;

            default:
                PDLOG(WARNING, "invalid et type %s", ::rtidb::api::GetType_Name(et_type).c_str());
                return -2;
        }
        if (jump_out) break;
        ::rtidb::base::Slice it_value = it->GetValue();
        tmp.push_back(std::make_pair(it->GetKey(), it_value));
        total_block_size += it_value.size();
        it->Next();
        if (total_block_size > FLAGS_scan_max_bytes_size) {
            PDLOG(WARNING, "reach the max byte size");
            return -3;
        }
    }
    int32_t ok = ::rtidb::base::EncodeRows(tmp, total_block_size, pairs);
    if (ok == -1) {
        PDLOG(WARNING, "fail to encode rows");
        return -4;
    }
    *count = tmp.size();
    return 0;
}

void TabletImpl::Scan(RpcController* controller,
              const ::rtidb::api::ScanRequest* request,
              ::rtidb::api::ScanResponse* response,
              Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (request->st() < request->et()) {
        response->set_code(117);
        response->set_msg("starttime less than endtime");
        return;
    }
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table is loading. tid %u, pid %u", 
                      request->tid(), request->pid());
        response->set_code(104);
        response->set_msg("table is loading");
        return;
    }
    if (table->GetTTLType() == ::rtidb::api::TTLType::kLatestTime) {
        response->set_code(112);
        response->set_msg("table ttl type is kLatestTime, cannot scan");
        return;
    }
    if (table->GetStorageMode() != ::rtidb::common::StorageMode::kMemory) {
        ScanFromDiskTable(table, request, response);
        return;
    }
    uint32_t index = 0;
    int ts_index = -1;
    if (request->has_idx_name() && request->idx_name().size() > 0) {
        std::map<std::string, uint32_t>::iterator iit = table->GetMapping().find(request->idx_name());
        if (iit == table->GetMapping().end()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(108);
            response->set_msg("idx name not found");
            return;
        }
        index = iit->second;
    }
    if (request->has_ts_name() && request->ts_name().size() > 0) {
        auto iter = table->GetTSMapping().find(request->ts_name());
        if (iter == table->GetTSMapping().end()) {
            PDLOG(WARNING, "ts name %s not found in table tid %u, pid %u", request->ts_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(137);
            response->set_msg("ts name not found");
            return;
        }
        ts_index = iter->second;
    }    

    // Use seek to process scan request
    // the first seek to find the total size to copy
    ::rtidb::storage::Ticket ticket;
    ::rtidb::storage::TableIterator* it = NULL;
    if (ts_index >= 0) {
        it = table->NewIterator(index, ts_index, request->pk(), ticket);
    } else {
        it = table->NewIterator(index, request->pk(), ticket);
    }
    if (it == NULL) {
        response->set_code(109);
        response->set_msg("key not found");
        return;
    }
    uint64_t ttl = ts_index < 0 ? table->GetTTL(index) : table->GetTTL(index, ts_index);
    std::string* pairs = response->mutable_pairs(); 
    uint32_t count = 0;
    int32_t code = 0;
    switch(table->GetTTLType()) {
        case ::rtidb::api::TTLType::kLatestTime:
            code = ScanLatestIndex(ttl, it, request->limit(),
                        request->st(), request->st_type(),
                        request->et(), request->et_type(),
                        pairs, &count);
            break;

        default:
            bool remove_duplicated_record = false;
            if (request->has_enable_remove_duplicated_record()) {
                remove_duplicated_record = request->enable_remove_duplicated_record();
            }
            uint64_t expire_ts = table->GetExpireTime(ttl);
            code = ScanTimeIndex(expire_ts, it, request->limit(),
                    request->st(), request->st_type(),
                    request->et(), request->et_type(),
                    pairs, &count, remove_duplicated_record);
            break;
    }
    delete it;
    response->set_code(code);
    response->set_count(count);
    switch(code) {
        case 0:
            return;
        case -1:
            response->set_msg("invalid args");
            response->set_code(307);
            return;
        case -2:
            response->set_msg("st/et sub key type is invalid");
            response->set_code(307);
            return;
        case -3:
            response->set_code(118);
            response->set_msg("reach the max scan byte size");
            return;
        case -4:
            response->set_msg("fail to encode data rows");
            response->set_code(322);
            return;
        default:
            return;
    }
}

void TabletImpl::ScanFromDiskTable(std::shared_ptr<Table> disk_table,
              const ::rtidb::api::ScanRequest* request,
              ::rtidb::api::ScanResponse* response) {
    if (disk_table->GetTTLType() == ::rtidb::api::TTLType::kLatestTime) {
        response->set_code(112);
        response->set_msg("table ttl type is kLatestTime, cannot scan");
        return;
    }
    ::rtidb::storage::TableIterator* it = NULL;
    ::rtidb::storage::Ticket ticket;
    if (request->has_idx_name() && request->idx_name().size() > 0) {
        std::map<std::string, uint32_t>::iterator iit = disk_table->GetMapping().find(request->idx_name());
        if (iit == disk_table->GetMapping().end()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(108);
            response->set_msg("idx name not found");
            return;
        }
        it = disk_table->NewIterator(iit->second, request->pk(), ticket);
    } else {
        it = disk_table->NewIterator(request->pk(), ticket);
    }
    if (request->st() == 0) {
        it->SeekToFirst();
    } else {
        it->Seek(request->st());
    }
    std::vector<std::pair<uint64_t, rtidb::base::Slice> > tmp;
    // reduce the times of memcpy in vector
    tmp.reserve(FLAGS_scan_reserve_size);
    uint32_t total_block_size = 0;
    uint64_t end_time = request->et();
    uint32_t scount = 0;
    uint64_t expire_time = disk_table->GetExpireTime(disk_table->GetTTL());
    end_time = std::max(end_time, expire_time);
    PDLOG(DEBUG, "scan pk %s st %lu end_time %lu expire_time %lu", 
                  request->pk().c_str(), request->st(), end_time, expire_time);
    uint32_t limit = 0;
    if (request->has_limit()) {
        limit = request->limit();
    }
    while (it->Valid()) {
        scount ++;
        if (it->GetKey() <= end_time) {
            break;
        }
        rtidb::base::Slice value = it->GetValue();
        tmp.push_back(std::make_pair(it->GetKey(), value));
        total_block_size += value.size();
        it->Next();
        if (limit > 0 && scount >= limit) {
            break;
        }
        // check reach the max bytes size
        if (total_block_size > FLAGS_scan_max_bytes_size) {
            response->set_code(118);
            response->set_msg("reache the scan max bytes size " + ::rtidb::base::HumanReadableString(total_block_size));
            delete it;
            return;
        }
    }
    delete it;
    uint32_t total_size = tmp.size() * (8+4) + total_block_size;
    std::string* pairs = response->mutable_pairs();
    if (tmp.size() <= 0) {
        pairs->resize(0);
    }else {
        pairs->resize(total_size);
    }
    char* rbuffer = reinterpret_cast<char*>(& ((*pairs)[0]));
    uint32_t offset = 0;
    for (const auto& pair : tmp) {
        ::rtidb::base::Encode(pair.first, pair.second.data(), pair.second.size(), rbuffer, offset);
        offset += (4 + 8 + pair.second.size());
    }
    response->set_code(0);
    response->set_msg("ok");
    response->set_count(tmp.size());
}

void TabletImpl::Count(RpcController* controller,
              const ::rtidb::api::CountRequest* request,
              ::rtidb::api::CountResponse* response,
              Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table is loading. tid %u, pid %u", 
                      request->tid(), request->pid());
        response->set_code(104);
        response->set_msg("table is loading");
        return;
    }
    if (table->GetStorageMode() != ::rtidb::common::StorageMode::kMemory) {
        PDLOG(WARNING, "table is disk table. tid %u, pid %u", 
                      request->tid(), request->pid());
        response->set_code(307);
        response->set_msg("disk table is not support count");
        return;
    }
    uint32_t index = 0;
    int ts_index = -1;
    if (request->has_idx_name() && request->idx_name().size() > 0) {
        std::map<std::string, uint32_t>::iterator iit = table->GetMapping().find(request->idx_name());
        if (iit == table->GetMapping().end()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(108);
            response->set_msg("idx name not found");
            return;
        }
        index = iit->second;
    }
    if (request->has_ts_name() && request->ts_name().size() > 0) {
        auto iter = table->GetTSMapping().find(request->ts_name());
        if (iter == table->GetTSMapping().end()) {
            PDLOG(WARNING, "ts name %s not found in table tid %u, pid %u", request->ts_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(137);
            response->set_msg("ts name not found");
            return;
        }
        ts_index = iter->second;
    }    
    if (!request->filter_expired_data()) {
        MemTable* mem_table = dynamic_cast<MemTable*>(table.get());
        if (mem_table != NULL) {
            uint64_t count = 0;
            if (ts_index >= 0) {
                if (mem_table->GetCount(index, ts_index, request->key(), count) < 0) {
                    count = 0;
                }
            } else {
                if (mem_table->GetCount(index, request->key(), count) < 0) {
                    count = 0;
                }
            }
            response->set_code(0);
            response->set_msg("ok");
            response->set_count(count);
            return;
        }
    }
    ::rtidb::storage::Ticket ticket;
    ::rtidb::storage::TableIterator* it = NULL;
    if (ts_index >= 0) {
        it = table->NewIterator(index, ts_index, request->key(), ticket);
    } else {
        it = table->NewIterator(index, request->key(), ticket);
    }
    if (it == NULL) {
        response->set_code(109);
        response->set_msg("key not found");
        return;
    }
    it->SeekToFirst();
    uint64_t ttl = ts_index < 0 ? table->GetTTL(index) : table->GetTTL(index, ts_index);
    uint64_t end_time = table->GetExpireTime(ttl);
    PDLOG(DEBUG, "end_time %lu", end_time);

    bool remove_duplicated_record = false;
    if (request->has_enable_remove_duplicated_record()) {
      remove_duplicated_record = request->enable_remove_duplicated_record();
    }
    uint64_t scount = 0;
    uint64_t last_time = 0;
    while (it->Valid()) {
        if (table->GetTTLType() == ::rtidb::api::TTLType::kLatestTime) {
            if (scount >= ttl) {
                break;
            }
        } else {
            if (it->GetKey() <= end_time) {
                break;
            }
        }
        if (remove_duplicated_record && last_time == it->GetKey()) {
          PDLOG(DEBUG, "filter duplicated ts record %lu", last_time);
          it->Next();
          continue;
        }
        last_time = it->GetKey();
        scount ++;
        it->Next();
    }
    delete it;
    response->set_code(0);
    response->set_msg("ok");
    response->set_count(scount);
}

void TabletImpl::Traverse(RpcController* controller,
              const ::rtidb::api::TraverseRequest* request,
              ::rtidb::api::TraverseResponse* response,
              Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table is loading. tid %u, pid %u", 
                      request->tid(), request->pid());
        response->set_code(104);
        response->set_msg("table is loading");
        return;
    }
    uint32_t index = 0;
    int ts_index = -1;
    if (request->has_idx_name() && request->idx_name().size() > 0) {
        std::map<std::string, uint32_t>::iterator iit = table->GetMapping().find(request->idx_name());
        if (iit == table->GetMapping().end()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(108);
            response->set_msg("idx name not found");
            return;
        }
        index = iit->second;
    }
    if (request->has_ts_name() && request->ts_name().size() > 0) {
        auto iter = table->GetTSMapping().find(request->ts_name());
        if (iter == table->GetTSMapping().end()) {
            PDLOG(WARNING, "ts name %s not found in table tid %u, pid %u", request->ts_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(137);
            response->set_msg("ts name not found");
            return;
        }
        ts_index = iter->second;
    }    
    ::rtidb::storage::TableIterator* it = NULL;
    if (ts_index >= 0) {
        it = table->NewTraverseIterator(index, ts_index);
    } else {
        it = table->NewTraverseIterator(index);
    }
    uint64_t last_time = 0;
    std::string last_pk;
    if (request->has_pk() && request->pk().size() > 0) {
        PDLOG(DEBUG, "tid %u, pid %u seek pk %s ts %lu", 
                    request->tid(), request->pid(), request->pk().c_str(), request->ts());
        it->Seek(request->pk(), request->ts());
        last_pk = request->pk();
        last_time = request->ts();
    } else {
        PDLOG(DEBUG, "tid %u, pid %u seek to first", request->tid(), request->pid());
        it->SeekToFirst();
    }
    std::map<std::string, std::vector<std::pair<uint64_t, rtidb::base::Slice>>> value_map;
    uint32_t total_block_size = 0;
    bool remove_duplicated_record = false;
    if (request->has_enable_remove_duplicated_record()) {
        remove_duplicated_record = request->enable_remove_duplicated_record();
    }
    uint32_t scount = 0;
    while (it->Valid()) {
        if (request->limit() > 0 && scount > request->limit() - 1) {
            PDLOG(DEBUG, "reache the limit %u ", request->limit());
            break;
        }
        PDLOG(DEBUG, "traverse pk %s ts %lu", it->GetPK().c_str(), it->GetKey());
        // skip duplicate record
        if (remove_duplicated_record && last_time == it->GetKey() && last_pk == it->GetPK()) {
            PDLOG(DEBUG, "filter duplicate record for key %s with ts %lu", last_pk.c_str(), last_time);
            it->Next();
            continue;
        }
        last_pk = it->GetPK();
        last_time = it->GetKey();
        if (value_map.find(last_pk) == value_map.end()) {
            value_map.insert(std::make_pair(last_pk, std::vector<std::pair<uint64_t, rtidb::base::Slice>>()));
            value_map[last_pk].reserve(request->limit());
        }
        rtidb::base::Slice value = it->GetValue();
        value_map[last_pk].push_back(std::make_pair(it->GetKey(), value));
        total_block_size += last_pk.length() + value.size();
        it->Next();
        scount ++;
    }
    delete it;
    uint32_t total_size = scount * (8+4+4) + total_block_size;
    std::string* pairs = response->mutable_pairs();
    if (scount <= 0) {
        pairs->resize(0);
    } else {
        pairs->resize(total_size);
    }
    char* rbuffer = reinterpret_cast<char*>(& ((*pairs)[0]));
    uint32_t offset = 0;
    for (const auto& kv : value_map) {
        for (const auto& pair : kv.second) {
            PDLOG(DEBUG, "encode pk %s ts %lu value %s size %u", kv.first.c_str(), pair.first, pair.second.data(), pair.second.size());
            ::rtidb::base::EncodeFull(kv.first, pair.first, pair.second.data(), pair.second.size(), rbuffer, offset);
            offset += (4 + 4 + 8 + kv.first.length() + pair.second.size());
        }
    }
    PDLOG(DEBUG, "traverse count %d. last_pk %s last_time %lu", scount, last_pk.c_str(), last_time);
    response->set_code(0);
    response->set_count(scount);
    response->set_pk(last_pk);
    response->set_ts(last_time);
}

void TabletImpl::Delete(RpcController* controller,
              const ::rtidb::api::DeleteRequest* request,
              ::rtidb::api::GeneralResponse* response,
              Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(DEBUG, "table is not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }    
    if (!table->IsLeader()) {
        PDLOG(DEBUG, "table is follower. tid %u, pid %u", request->tid(),
                request->pid());
        response->set_code(103);
        response->set_msg("table is follower");
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table is loading. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(104);
        response->set_msg("table is loading");
        return;
    }
    uint32_t idx = 0;
    if (request->has_idx_name() && request->idx_name().size() > 0) {
        std::map<std::string, uint32_t>::iterator iit = table->GetMapping().find(request->idx_name());
        if (iit == table->GetMapping().end()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(108);
            response->set_msg("idx name not found");
            return;
        }
        idx = iit->second;
    }
    if (table->Delete(request->key(), idx)) {
        response->set_code(0);
        response->set_msg("ok");
        PDLOG(DEBUG, "delete ok. tid %u, pid %u, key %s", request->tid(), request->pid(), request->key().c_str());
    } else {
        response->set_code(136);
        response->set_msg("delete failed");
        return;
    }
    if (table->GetStorageMode() == ::rtidb::common::StorageMode::kMemory) {
        std::shared_ptr<LogReplicator> replicator;
        do {
            replicator = GetReplicator(request->tid(), request->pid());
            if (!replicator) {
                PDLOG(WARNING, "fail to find table tid %u pid %u leader's log replicator", request->tid(),
                        request->pid());
                break;
            }
            ::rtidb::api::LogEntry entry;
            entry.set_term(replicator->GetLeaderTerm());
            entry.set_method_type(::rtidb::api::MethodType::kDelete);
            ::rtidb::api::Dimension* dimension = entry.add_dimensions();
            dimension->set_key(request->key());
            dimension->set_idx(idx);
            replicator->AppendEntry(entry);
        } while(false);
        if (replicator && FLAGS_binlog_notify_on_put) {
            replicator->Notify(); 
        }
    }
    return;
}

void TabletImpl::ChangeRole(RpcController* controller, 
            const ::rtidb::api::ChangeRoleRequest* request,
            ::rtidb::api::ChangeRoleResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    if (table->GetTableStat() != ::rtidb::storage::kNormal) {
        PDLOG(WARNING, "table state[%u] can not change role. tid[%u] pid[%u]", 
                    table->GetTableStat(), tid, pid);
        response->set_code(105);
        response->set_msg("table status is not kNormal");
        return;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        response->set_code(110);
        response->set_msg("replicator is not exist");
        return;
    }
    bool is_leader = false;
    if (request->mode() == ::rtidb::api::TableMode::kTableLeader) {
        is_leader = true;
    }
    std::vector<std::string> vec;
    for (int idx = 0; idx < request->replicas_size(); idx++) {
        vec.push_back(request->replicas(idx).c_str());
    }
    if (is_leader) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (table->IsLeader()) {
                PDLOG(WARNING, "table is leader. tid[%u] pid[%u]", tid, pid);
                response->set_code(102);
                response->set_msg("table is leader");
                return ;
            }
            PDLOG(INFO, "change to leader. tid[%u] pid[%u] term[%lu]", tid, pid, request->term());
            table->SetLeader(true);
            replicator->SetRole(ReplicatorRole::kLeaderNode);
            if (!FLAGS_zk_cluster.empty()) {
                replicator->SetLeaderTerm(request->term());
            }
        }
        if (replicator->AddReplicateNode(vec) < 0) {
            PDLOG(WARNING,"add replicator failed. tid[%u] pid[%u]", tid, pid);
        }
    } else {
        std::lock_guard<std::mutex> lock(mu_);
        if (!table->IsLeader()) {
            PDLOG(WARNING, "table is follower. tid[%u] pid[%u]", tid, pid);
            response->set_code(0);
            response->set_msg("table is follower");
            return;
        }
        replicator->DelAllReplicateNode();
        replicator->SetRole(ReplicatorRole::kFollowerNode);
        table->SetLeader(false);
        PDLOG(INFO, "change to follower. tid[%u] pid[%u]", tid, pid);
    }
    response->set_code(0);
    response->set_msg("ok");
}

void TabletImpl::AddReplica(RpcController* controller, 
            const ::rtidb::api::ReplicaRequest* request,
            ::rtidb::api::AddReplicaResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);        
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kAddReplica, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    do {
        if (!table) {
            PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(),
                    request->pid());
            response->set_code(100);
            response->set_msg("table is not exist");
            break;
        }
        if (!table->IsLeader()) {
            PDLOG(WARNING, "table is follower. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(103);
            response->set_msg("table is follower");
            break;
        }
        std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
        if (!replicator) {
            response->set_code(110);
            response->set_msg("replicator is not exist");
            PDLOG(WARNING,"replicator is not exist. tid %u, pid %u", request->tid(), request->pid());
            break;
        }
        std::vector<std::string> vec;
        vec.push_back(request->endpoint());
        int ret = replicator->AddReplicateNode(vec);
        if (ret == 0) {
            response->set_code(0);
            response->set_msg("ok");
        } else if (ret < 0) {
            response->set_code(120);
            PDLOG(WARNING, "fail to add replica endpoint. tid %u pid %u", request->tid(), request->pid());
            response->set_msg("fail to add replica endpoint");
            break;
        } else {
            response->set_code(119);
            response->set_msg("replica endpoint already exists");
            PDLOG(WARNING, "replica endpoint already exists. tid %u pid %u", request->tid(), request->pid());
        }
        if (task_ptr) {
            std::lock_guard<std::mutex> lock(mu_);
            task_ptr->set_status(::rtidb::api::TaskStatus::kDone);
        }
        return;
    } while(0);
    if (task_ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
    }
}

void TabletImpl::DelReplica(RpcController* controller, 
            const ::rtidb::api::ReplicaRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);        
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kDelReplica, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    do {
        if (!table) {
            PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(),
                    request->pid());
            response->set_code(100);
            response->set_msg("table is not exist");
            break;
        }
        if (!table->IsLeader()) {
            PDLOG(WARNING, "table is follower. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(103);
            response->set_msg("table is follower");
            break;
        }
        std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
        if (!replicator) {
            response->set_code(110);
            response->set_msg("replicator is not exist");
            PDLOG(WARNING,"replicator is not exist. tid %u, pid %u", request->tid(), request->pid());
            break;
        }
        int ret = replicator->DelReplicateNode(request->endpoint());
        if (ret == 0) {
            response->set_code(0);
            response->set_msg("ok");
        } else if (ret < 0) {
            response->set_code(121);
            PDLOG(WARNING, "replicator role is not leader. table %u pid %u", request->tid(), request->pid());
            response->set_msg("replicator role is not leader");
            break;
        } else {
            response->set_code(0);
            PDLOG(WARNING, "fail to del endpoint for table %u pid %u. replica does not exist", 
                            request->tid(), request->pid());
            response->set_msg("replica does not exist");
        }
        if (task_ptr) {
            std::lock_guard<std::mutex> lock(mu_);
            task_ptr->set_status(::rtidb::api::TaskStatus::kDone);
        }
        return;
    } while (0);
    if (task_ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
    }
}

void TabletImpl::AppendEntries(RpcController* controller,
        const ::rtidb::api::AppendEntriesRequest* request,
        ::rtidb::api::AppendEntriesResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(),
                request->pid());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    if (table->IsLeader()) {
        PDLOG(WARNING, "table is leader. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(102);
        response->set_msg("table is leader");
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        response->set_code(104);
        response->set_msg("table is loading");
        PDLOG(WARNING, "table is loading. tid %u, pid %u", request->tid(), request->pid());
        return;
    }    
    std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
    if (!replicator) {
        response->set_code(110);
        response->set_msg("replicator is not exist");
        return;
    }
    bool ok = replicator->AppendEntries(request, response);
    if (!ok) {
        response->set_code(122);
        response->set_msg("fail to append entries to replicator");
    } else {
        response->set_code(0);
        response->set_msg("ok");
    }
}

void TabletImpl::GetTableSchema(RpcController* controller,
            const ::rtidb::api::GetTableSchemaRequest* request,
            ::rtidb::api::GetTableSchemaResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);        
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        response->set_code(100);
        response->set_msg("table is not exist");
        PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(),
                request->pid());
        return;
    } else {
        response->set_schema(table->GetSchema());
    }
    response->set_code(0);
    response->set_msg("ok");
    response->set_schema(table->GetSchema());
    response->mutable_table_meta()->CopyFrom(table->GetTableMeta());
}

void TabletImpl::GetTableStatus(RpcController* controller,
            const ::rtidb::api::GetTableStatusRequest* request,
            ::rtidb::api::GetTableStatusResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);        
    std::lock_guard<std::mutex> lock(mu_);        
    for (auto it = tables_.begin(); it != tables_.end(); ++it) {
        if (request->has_tid() && request->tid() != it->first) {
            continue;
        }
        for (auto pit = it->second.begin(); pit != it->second.end(); ++pit) {
            if (request->has_pid() && request->pid() != pit->first) {
                continue;
            }
            std::shared_ptr<Table> table = pit->second;
            ::rtidb::api::TableStatus* status = response->add_all_table_status();
            status->set_mode(::rtidb::api::TableMode::kTableFollower);
            if (table->IsLeader()) {
                status->set_mode(::rtidb::api::TableMode::kTableLeader);
            }
            status->set_tid(table->GetId());
            status->set_pid(table->GetPid());
            status->set_ttl(table->GetTTL());
            status->set_ttl_type(table->GetTTLType());
            status->set_compress_type(table->GetCompressType());
            status->set_storage_mode(table->GetStorageMode());
            status->set_name(table->GetName());
            if (::rtidb::api::TableState_IsValid(table->GetTableStat())) {
                status->set_state(::rtidb::api::TableState(table->GetTableStat()));
            }
            std::shared_ptr<LogReplicator> replicator = GetReplicatorUnLock(table->GetId(), table->GetPid());
            if (replicator) {
                status->set_offset(replicator->GetOffset());
            }
            status->set_record_cnt(table->GetRecordCnt());
            if (table->GetStorageMode() == ::rtidb::common::StorageMode::kMemory) {
                if (MemTable* mem_table = dynamic_cast<MemTable*>(table.get())) {
                    status->set_time_offset(mem_table->GetTimeOffset());
                    status->set_is_expire(mem_table->GetExpireStatus());
                    status->set_record_byte_size(mem_table->GetRecordByteSize());
                    status->set_record_idx_byte_size(mem_table->GetRecordIdxByteSize());
                    status->set_record_pk_cnt(mem_table->GetRecordPkCnt());
                    status->set_skiplist_height(mem_table->GetKeyEntryHeight());
                    uint64_t record_idx_cnt = 0;
                    for (auto iit = table->GetMapping().begin(); iit != table->GetMapping().end(); ++iit) {
                        ::rtidb::api::TsIdxStatus* ts_idx_status = status->add_ts_idx_status();
                        ts_idx_status->set_idx_name(iit->first);
                        uint64_t* stats = NULL;
                        uint32_t size = 0;
                        bool ok = mem_table->GetRecordIdxCnt(iit->second, &stats, &size);
                        if (ok) {
                            for (uint32_t i = 0; i < size; i++) {
                                ts_idx_status->add_seg_cnts(stats[i]); 
                                record_idx_cnt += stats[i];
                            }
                        }
                        delete stats;
                    }
                    status->set_idx_cnt(record_idx_cnt);
                }
            }
            if (request->has_need_schema() && request->need_schema()) {
                status->set_schema(table->GetSchema());
            }
        }
    }
    response->set_code(0);
}

void TabletImpl::SetExpire(RpcController* controller,
            const ::rtidb::api::SetExpireRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);        
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    if (table->GetStorageMode() == ::rtidb::common::StorageMode::kMemory) {
        MemTable* mem_table = dynamic_cast<MemTable*>(table.get());
        if (mem_table != NULL) {
            mem_table->SetExpire(request->is_expire());
            PDLOG(INFO, "set table expire[%d]. tid[%u] pid[%u]", request->is_expire(), request->tid(), request->pid());
        }
    }
    response->set_code(0);
    response->set_msg("ok");
}

void TabletImpl::SetTTLClock(RpcController* controller,
            const ::rtidb::api::SetTTLClockRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);        
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    if (table->GetStorageMode() == ::rtidb::common::StorageMode::kMemory) {
        MemTable* mem_table = dynamic_cast<MemTable*>(table.get());
        if (mem_table != NULL) {
            int64_t cur_time = ::baidu::common::timer::get_micros() / 1000000;
            int64_t offset = (int64_t)request->timestamp() - cur_time;
            mem_table->SetTimeOffset(offset);
            PDLOG(INFO, "set table virtual timestamp[%lu] cur timestamp[%lu] offset[%ld]. tid[%u] pid[%u]", 
                        request->timestamp(), cur_time, offset, request->tid(), request->pid());
        }
    }
    response->set_code(0);
    response->set_msg("ok");
}

void TabletImpl::MakeSnapshotInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task) {
    std::shared_ptr<Table> table;
    std::shared_ptr<Snapshot> snapshot;
    std::shared_ptr<LogReplicator> replicator;
    {
        std::lock_guard<std::mutex> lock(mu_);
        table = GetTableUnLock(tid, pid);
        bool has_error = true;
        do {
            if (!table) {
                PDLOG(WARNING, "table is not exist. tid[%u] pid[%u]", tid, pid);
                break;
            }
            if (table->GetTableStat() != ::rtidb::storage::kNormal) {
                PDLOG(WARNING, "table state is %d, cannot make snapshot. %u, pid %u", 
                             table->GetTableStat(), tid, pid);
                break;
            }    
            snapshot = GetSnapshotUnLock(tid, pid);
            if (!snapshot) {
                PDLOG(WARNING, "snapshot is not exist. tid[%u] pid[%u]", tid, pid);
                break;
            }
            replicator = GetReplicatorUnLock(tid, pid);
            if (!replicator) {
                PDLOG(WARNING, "replicator is not exist. tid[%u] pid[%u]", tid, pid);
                break;
            }
            has_error = false;
        } while (0);
        if (has_error) {
            if (task) {
                task->set_status(::rtidb::api::kFailed);
            }    
            return;
        }
        table->SetTableStat(::rtidb::storage::kMakingSnapshot);
    }
    uint64_t cur_offset = replicator->GetOffset();
    uint64_t snapshot_offset = snapshot->GetOffset();
    int ret = 0;
    if (cur_offset < snapshot_offset + FLAGS_make_snapshot_threshold_offset) {
        PDLOG(INFO, "offset can't reach the threshold. tid[%u] pid[%u] cur_offset[%lu], snapshot_offset[%lu]", 
                tid, pid, cur_offset, snapshot_offset);
    } else {
        if (table->GetStorageMode() != ::rtidb::common::StorageMode::kMemory) {
            ::rtidb::storage::DiskTableSnapshot* disk_snapshot = 
                dynamic_cast<::rtidb::storage::DiskTableSnapshot*>(snapshot.get());
            if (disk_snapshot != NULL) {
                disk_snapshot->SetTerm(replicator->GetLeaderTerm());
            }    
        }
        uint64_t offset = 0;
        ret = snapshot->MakeSnapshot(table, offset);
        if (ret == 0) {
            std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
            if (replicator) {
                replicator->SetSnapshotLogPartIndex(offset);
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        table->SetTableStat(::rtidb::storage::kNormal);
        if (task) {
            if (ret == 0) {
                task->set_status(::rtidb::api::kDone);
            } else {
                task->set_status(::rtidb::api::kFailed);
            }    
        }
    }
}

void TabletImpl::MakeSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kMakeSnapshot, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }    
    uint32_t tid = request->tid();        
    uint32_t pid = request->pid();        
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<Snapshot> snapshot = GetSnapshotUnLock(tid, pid);
    do {
        if (!snapshot) {
            response->set_code(111);
            response->set_msg("snapshot is not exist");
            PDLOG(WARNING, "snapshot is not exist. tid[%u] pid[%u]", tid, pid);
            break;
        }
        std::shared_ptr<Table> table = GetTableUnLock(request->tid(), request->pid());
        if (!table) {
            PDLOG(WARNING, "table is not exist. tid %u, pid %u", tid, pid);
            response->set_code(100);
            response->set_msg("table is not exist");
            break;
        }
        if (table->GetTableStat() != ::rtidb::storage::kNormal) {
            response->set_code(105);
            response->set_msg("table status is not kNormal");
            PDLOG(WARNING, "table state is %d, cannot make snapshot. %u, pid %u", 
                         table->GetTableStat(), tid, pid);
            break;
        }
        if (task_ptr) {
            task_ptr->set_status(::rtidb::api::TaskStatus::kDoing);
        }    
        snapshot_pool_.AddTask(boost::bind(&TabletImpl::MakeSnapshotInternal, this, tid, pid, task_ptr));
        response->set_code(0);
        response->set_msg("ok");
        return;
    } while (0);
    if (task_ptr) {       
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
    }
}

void TabletImpl::SchedMakeSnapshot() {
    int now_hour = ::rtidb::base::GetNowHour();
    if (now_hour != FLAGS_make_snapshot_time) {
        snapshot_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
        return;
    }
    std::vector<std::pair<uint32_t, uint32_t> > table_set;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto iter = tables_.begin(); iter != tables_.end(); ++iter) {
            for (auto inner = iter->second.begin(); inner != iter->second.end(); ++ inner) {
                if (iter->first == 0 && inner->first == 0) {
                    continue;
                }
                table_set.push_back(std::make_pair(iter->first, inner->first));
            }
        }
    }
    for (auto iter = table_set.begin(); iter != table_set.end(); ++iter) {
        PDLOG(INFO, "start make snapshot tid[%u] pid[%u]", iter->first, iter->second);
        MakeSnapshotInternal(iter->first, iter->second, std::shared_ptr<::rtidb::api::TaskInfo>());
    }
    // delay task one hour later avoid execute  more than one time
    snapshot_pool_.DelayTask(FLAGS_make_snapshot_check_interval + 60 * 60 * 1000, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
}

void TabletImpl::SendData(RpcController* controller,
            const ::rtidb::api::SendDataRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller*>(controller);
    uint32_t tid = request->tid(); 
    uint32_t pid = request->pid(); 
    std::string combine_key = std::to_string(tid) + "_" + std::to_string(pid) + "_" + request->file_name();
    std::shared_ptr<FileReceiver> receiver;
    std::string path = FLAGS_db_root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid) + "/";
    if (request->file_name() != "table_meta.txt") {
        path.append("snapshot/");
    }
    std::string dir_name;
    if (request->has_dir_name() && request->dir_name().size() > 0) {
        dir_name = request->dir_name();
        path.append(request->dir_name() + "/");
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = file_receiver_map_.find(combine_key);
        if (request->block_id() == 0) {
            std::shared_ptr<Table> table = GetTableUnLock(tid, pid);
            if (table) {
                PDLOG(WARNING, "table already exists. tid %u, pid %u", tid, pid);
                response->set_code(101);
                response->set_msg("table already exists");
                return;
            }
            if (iter == file_receiver_map_.end()) {
                file_receiver_map_.insert(std::make_pair(combine_key, 
                            std::make_shared<FileReceiver>(request->file_name(), dir_name, path)));
                iter = file_receiver_map_.find(combine_key);
            }
            if (!iter->second->Init()) {
                PDLOG(WARNING, "file receiver init failed. tid %u, pid %u, file_name %s", tid, pid, request->file_name().c_str());
                response->set_code(123);
                response->set_msg("file receiver init failed");
                file_receiver_map_.erase(iter);
                return;
            }
            PDLOG(INFO, "file receiver init ok. tid %u, pid %u, file_name %s", tid, pid, request->file_name().c_str());
            response->set_code(0);
            response->set_msg("ok");
        } else if (iter == file_receiver_map_.end()){
            PDLOG(WARNING, "cannot find receiver. tid %u, pid %u, file_name %s", tid, pid, request->file_name().c_str());
            response->set_code(124);
            response->set_msg("cannot find receiver");
            return;
        }
        receiver = iter->second;
    }
    if (!receiver) {
        PDLOG(WARNING, "cannot find receiver. tid %u, pid %u, file_name %s", tid, pid, request->file_name().c_str());
        response->set_code(124);
        response->set_msg("cannot find receiver");
        return;
    }
    if (receiver->GetBlockId() == request->block_id()) {
        response->set_msg("ok");
        response->set_code(0);
        return;
    }
    if (request->block_id() != receiver->GetBlockId() + 1) {
        response->set_msg("block_id mismatch");
        PDLOG(WARNING, "block_id mismatch. tid %u, pid %u, file_name %s, request block_id %lu cur block_id %lu", 
                        tid, pid, request->file_name().c_str(), request->block_id(), receiver->GetBlockId());
        response->set_code(125);
        return;
    }
    std::string data = cntl->request_attachment().to_string();
    if (data.length() != request->block_size()) {
        PDLOG(WARNING, "receive data error. tid %u, pid %u, file_name %s, expected length %u real length %u", 
                        tid, pid, request->file_name().c_str(), request->block_size(), data.length());
        response->set_code(126);
        response->set_msg("receive data error");
        return;
    }
    if (receiver->WriteData(data, request->block_id()) < 0) {
        PDLOG(WARNING, "receiver write data failed. tid %u, pid %u, file_name %s", tid, pid, request->file_name().c_str());
        response->set_code(127);
        response->set_msg("write data failed");
        return;
    }
    if (request->eof()) {
        receiver->SaveFile();
        std::lock_guard<std::mutex> lock(mu_);
        file_receiver_map_.erase(combine_key);
    }
    response->set_msg("ok");
    response->set_code(0);
}

void TabletImpl::SendSnapshot(RpcController* controller,
            const ::rtidb::api::SendSnapshotRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kSendSnapshot, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }    
    std::lock_guard<std::mutex> lock(mu_);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTableUnLock(tid, pid);
    std::string sync_snapshot_key = request->endpoint() + "_" + 
                    std::to_string(tid) + "_" + std::to_string(pid);
    do {
        if (sync_snapshot_set_.find(sync_snapshot_key) != sync_snapshot_set_.end()) {
            PDLOG(WARNING, "snapshot is sending. tid %u pid %u endpoint %s", 
                            tid, pid, request->endpoint().c_str());
            response->set_code(128);
            response->set_msg("snapshot is sending");
            break;
        }
        if (!table) {
            PDLOG(WARNING, "table is not exist. tid %u, pid %u", tid, pid);
            response->set_code(100);
            response->set_msg("table is not exist");
            break;
        }
        if (!table->IsLeader()) {
            PDLOG(WARNING, "table is follower. tid %u, pid %u", tid, pid);
            response->set_code(103);
            response->set_msg("table is follower");
            break;
        }
        if (table->GetTableStat() != ::rtidb::storage::kSnapshotPaused) {
            PDLOG(WARNING, "table status is not kSnapshotPaused. tid %u, pid %u", tid, pid);
            response->set_code(107);
            response->set_msg("table status is not kSnapshotPaused");
            break;
        }
        if (task_ptr) {
            task_ptr->set_status(::rtidb::api::TaskStatus::kDoing);
        }    
        sync_snapshot_set_.insert(sync_snapshot_key);
        task_pool_.AddTask(boost::bind(&TabletImpl::SendSnapshotInternal, this, 
                    request->endpoint(), tid, pid, task_ptr));
        response->set_code(0);
        response->set_msg("ok");
        return;
    } while(0);
    if (task_ptr) {
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
    }
}

void TabletImpl::SendSnapshotInternal(const std::string& endpoint, uint32_t tid, uint32_t pid, 
            std::shared_ptr<::rtidb::api::TaskInfo> task) {
    bool has_error = true;
    FileSender sender(tid, pid, endpoint);
    if (!sender.Init()) {
        PDLOG(WARNING, "Init FileSender failed. tid[%u] pid[%u] endpoint[%s]", tid, pid, endpoint.c_str());
        return;
    }
    do {
        std::shared_ptr<Table> table = GetTable(tid, pid);
        if (!table) {
            PDLOG(WARNING, "table is not exist. tid %u, pid %u", tid, pid);
            break;
        }
        std::string db_root_path = FLAGS_db_root_path;
        if (table->GetStorageMode() == ::rtidb::common::StorageMode::kSSD) {
            db_root_path = FLAGS_ssd_root_path;
        } else if (table->GetStorageMode() == ::rtidb::common::StorageMode::kHDD) {
            db_root_path = FLAGS_hdd_root_path;
        }
        // send table_meta file
        std::string full_path = db_root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid) + "/";
        std::string file_name = "table_meta.txt";
        if (sender.SendFile(file_name, full_path + file_name) < 0) {
            PDLOG(WARNING, "send table_meta.txt failed. tid[%u] pid[%u]", tid, pid);
            break;
        }

        full_path.append("snapshot/");
        std::string manifest_file = full_path + "MANIFEST";
        std::string snapshot_file;
        {
            int fd = open(manifest_file.c_str(), O_RDONLY);
            if (fd < 0) {
                PDLOG(WARNING, "[%s] is not exist", manifest_file.c_str());
                has_error = false;
                break;
            }
            google::protobuf::io::FileInputStream fileInput(fd);
            fileInput.SetCloseOnDelete(true);
            ::rtidb::api::Manifest manifest;
            if (!google::protobuf::TextFormat::Parse(&fileInput, &manifest)) {
                PDLOG(WARNING, "parse manifest failed. tid[%u] pid[%u]", tid, pid);
                break;
            }
            snapshot_file = manifest.name();
        }
        if (table->GetStorageMode() == ::rtidb::common::StorageMode::kMemory) {
            // send snapshot file
            if (sender.SendFile(snapshot_file, full_path + snapshot_file) < 0) {
                PDLOG(WARNING, "send snapshot failed. tid[%u] pid[%u]", tid, pid);
                break;
            }
        } else {
            if (sender.SendDir(snapshot_file, full_path + snapshot_file) < 0) {
                PDLOG(WARNING, "send snapshot failed. tid[%u] pid[%u]", tid, pid);
                break;
            }
        }
        // send manifest file
        file_name = "MANIFEST";
        if (sender.SendFile(file_name, full_path + file_name) < 0) {
            PDLOG(WARNING, "send MANIFEST failed. tid[%u] pid[%u]", tid, pid);
            break;
        }
        has_error = false;
        PDLOG(INFO, "send snapshot success. endpoint %s tid %u pid %u", endpoint.c_str(), tid, pid);
    } while(0);
    std::lock_guard<std::mutex> lock(mu_);
    if (task) {
        if (has_error) {
            task->set_status(::rtidb::api::kFailed);
           } else {
            task->set_status(::rtidb::api::kDone);
        }
    }
    std::string sync_snapshot_key = endpoint + "_" + 
                    std::to_string(tid) + "_" + std::to_string(pid);
    sync_snapshot_set_.erase(sync_snapshot_key);
}

void TabletImpl::PauseSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);        
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kPauseSnapshot, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }    
    std::lock_guard<std::mutex> lock(mu_);
    std::shared_ptr<Table> table = GetTableUnLock(request->tid(), request->pid());
    do {
        if (!table) {
            PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(100);
            response->set_msg("table is not exist");
            break;
        }
        if (table->GetTableStat() == ::rtidb::storage::kSnapshotPaused) {
            PDLOG(INFO, "table status is kSnapshotPaused, need not pause. tid[%u] pid[%u]", 
                        request->tid(), request->pid());
        } else if (table->GetTableStat() != ::rtidb::storage::kNormal) {
            PDLOG(WARNING, "table status is [%u], cann't pause. tid[%u] pid[%u]", 
                            table->GetTableStat(), request->tid(), request->pid());
            response->set_code(105);
            response->set_msg("table status is not kNormal");
            break;
        } else {
            table->SetTableStat(::rtidb::storage::kSnapshotPaused);
            PDLOG(INFO, "table status has set[%u]. tid[%u] pid[%u]", 
                       table->GetTableStat(), request->tid(), request->pid());
        }           
        if (task_ptr) {
            task_ptr->set_status(::rtidb::api::TaskStatus::kDone);
        }
        response->set_code(0);
        response->set_msg("ok");
        return;
    } while(0);
    if (task_ptr) {
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
    }
}

void TabletImpl::RecoverSnapshot(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);        
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kRecoverSnapshot, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    do {
        std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
        if (!table) {
            PDLOG(WARNING, "table is not exist. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(100);
            response->set_msg("table is not exist");
            break;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (table->GetTableStat() == rtidb::storage::kNormal) {
                PDLOG(INFO, "table status is already kNormal, need not recover. tid[%u] pid[%u]", 
                            request->tid(), request->pid());

            } else if (table->GetTableStat() != ::rtidb::storage::kSnapshotPaused) {
                PDLOG(WARNING, "table status is [%u], cann't recover. tid[%u] pid[%u]", 
                        table->GetTableStat(), request->tid(), request->pid());
                response->set_code(107);
                response->set_msg("table status is not kSnapshotPaused");
                break;
            } else {
                table->SetTableStat(::rtidb::storage::kNormal);
                PDLOG(INFO, "table status has set[%u]. tid[%u] pid[%u]", 
                           table->GetTableStat(), request->tid(), request->pid());
            }
            if (task_ptr) {       
                task_ptr->set_status(::rtidb::api::TaskStatus::kDone);
            }
        }
        response->set_code(0);
        response->set_msg("ok");
        return;
    } while(0);
    if (task_ptr) {       
        std::lock_guard<std::mutex> lock(mu_);
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
    }
}

void TabletImpl::LoadTable(RpcController* controller,
            const ::rtidb::api::LoadTableRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kLoadTable, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    do {
        ::rtidb::api::TableMeta table_meta;
        table_meta.CopyFrom(request->table_meta());
        std::string msg;
        if (CheckTableMeta(&table_meta, msg) != 0) {
            response->set_code(129);
            response->set_msg(msg);
            break;
        }
        uint32_t tid = table_meta.tid();
        uint32_t pid = table_meta.pid();
        std::string root_path = FLAGS_db_root_path;
        if (table_meta.storage_mode() == ::rtidb::common::kHDD) {
            root_path = FLAGS_hdd_root_path;
        } else if (table_meta.storage_mode() == ::rtidb::common::kSSD) {
            root_path = FLAGS_ssd_root_path;
        }
        std::string db_path = root_path + "/" + std::to_string(tid) + 
                        "_" + std::to_string(pid);
        if (!::rtidb::base::IsExists(db_path)) {
            PDLOG(WARNING, "table db path is not exist. tid %u, pid %u", tid, pid);
            response->set_code(130);
            response->set_msg("table db path is not exist");
            break;
        }
        if (table_meta.storage_mode() != rtidb::common::kMemory) {
            std::lock_guard<std::mutex> lock(mu_);
            std::shared_ptr<Table> disk_table = GetTableUnLock(tid, pid);
            if (disk_table) {
                PDLOG(WARNING, "table with tid[%u] and pid[%u] exists", tid, pid);
                response->set_code(101);
                response->set_msg("table already exists");
                return;
            }
            UpdateTableMeta(db_path, &table_meta);
            if (WriteTableMeta(db_path, &table_meta) < 0) {
                PDLOG(WARNING, "write table_meta failed. tid[%u] pid[%u]", tid, pid);
                response->set_code(127);
                response->set_msg("write data failed");
                return;
            }
            std::string msg;
            if (CreateDiskTableInternal(&table_meta, true, msg) < 0) {
                response->set_code(131);
                response->set_msg(msg.c_str());
                return;
            }
            if (table_meta.ttl() > 0) {
                gc_pool_.DelayTask(FLAGS_disk_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid, false));
            }
            response->set_code(0);
            response->set_msg("ok");
            PDLOG(INFO, "load table ok. tid[%u] pid[%u] storage mode[%s]", 
                        tid, pid, ::rtidb::common::StorageMode_Name(table_meta.storage_mode()).c_str());
            return;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::shared_ptr<Table> table = GetTableUnLock(tid, pid);
            if (!table) {
                UpdateTableMeta(db_path, &table_meta);
                if (WriteTableMeta(db_path, &table_meta) < 0) {
                    PDLOG(WARNING, "write table_meta failed. tid[%lu] pid[%lu]", tid, pid);
                    response->set_code(127);
                    response->set_msg("write data failed");
                    break;
                }
                std::string msg;
                if (CreateTableInternal(&table_meta, msg) < 0) {
                    response->set_code(131);
                    response->set_msg(msg.c_str());
                    break;
                }
            } else {
                response->set_code(101);
                response->set_msg("table already exists");
                break;
            }
        }
        uint64_t ttl = table_meta.ttl();
        std::string name = table_meta.name();
        uint32_t seg_cnt = 8;
        if (table_meta.seg_cnt() > 0) {
            seg_cnt = table_meta.seg_cnt();
        }
        PDLOG(INFO, "start to recover table with id %u pid %u name %s seg_cnt %d idx_cnt %u schema_size %u ttl %llu", tid, 
                   pid, name.c_str(), seg_cnt, table_meta.dimensions_size(), table_meta.schema().size(), ttl);
        task_pool_.AddTask(boost::bind(&TabletImpl::LoadTableInternal, this, tid, pid, task_ptr));
        response->set_code(0);
        response->set_msg("ok");
        return;
    } while(0);
    if (task_ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
    }        
}

int TabletImpl::LoadTableInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task_ptr) {
    do {
        // load snapshot data
        std::shared_ptr<Table> table = GetTable(tid, pid);        
        if (!table) {
            PDLOG(WARNING, "table with tid %u and pid %u does not exist", tid, pid);
            break; 
        }
        std::shared_ptr<Snapshot> snapshot = GetSnapshot(tid, pid);
        if (!snapshot) {
            PDLOG(WARNING, "snapshot with tid %u and pid %u does not exist", tid, pid);
            break; 
        }
        std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
        if (!replicator) {
            PDLOG(WARNING, "replicator with tid %u and pid %u does not exist", tid, pid);
            break;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            table->SetTableStat(::rtidb::storage::kLoading);
        }
        uint64_t latest_offset = 0;
        bool ok = snapshot->Recover(table, latest_offset);
        if (ok) {
            table->SetTableStat(::rtidb::storage::kNormal);
            replicator->SetOffset(latest_offset);
            replicator->SetSnapshotLogPartIndex(snapshot->GetOffset());
            replicator->StartSyncing();
            table->SchedGc();
            gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid, false));
            io_pool_.DelayTask(FLAGS_binlog_sync_to_disk_interval, boost::bind(&TabletImpl::SchedSyncDisk, this, tid, pid));
            task_pool_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&TabletImpl::SchedDelBinlog, this, tid, pid));
            PDLOG(INFO, "load table success. tid %u pid %u", tid, pid);
            if (task_ptr) {
                std::lock_guard<std::mutex> lock(mu_);
                task_ptr->set_status(::rtidb::api::TaskStatus::kDone);
                return 0;
            }
        } else {
           DeleteTableInternal(tid, pid, std::shared_ptr<::rtidb::api::TaskInfo>());
        }
    } while (0);    
    if (task_ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
    }
    return -1;
}

int32_t TabletImpl::DeleteTableInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task_ptr) {
    std::shared_ptr<Table> table = GetTable(tid, pid);
    std::string root_path = FLAGS_db_root_path;
    std::string recycle_bin_root_path = FLAGS_recycle_bin_root_path;
    if (table->GetStorageMode() != ::rtidb::common::StorageMode::kMemory) {
        if (table->GetStorageMode() == ::rtidb::common::StorageMode::kSSD) {
            root_path = FLAGS_ssd_root_path;
            recycle_bin_root_path = FLAGS_recycle_ssd_bin_root_path;
        } else {
            root_path = FLAGS_hdd_root_path;
            recycle_bin_root_path = FLAGS_recycle_hdd_bin_root_path;

        }
        std::lock_guard<std::mutex> lock(mu_);
        tables_[tid].erase(pid);
    } else {
        std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
        // do block other requests
        {
            std::lock_guard<std::mutex> lock(mu_);
            tables_[tid].erase(pid);
            replicators_[tid].erase(pid);
            snapshots_[tid].erase(pid);
        }

        if (replicator) {
            replicator->DelAllReplicateNode();
            PDLOG(INFO, "drop replicator for tid %u, pid %u", tid, pid);
        }
        root_path = FLAGS_db_root_path;
    }
    std::string source_path = root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid);

    if (!::rtidb::base::IsExists(source_path)) {
        if (task_ptr) {
            std::lock_guard<std::mutex> lock(mu_);
            task_ptr->set_status(::rtidb::api::TaskStatus::kDone);
        }        
        PDLOG(INFO, "drop table ok. tid[%u] pid[%u]", tid, pid);
        return 0;
    }

    std::string recycle_path = recycle_bin_root_path + "/" + std::to_string(tid) + 
           "_" + std::to_string(pid) + "_" + ::rtidb::base::GetNowTime();
    ::rtidb::base::Rename(source_path, recycle_path);
    if (task_ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        task_ptr->set_status(::rtidb::api::TaskStatus::kDone);
    }
    PDLOG(INFO, "drop table ok. tid[%u] pid[%u]", tid, pid);
    return 0;
}

void TabletImpl::CreateTable(RpcController* controller,
            const ::rtidb::api::CreateTableRequest* request,
            ::rtidb::api::CreateTableResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const ::rtidb::api::TableMeta* table_meta = &request->table_meta();
    std::string msg;
    uint32_t tid = table_meta->tid();
    uint32_t pid = table_meta->pid();
    if (CheckTableMeta(table_meta, msg) != 0) {
        response->set_code(129);
        response->set_msg(msg);
        PDLOG(WARNING, "check table_meta failed. tid[%u] pid[%u], err_msg[%s]", tid, pid, msg.c_str());
        return;
    }
    std::shared_ptr<Table> table = GetTable(tid, pid);
    std::shared_ptr<Snapshot> snapshot = GetSnapshot(tid, pid);
    if (table || snapshot) {
        if (table) {
            PDLOG(WARNING, "table with tid[%u] and pid[%u] exists", tid, pid);
        }
        if (snapshot) {
            PDLOG(WARNING, "snapshot with tid[%u] and pid[%u] exists", tid, pid);
        }
        response->set_code(101);
        response->set_msg("table already exists");
        return;
    }       
    ::rtidb::api::TTLType type = table_meta->ttl_type();
    uint64_t ttl = table_meta->ttl();
    PDLOG(INFO, "start creating table tid[%u] pid[%u] with mode %s", 
            tid, pid, ::rtidb::api::TableMode_Name(request->table_meta().mode()).c_str());
    std::string name = table_meta->name();
    if (table_meta->storage_mode() != rtidb::common::kMemory) {
        std::lock_guard<std::mutex> lock(mu_);
        std::string db_root_path = table_meta->storage_mode() == 
                    ::rtidb::common::StorageMode::kSSD ? FLAGS_ssd_root_path : FLAGS_hdd_root_path;
        std::string table_db_path = db_root_path + "/" + std::to_string(tid) +
                        "_" + std::to_string(pid);
        if (WriteTableMeta(table_db_path, table_meta) < 0) {
            PDLOG(WARNING, "write table_meta failed. tid[%u] pid[%u]", tid, pid);
            response->set_code(127);
            response->set_msg("write data failed");
            return;
        }
        std::string msg;
        if (CreateDiskTableInternal(table_meta, false, msg) < 0) {
            response->set_code(131);
            response->set_msg(msg.c_str());
            return;
        }
    } else {
        std::lock_guard<std::mutex> lock(mu_);
        std::string table_db_path = FLAGS_db_root_path + "/" + std::to_string(tid) +
                        "_" + std::to_string(pid);
        if (WriteTableMeta(table_db_path, table_meta) < 0) {
            PDLOG(WARNING, "write table_meta failed. tid[%lu] pid[%lu]", tid, pid);
            response->set_code(127);
            response->set_msg("write data failed");
            return;
        }
        std::string msg;
        if (CreateTableInternal(table_meta, msg) < 0) {
            response->set_code(131);
            response->set_msg(msg.c_str());
            return;
        }
    }
    response->set_code(0);
    response->set_msg("ok");
    table = GetTable(tid, pid);        
    if (!table) {
        PDLOG(WARNING, "table with tid %u and pid %u does not exist", tid, pid);
        return; 
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        PDLOG(WARNING, "replicator with tid %u and pid %u does not exist", tid, pid);
        return;
    }
    table->SetTableStat(::rtidb::storage::kNormal);
    replicator->StartSyncing();
    io_pool_.DelayTask(FLAGS_binlog_sync_to_disk_interval, boost::bind(&TabletImpl::SchedSyncDisk, this, tid, pid));
    task_pool_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&TabletImpl::SchedDelBinlog, this, tid, pid));
    PDLOG(INFO, "create table with id %u pid %u name %s ttl %llu type %s", 
                tid, pid, name.c_str(), ttl, ::rtidb::api::TTLType_Name(type).c_str());
    gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid, false));
}

void TabletImpl::ExecuteGc(RpcController* controller,
            const ::rtidb::api::ExecuteGcRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        PDLOG(DEBUG, "table is not exist. tid %u pid %u", tid, pid);
        response->set_code(-1);
        response->set_msg("table not found");
        return;
    }
    gc_pool_.AddTask(boost::bind(&TabletImpl::GcTable, this, tid, pid, true));
    response->set_code(0);
    response->set_msg("ok");
    PDLOG(INFO, "ExecuteGc. tid %u pid %u", tid, pid);
}

void TabletImpl::GetTableFollower(RpcController* controller,
            const ::rtidb::api::GetTableFollowerRequest* request,
            ::rtidb::api::GetTableFollowerResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        PDLOG(DEBUG, "table is not exist. tid %u pid %u", tid, pid);
        response->set_code(100);
        response->set_msg("table is not exist");
        return;
    }
    if (!table->IsLeader()) {
        PDLOG(DEBUG, "table is follower. tid %u, pid %u", tid, pid);
        response->set_msg("table is follower");
        response->set_code(103);
        return;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        PDLOG(DEBUG, "replicator is not exist. tid %u pid %u", tid, pid);
        response->set_msg("replicator is not exist");
        response->set_code(110);
        return;
    }
    response->set_offset(replicator->GetOffset());
    std::map<std::string, uint64_t> info_map;
    replicator->GetReplicateInfo(info_map);
    if (info_map.empty()) {
        response->set_msg("has no follower");
        response->set_code(134);
    }
    for (const auto& kv : info_map) {
        ::rtidb::api::FollowerInfo* follower_info = response->add_follower_info();
        follower_info->set_endpoint(kv.first);
        follower_info->set_offset(kv.second);
    }
    response->set_msg("ok");
    response->set_code(0);
}

void TabletImpl::GetTermPair(RpcController* controller,
            const ::rtidb::api::GetTermPairRequest* request,
            ::rtidb::api::GetTermPairResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (FLAGS_zk_cluster.empty()) {
        response->set_code(-1);
        response->set_msg("tablet is not run in cluster mode");
        PDLOG(WARNING, "tablet is not run in cluster mode");
        return;
    }
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        response->set_code(0);
        response->set_has_table(false);
        response->set_msg("table is not exist");

        std::string db_path = FLAGS_db_root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid);
        std::string manifest_file =  db_path + "/snapshot/MANIFEST";
        int fd = open(manifest_file.c_str(), O_RDONLY);
        response->set_msg("ok");
        if (fd < 0) {
            PDLOG(WARNING, "[%s] is not exist", manifest_file.c_str());
            response->set_term(0);
            response->set_offset(0);
            return;
        }
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        ::rtidb::api::Manifest manifest;
        if (!google::protobuf::TextFormat::Parse(&fileInput, &manifest)) {
            PDLOG(WARNING, "parse manifest failed");
            response->set_term(0);
            response->set_offset(0);
            return;
        }
        std::string snapshot_file = db_path + "/snapshot/" + manifest.name();
        if (!::rtidb::base::IsExists(snapshot_file)) {
            PDLOG(WARNING, "snapshot file[%s] is not exist", snapshot_file.c_str());
            response->set_term(0);
            response->set_offset(0);
            return;
        }
        response->set_term(manifest.term());
        response->set_offset(manifest.offset());
        return;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        response->set_code(110);
        response->set_msg("replicator is not exist");
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
    response->set_has_table(true);
    if (table->IsLeader()) {
        response->set_is_leader(true);
    } else {
        response->set_is_leader(false);
    }
    response->set_term(replicator->GetLeaderTerm());
    response->set_offset(replicator->GetOffset());
}

void TabletImpl::DeleteBinlog(RpcController* controller,
            const ::rtidb::api::GeneralRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::string db_path = FLAGS_db_root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid);
    std::string binlog_path = db_path + "/binlog";
    if (::rtidb::base::IsExists(binlog_path)) {
        std::string recycle_path = FLAGS_recycle_bin_root_path + "/" + std::to_string(tid) + 
               "_" + std::to_string(pid) + "_binlog_" + ::rtidb::base::GetNowTime();
        ::rtidb::base::Rename(binlog_path, recycle_path);
        PDLOG(INFO, "binlog has moved form %s to %s. tid %u pid %u", 
                    binlog_path.c_str(), recycle_path.c_str(), tid, pid);
    }
    response->set_code(0);
    response->set_msg("ok");
}        

void TabletImpl::CheckFile(RpcController* controller,
            const ::rtidb::api::CheckFileRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::string file_name = request->file();
    std::string full_path = FLAGS_db_root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid) + "/";
    if (file_name != "table_meta.txt") {
        full_path += "snapshot/";
    }
    if (request->has_dir_name() && request->dir_name().size() > 0) {
        full_path.append(request->dir_name() + "/");
    }
    full_path += file_name;
    uint64_t size = 0;
    if (::rtidb::base::GetSize(full_path, size) < 0) {
        response->set_code(-1);
        response->set_msg("get size failed");
        PDLOG(WARNING, "get size failed. file[%s]", full_path.c_str());
        return;
    }
    if (size != request->size()) {
        response->set_code(-1);
        response->set_msg("check size failed");
        PDLOG(WARNING, "check size failed. file[%s] cur_size[%lu] expect_size[%lu]", 
                        full_path.c_str(), size, request->size());
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
}

void TabletImpl::GetManifest(RpcController* controller,
            const ::rtidb::api::GetManifestRequest* request,
            ::rtidb::api::GetManifestResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::string db_path = FLAGS_db_root_path + "/" + std::to_string(request->tid()) + "_" + 
                std::to_string(request->pid());
    std::string manifest_file =  db_path + "/snapshot/MANIFEST";
    ::rtidb::api::Manifest manifest;
    int fd = open(manifest_file.c_str(), O_RDONLY);
    if (fd >= 0) {
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        if (!google::protobuf::TextFormat::Parse(&fileInput, &manifest)) {
            PDLOG(WARNING, "parse manifest failed");
            response->set_code(-1);
            response->set_msg("parse manifest failed");
            return;
        }
    } else {
        PDLOG(INFO, "[%s] is not exist", manifest_file.c_str());
        manifest.set_offset(0);
    }
    response->set_code(0);
    response->set_msg("ok");
    ::rtidb::api::Manifest* manifest_r = response->mutable_manifest();
    manifest_r->CopyFrom(manifest);
}

int TabletImpl::WriteTableMeta(const std::string& path, const ::rtidb::api::TableMeta* table_meta) {
    if (!::rtidb::base::MkdirRecur(path)) {
        PDLOG(WARNING, "fail to create path %s", path.c_str());
        return -1;
    }
    std::string full_path = path + "/table_meta.txt";
    std::string table_meta_info;
    google::protobuf::TextFormat::PrintToString(*table_meta, &table_meta_info);
    FILE* fd_write = fopen(full_path.c_str(), "w");
    if (fd_write == NULL) {
        PDLOG(WARNING, "fail to open file %s. err[%d: %s]", full_path.c_str(), errno, strerror(errno));
        return -1;
    }
    if (fputs(table_meta_info.c_str(), fd_write) == EOF) {
        PDLOG(WARNING, "write error. path[%s], err[%d: %s]", full_path.c_str(), errno, strerror(errno));
        fclose(fd_write);
        return -1;
    }
    fclose(fd_write);
    return 0;
}

int TabletImpl::UpdateTableMeta(const std::string& path, ::rtidb::api::TableMeta* table_meta) {
    std::string full_path = path + "/table_meta.txt";
    int fd = open(full_path.c_str(), O_RDONLY);
    ::rtidb::api::TableMeta old_meta;
    if (fd < 0) {
        PDLOG(WARNING, "[%s] is not exist", "table_meta.txt");
        return 1;
    } else {
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        if (!google::protobuf::TextFormat::Parse(&fileInput, &old_meta)) {
            PDLOG(WARNING, "parse table_meta failed");
            return -1;
        }
    }
    // use replicas in LoadRequest
    old_meta.clear_replicas();
    old_meta.MergeFrom(*table_meta);
    table_meta->CopyFrom(old_meta);
    std::string new_name = full_path + "." + ::rtidb::base::GetNowTime();
    rename(full_path.c_str(), new_name.c_str());
    return 0;
}

int TabletImpl::CreateTableInternal(const ::rtidb::api::TableMeta* table_meta, std::string& msg) {
    std::vector<std::string> endpoints;
    for (int32_t i = 0; i < table_meta->replicas_size(); i++) {
        endpoints.push_back(table_meta->replicas(i));
    }
    Table* table_ptr = new MemTable(*table_meta);
    std::shared_ptr<Table> table(table_ptr);
    if (!table->Init()) {
        PDLOG(WARNING, "fail to init table. tid %u, pid %u", table_meta->tid(), table_meta->pid());
        msg.assign("fail to init table");
        return -1;
    }
    std::string table_db_path = FLAGS_db_root_path + "/" + std::to_string(table_meta->tid()) +
                "_" + std::to_string(table_meta->pid());
    std::shared_ptr<LogReplicator> replicator;
    if (table->IsLeader()) {
        replicator = std::make_shared<LogReplicator>(table_db_path, 
                                                     endpoints,
                                                     ReplicatorRole::kLeaderNode, 
                                                     table);
    } else {
        replicator = std::make_shared<LogReplicator>(table_db_path, 
                                                     std::vector<std::string>(), 
                                                     ReplicatorRole::kFollowerNode,
                                                     table);
    }
    if (!replicator) {
        PDLOG(WARNING, "fail to create replicator for table tid %u, pid %u", table_meta->tid(), table_meta->pid());
        msg.assign("fail create replicator for table");
        return -1;
    }
    bool ok = replicator->Init();
    if (!ok) {
        PDLOG(WARNING, "fail to init replicator for table tid %u, pid %u", table_meta->tid(), table_meta->pid());
        // clean memory
        msg.assign("fail init replicator for table");
        return -1;
    }
    if (!FLAGS_zk_cluster.empty() && table_meta->mode() == ::rtidb::api::TableMode::kTableLeader) {
        replicator->SetLeaderTerm(table_meta->term());
    }
    ::rtidb::storage::Snapshot* snapshot_ptr = 
        new ::rtidb::storage::MemTableSnapshot(table_meta->tid(), table_meta->pid(), replicator->GetLogPart());
    if (!snapshot_ptr->Init()) {
        PDLOG(WARNING, "fail to init snapshot for tid %u, pid %u", table_meta->tid(), table_meta->pid());
        msg.assign("fail to init snapshot");
        return -1;
    }
    std::shared_ptr<Snapshot> snapshot(snapshot_ptr);
    tables_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), table));
    snapshots_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), snapshot));
    replicators_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), replicator));
    return 0;
}

int TabletImpl::CreateDiskTableInternal(const ::rtidb::api::TableMeta* table_meta, bool is_load, std::string& msg) {
    DiskTable* table_ptr = new DiskTable(*table_meta);
    std::shared_ptr<Table> table((Table*)table_ptr);
    if (is_load) {
        if (!table_ptr->LoadTable()) {
            return -1;
        }
    } else {
        if (!table->Init()) {
            return -1;
        }
    }
    tables_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), table));
    ::rtidb::storage::Snapshot* snapshot_ptr = 
        new ::rtidb::storage::DiskTableSnapshot(table_meta->tid(), table_meta->pid(), table_meta->storage_mode());
    if (!snapshot_ptr->Init()) {
        PDLOG(WARNING, "fail to init snapshot for tid %u, pid %u", table_meta->tid(), table_meta->pid());
        msg.assign("fail to init snapshot");
        return -1;
    }
    std::string table_db_path = table_meta->storage_mode() == ::rtidb::common::StorageMode::kSSD ? FLAGS_ssd_root_path : FLAGS_hdd_root_path;
    table_db_path += "/" + std::to_string(table_meta->tid()) + "_" + std::to_string(table_meta->pid());
    std::shared_ptr<LogReplicator> replicator;
    if (table->IsLeader()) {
        replicator = std::make_shared<LogReplicator>(table_db_path, 
                                                     std::vector<std::string>(),
                                                     ReplicatorRole::kLeaderNode, 
                                                     table);
    } else {
        replicator = std::make_shared<LogReplicator>(table_db_path, 
                                                     std::vector<std::string>(), 
                                                     ReplicatorRole::kFollowerNode,
                                                     table);
    }
    if (!replicator) {
        PDLOG(WARNING, "fail to create replicator for table tid %u, pid %u", table_meta->tid(), table_meta->pid());
        msg.assign("fail create replicator for table");
        return -1;
    }
    bool ok = replicator->Init();
    if (!ok) {
        PDLOG(WARNING, "fail to init replicator for table tid %u, pid %u", table_meta->tid(), table_meta->pid());
        // clean memory
        msg.assign("fail init replicator for table");
        return -1;
    }
    if (!FLAGS_zk_cluster.empty() && table_meta->mode() == ::rtidb::api::TableMode::kTableLeader) {
        replicator->SetLeaderTerm(table_meta->term());
    }
    std::shared_ptr<Snapshot> snapshot(snapshot_ptr);
    tables_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), table));
    snapshots_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), snapshot));
    replicators_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), replicator));
    return 0;
}

void TabletImpl::DropTable(RpcController* controller,
            const ::rtidb::api::DropTableRequest* request,
            ::rtidb::api::DropTableResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);        
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::rtidb::api::TaskType::kDropTable, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    PDLOG(INFO, "drop table. tid[%u] pid[%u]", tid, pid);
    do {
        std::shared_ptr<Table> table = GetTable(tid, pid);
        if (!table) {
            response->set_code(100);
            response->set_msg("table is not exist");
            break;
        } else {
            if (table->GetTableStat() == ::rtidb::storage::kMakingSnapshot) {
                PDLOG(WARNING, "making snapshot task is running now. tid[%u] pid[%u]", tid, pid);
                response->set_code(106);
                response->set_msg("table status is kMakingSnapshot");
                break;
            }
        }
        response->set_code(0);
        response->set_msg("ok");
        task_pool_.AddTask(boost::bind(&TabletImpl::DeleteTableInternal, this, tid, pid, task_ptr));
        return;
    } while (0);
    if (task_ptr) {       
        std::lock_guard<std::mutex> lock(mu_);
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
    }
}

void TabletImpl::GetTaskStatus(RpcController* controller,
        const ::rtidb::api::TaskStatusRequest* request,
        ::rtidb::api::TaskStatusResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : task_map_) {
        for (const auto& task_info : kv.second) {
            ::rtidb::api::TaskInfo* task = response->add_task();
            task->CopyFrom(*task_info);
        }
    }
    response->set_code(0);
    response->set_msg("ok");
}

void TabletImpl::DeleteOPTask(RpcController* controller,
        const ::rtidb::api::DeleteTaskRequest* request,
        ::rtidb::api::GeneralResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    for (int idx = 0; idx < request->op_id_size(); idx++) {
        auto iter = task_map_.find(request->op_id(idx));
        if (iter == task_map_.end()) {
            continue;
        }
        if (!iter->second.empty()) {
            PDLOG(INFO, "delete op task. op_id[%lu] op_type[%s] task_num[%u]", 
                        request->op_id(idx),
                        ::rtidb::api::OPType_Name(iter->second.front()->op_type()).c_str(),
                        iter->second.size());
            iter->second.clear();
        }
        task_map_.erase(iter);
    }
    response->set_code(0);
    response->set_msg("ok");
}

void TabletImpl::ConnectZK(RpcController* controller,
            const ::rtidb::api::ConnectZKRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (zk_client_->Reconnect() && zk_client_->Register()) {
        response->set_code(0);
        response->set_msg("ok");
        PDLOG(INFO, "connect zk ok"); 
        return;
    }
    response->set_code(-1);
    response->set_msg("connect failed");
}

void TabletImpl::DisConnectZK(RpcController* controller,
            const ::rtidb::api::DisConnectZKRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
    zk_client_->CloseZK();
    response->set_code(0);
    response->set_msg("ok");
    PDLOG(INFO, "disconnect zk ok"); 
    return;
}

void TabletImpl::SetConcurrency(RpcController* ctrl,
        const ::rtidb::api::SetConcurrencyRequest* request,
        ::rtidb::api::SetConcurrencyResponse* response,
        Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (server_ == NULL) {
        response->set_code(-1);
        response->set_msg("server is NULL");
        return;
    }

    if (request->max_concurrency() < 0) {
        response->set_code(135);
        response->set_msg("invalid concurrency " + request->max_concurrency());
        return;
    }

    if (SERVER_CONCURRENCY_KEY.compare(request->key()) == 0) {
        PDLOG(INFO, "update server max concurrency to %d", request->max_concurrency());
        server_->ResetMaxConcurrency(request->max_concurrency());
    }else {
        PDLOG(INFO, "update server api %s max concurrency to %d", request->key().c_str(), request->max_concurrency());
        server_->MaxConcurrencyOf(this, request->key()) = request->max_concurrency();
    }
    response->set_code(0);
    response->set_msg("ok");
}

int TabletImpl::AddOPTask(const ::rtidb::api::TaskInfo& task_info, ::rtidb::api::TaskType task_type,
            std::shared_ptr<::rtidb::api::TaskInfo>& task_ptr) {
    std::lock_guard<std::mutex> lock(mu_);
    if (FindTask(task_info.op_id(), task_info.task_type())) {
        PDLOG(WARNING, "task is running. op_id[%lu] op_type[%s] task_type[%s]", 
                        task_info.op_id(),
                        ::rtidb::api::OPType_Name(task_info.op_type()).c_str(),
                        ::rtidb::api::TaskType_Name(task_info.task_type()).c_str());
        return -1;
    }
    task_ptr.reset(task_info.New());
    task_ptr->CopyFrom(task_info);
    task_ptr->set_status(::rtidb::api::TaskStatus::kDoing);
    auto iter = task_map_.find(task_info.op_id());
    if (iter == task_map_.end()) {
        task_map_.insert(std::make_pair(task_info.op_id(), 
                std::list<std::shared_ptr<::rtidb::api::TaskInfo>>()));
    }
    task_map_[task_info.op_id()].push_back(task_ptr);
    if (task_info.task_type() != task_type) {
        PDLOG(WARNING, "task type is not match. type is[%s]", 
                        ::rtidb::api::TaskType_Name(task_info.task_type()).c_str());
        task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
        return -1;
    }
    return 0;
}

std::shared_ptr<::rtidb::api::TaskInfo> TabletImpl::FindTask(
        uint64_t op_id, ::rtidb::api::TaskType task_type) {
    auto iter = task_map_.find(op_id);
    if (iter == task_map_.end()) {
        return std::shared_ptr<::rtidb::api::TaskInfo>();
    }
    for (auto& task : iter->second) {
        if (task->op_id() == op_id && task->task_type() == task_type) {
            return task;
        }
    }
    return std::shared_ptr<::rtidb::api::TaskInfo>();
}

void TabletImpl::GcTable(uint32_t tid, uint32_t pid, bool execute_once) {
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (table) {
        int32_t gc_interval = FLAGS_gc_interval;
        if (table->GetStorageMode() != ::rtidb::common::StorageMode::kMemory) {
            gc_interval = FLAGS_disk_gc_interval;
        }
        table->SchedGc();
        if (!execute_once) {
            gc_pool_.DelayTask(gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid, false));
        }
        return;
    }
}

std::shared_ptr<Snapshot> TabletImpl::GetSnapshot(uint32_t tid, uint32_t pid) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetSnapshotUnLock(tid, pid);
}

std::shared_ptr<Snapshot> TabletImpl::GetSnapshotUnLock(uint32_t tid, uint32_t pid) {
    Snapshots::iterator it = snapshots_.find(tid);
    if (it != snapshots_.end()) {
        auto tit = it->second.find(pid);
        if (tit != it->second.end()) {
            return tit->second;
        }
    }
    return std::shared_ptr<Snapshot>();
}

std::shared_ptr<LogReplicator> TabletImpl::GetReplicatorUnLock(uint32_t tid, uint32_t pid) {
    Replicators::iterator it = replicators_.find(tid);
    if (it != replicators_.end()) {
        auto tit = it->second.find(pid);
        if (tit != it->second.end()) {
            return tit->second;
        }
    }
    return std::shared_ptr<LogReplicator>();
}

std::shared_ptr<LogReplicator> TabletImpl::GetReplicator(uint32_t tid, uint32_t pid) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetReplicatorUnLock(tid, pid);
}

std::shared_ptr<Table> TabletImpl::GetTable(uint32_t tid, uint32_t pid) {
    std::lock_guard<std::mutex> lock(mu_);
    return GetTableUnLock(tid, pid);
}

std::shared_ptr<Table> TabletImpl::GetTableUnLock(uint32_t tid, uint32_t pid) {
    Tables::iterator it = tables_.find(tid);
    if (it != tables_.end()) {
        auto tit = it->second.find(pid);
        if (tit != it->second.end()) {
            return tit->second;
        }
    }
    return std::shared_ptr<Table>();
}

void TabletImpl::ShowMemPool(RpcController* controller,
            const ::rtidb::api::HttpRequest* request,
            ::rtidb::api::HttpResponse* response,
            Closure* done) {
    brpc::ClosureGuard done_guard(done);
#ifdef TCMALLOC_ENABLE
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MallocExtension* tcmalloc = MallocExtension::instance();
    std::string stat;
    stat.resize(1024);
    char* buffer = reinterpret_cast<char*>(& (stat[0]));
    tcmalloc->GetStats(buffer, 1024);
    cntl->response_attachment().append("<html><head><title>Mem Stat</title></head><body><pre>");
    cntl->response_attachment().append(stat);
    cntl->response_attachment().append("</pre></body></html>");
#endif
}

void TabletImpl::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        PDLOG(WARNING, "reconnect zk"); 
        if (zk_client_->Reconnect() && zk_client_->Register()) {
            PDLOG(INFO, "reconnect zk ok"); 
        }
    } else if (!zk_client_->IsRegisted()) {
        PDLOG(WARNING, "registe zk"); 
        if (zk_client_->Register()) {
            PDLOG(INFO, "registe zk ok"); 
        }
    }
    keep_alive_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&TabletImpl::CheckZkClient, this));
}

int TabletImpl::CheckDimessionPut(const ::rtidb::api::PutRequest* request,
                                      uint32_t idx_cnt) {
    for (int32_t i = 0; i < request->dimensions_size(); i++) {
        if (idx_cnt <= request->dimensions(i).idx()) {
            PDLOG(WARNING, "invalid put request dimensions, request idx %u is greater than table idx cnt %u", 
                    request->dimensions(i).idx(), idx_cnt);
            return -1;
        }
        if (request->dimensions(i).key().length() <= 0) {
            PDLOG(WARNING, "invalid put request dimension key is empty with idx %u", request->dimensions(i).idx());
            return 1;
        }
    }
    return 0;
}

void TabletImpl::SchedSyncDisk(uint32_t tid, uint32_t pid) {
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (replicator) {
        replicator->SyncToDisk();
        io_pool_.DelayTask(FLAGS_binlog_sync_to_disk_interval, boost::bind(&TabletImpl::SchedSyncDisk, this, tid, pid));
    }
}

void TabletImpl::SchedDelBinlog(uint32_t tid, uint32_t pid) {
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (replicator) {
        replicator->DeleteBinlog();
        task_pool_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&TabletImpl::SchedDelBinlog, this, tid, pid));
    }
}

}
}




