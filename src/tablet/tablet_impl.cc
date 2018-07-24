//
// tablet_impl.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-01
//

#include "tablet/tablet_impl.h"

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
DECLARE_int32(gc_pool_size);
DECLARE_int32(gc_safe_offset);
DECLARE_int32(statdb_ttl);
DECLARE_uint32(scan_max_bytes_size);
DECLARE_uint32(scan_reserve_size);
DECLARE_double(mem_release_rate);
DECLARE_string(db_root_path);
DECLARE_bool(binlog_notify_on_put);
DECLARE_int32(task_pool_size);
DECLARE_int32(io_pool_size);
DECLARE_int32(make_snapshot_time);
DECLARE_int32(make_snapshot_check_interval);
DECLARE_string(recycle_bin_root_path);

DECLARE_int32(request_max_retry);
DECLARE_int32(request_timeout_ms);
DECLARE_int32(stream_wait_time_ms);
DECLARE_int32(stream_close_wait_time_ms);
DECLARE_int32(stream_block_size);
DECLARE_int32(stream_bandwidth_limit);
DECLARE_int32(send_file_max_try);
DECLARE_int32(retry_send_file_wait_time_ms);

// cluster config
DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

DECLARE_int32(binlog_sync_to_disk_interval);
DECLARE_int32(binlog_delete_interval);

namespace rtidb {
namespace tablet {

StreamReceiver::StreamReceiver(const std::string& file_name, uint32_t tid, uint32_t pid):
        file_name_(file_name), tid_(tid), pid_(pid), size_(0), file_(NULL) {}

StreamReceiver::~StreamReceiver() {
    if (file_) {
        fclose(file_);
    }    
    std::string combine_key = std::to_string(tid_) + "_" + std::to_string(pid_) + "_" + file_name_;
    stream_receiver_set_.erase(combine_key);
}

::rtidb::base::set<std::string> StreamReceiver::stream_receiver_set_;

int StreamReceiver::Init() {
    std::string combine_key = std::to_string(tid_) + "_" + std::to_string(pid_) + "_" + file_name_;
    if (stream_receiver_set_.contain(combine_key)) {
        PDLOG(WARNING, "stream is exist! tid[%u] pid[%u] file[%s]", tid_, pid_, file_name_.c_str());
        return -1;
    }
    stream_receiver_set_.insert(combine_key);
    std::string tmp_file_path = FLAGS_db_root_path + "/" + std::to_string(tid_) + "_" + std::to_string(pid_) + "/";
    if (file_name_ != "table_meta.txt") {
        tmp_file_path += "snapshot/";
    }
    if (!::rtidb::base::MkdirRecur(tmp_file_path)) {
        PDLOG(WARNING, "mkdir failed! path[%s]", tmp_file_path.c_str());
        return -1;
    }
    std::string full_path = tmp_file_path + file_name_ + ".tmp";
	FILE* file = fopen(full_path.c_str(), "wb");
    if (file == NULL) {
        PDLOG(WARNING, "fail to open file %s", full_path.c_str());
        return -1;
    }
    file_ = file;
    return 0;
}

int StreamReceiver::on_received_messages(brpc::StreamId id,
		butil::IOBuf *const messages[], size_t size) {
    if (file_ == NULL) {
        PDLOG(WARNING, "file is NULL");
    }
    for (size_t i = 0; i < size; i++) {
        if (messages[i]->empty()) {
            continue;
        }
        std::string data = messages[i]->to_string();
        size_t r = fwrite_unlocked(data.c_str(), 1, data.size(), file_);
        if (r < data.size()) {
            PDLOG(WARNING, "write error. tid[%u] pid[%u]", tid_, pid_);
        }
        size_ += r;
    }
	return 0;
}

void StreamReceiver::on_idle_timeout(brpc::StreamId id) {
    PDLOG(WARNING, "on_idle_timeout. tid %u pid %u", tid_, pid_);
}

void StreamReceiver::on_closed(brpc::StreamId id) {
    std::string full_path = FLAGS_db_root_path + "/" + std::to_string(tid_) + "_" + std::to_string(pid_) + "/";
    if (file_name_ != "table_meta.txt") {
        full_path += "snapshot/";
    }
	full_path += file_name_;
    std::string tmp_file_path = full_path + ".tmp";
    if (::rtidb::base::IsExists(full_path)) {
        std::string backup_file = full_path + "." + ::rtidb::base::GetNowTime();
        rename(full_path.c_str(), backup_file.c_str());
    }
    rename(tmp_file_path.c_str(), full_path.c_str());
    PDLOG(INFO, "file %s received. size %lu tid %u pid %u", file_name_.c_str(), size_, tid_, pid_);
    brpc::StreamClose(id);
    delete this;
}

TabletImpl::TabletImpl():tables_(),mu_(), gc_pool_(FLAGS_gc_pool_size),
    replicators_(), snapshots_(), zk_client_(NULL),
    keep_alive_pool_(1), task_pool_(FLAGS_task_pool_size),
    io_pool_(FLAGS_io_pool_size){}

TabletImpl::~TabletImpl() {
    task_pool_.Stop(true);
    keep_alive_pool_.Stop(true);
    gc_pool_.Stop(true);
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
    task_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
#ifdef TCMALLOC_ENABLE
    MallocExtension* tcmalloc = MallocExtension::instance();
    tcmalloc->SetMemoryReleaseRate(FLAGS_mem_release_rate);
#endif 
    return true;
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

void TabletImpl::Get(RpcController* controller,
             const ::rtidb::api::GetRequest* request,
             ::rtidb::api::GetResponse* response,
             Closure* done) {
    brpc::ClosureGuard done_guard(done);         
    if (request->tid() < 1) {
        PDLOG(WARNING, "invalid table tid %u", request->tid());
        response->set_code(11);
        response->set_msg("invalid table id");
        return;
    }

    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "fail to find table with tid %u, pid %u", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not found");
        return;
    }

    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table with tid %u, pid %u is unavailable now", 
                      request->tid(), request->pid());
        response->set_code(20);
        response->set_msg("table is unavailable now");
        return;
    }

    ::rtidb::storage::Ticket ticket;
    ::rtidb::storage::Iterator* it = NULL;
    if (request->has_idx_name() && request->idx_name().size() > 0) {
        std::map<std::string, uint32_t>::iterator iit = table->GetMapping().find(request->idx_name());
        if (iit == table->GetMapping().end()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(30);
            response->set_msg("idx name not found");
            return;
        }
        it = table->NewIterator(iit->second,
                                request->key(), ticket);
    } else {
        it = table->NewIterator(request->key(), ticket);
    }
    if (it == NULL) {
        response->set_code(30);
        response->set_msg("idx name not found");
        return;
    }
    ::rtidb::api::GetType get_type = ::rtidb::api::GetType::kSubKeyEq;
    if (request->has_type()) {
        get_type = request->type();
    }
    bool has_found = true;
    // filter with time
    if (request->ts() > 0) {
        if (table->GetTTLType() == ::rtidb::api::TTLType::kLatestTime) {
            uint64_t keep_cnt = table->GetTTL();
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
        } else if (request->ts() > table->GetExpireTime()) {
            it->Seek(request->ts());
            if (it->Valid() && it->GetKey() != request->ts()) {
                has_found = false;
            }
        }

    } else {
        it->SeekToFirst();
        if (it->Valid() && table->GetTTLType() == ::rtidb::api::TTLType::kAbsoluteTime) {
            if (it->GetKey() <= table->GetExpireTime()) {
                has_found = false;
            }
        }
    }
    if (it->Valid() && has_found) {
        response->set_code(0);
        response->set_msg("ok");
        response->set_key(request->key());
        response->set_ts(it->GetKey());
        response->set_value(it->GetValue()->data, it->GetValue()->size);
        PDLOG(DEBUG, "Get key %s ts %lu value %s", request->key().c_str(),
                request->ts(), it->GetValue()->data);
    } else {
        response->set_code(1);
        response->set_msg("Not Found");
        PDLOG(DEBUG, "not found key %s ts %lu ", request->key().c_str(),
                request->ts());

    }
    delete it; 
}

void TabletImpl::Put(RpcController* controller,
        const ::rtidb::api::PutRequest* request,
        ::rtidb::api::PutResponse* response,
        Closure* done) {
    if (request->tid() < 1) {
        PDLOG(WARNING, "invalid table tid %u", request->tid());
        response->set_code(11);
        response->set_msg("invalid table id");
        done->Run();
        return;
    }
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "fail to find table with tid %u, pid %u", request->tid(),
                request->pid());
        response->set_code(10);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    if (!table->IsLeader()) {
        PDLOG(WARNING, "table with tid %u, pid %u is follower and it's readonly ", request->tid(),
                request->pid());
        response->set_code(20);
        response->set_msg("table is follower, and it's readonly");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table with tid %u, pid %u is unavailable now", 
                      request->tid(), request->pid());
        response->set_code(20);
        response->set_msg("table is unavailable now");
        done->Run();
        return;
    }
    bool ok = false;
    if (request->dimensions_size() > 0) {
        int32_t ret_code = CheckDimessionPut(request, table);
        if (ret_code != 0) {
            response->set_code(ret_code);
            response->set_msg("invalid dimension parameter");
            done->Run();
            return;
        }
        ok = table->Put(request->time(), 
                   request->value(),
                   request->dimensions());
    } else {
        ok = table->Put(request->pk(), 
                   request->time(), 
                   request->value().c_str(),
                   request->value().size());
        PDLOG(DEBUG, "put key %s ok ts %lld", request->pk().c_str(), request->time());
    }
    if (!ok) {
        response->set_code(-1);
        response->set_msg("put failed");
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
        replicator->AppendEntry(entry);
    } while(false);
    done->Run();
    if (replicator) {
        if (FLAGS_binlog_notify_on_put) {
            replicator->Notify(); 
        }
    }
}

inline bool TabletImpl::CheckScanRequest(const rtidb::api::ScanRequest* request) {
    if (request->st() < request->et()) {
        return false;
    }
    return true;
}

inline bool TabletImpl::CheckTableMeta(const rtidb::api::TableMeta* table_meta) {
    if (table_meta->name().size() <= 0) {
        return false;
    }
    if (table_meta->tid() <= 0) {
        return false;
    }
    return true;
}

void TabletImpl::Scan(RpcController* controller,
              const ::rtidb::api::ScanRequest* request,
              ::rtidb::api::ScanResponse* response,
              Closure* done) {

    if (!CheckScanRequest(request)) {
        response->set_code(8);
        response->set_msg("bad scan request");
        done->Run();
        return;
    }

    ::rtidb::api::RpcMetric* metric = response->mutable_metric();
    metric->CopyFrom(request->metric());
    metric->set_rqtime(::baidu::common::timer::get_micros());
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "fail to find table with tid %u, pid %u", request->tid(), request->pid());
        response->set_code(10);
        response->set_msg("table not found");
        done->Run();
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        PDLOG(WARNING, "table with tid %u, pid %u is unavailable now", 
                      request->tid(), request->pid());
        response->set_code(20);
        response->set_msg("table is unavailable now");
        done->Run();
        return;
    }
    if (table->GetTTLType() == ::rtidb::api::TTLType::kLatestTime) {
        response->set_code(21);
        response->set_msg("table ttl type is kLatestTime, cannot scan");
        done->Run();
        return;
    }

    metric->set_sctime(::baidu::common::timer::get_micros());
    // Use seek to process scan request
    // the first seek to find the total size to copy
    ::rtidb::storage::Ticket ticket;
    ::rtidb::storage::Iterator* it = NULL;
    if (request->has_idx_name() && request->idx_name().size() > 0) {
        std::map<std::string, uint32_t>::iterator iit = table->GetMapping().find(request->idx_name());
        if (iit == table->GetMapping().end()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(),
                  request->tid(), request->pid());
            response->set_code(30);
            response->set_msg("idx name not found");
            done->Run();
            return;
        }
        it = table->NewIterator(iit->second,
                                request->pk(), ticket);
    }else {
        it = table->NewIterator(request->pk(), ticket);
    }
    if (it == NULL) {
        response->set_code(30);
        response->set_msg("idx name not found");
        done->Run();
        return;
    }
    if (request->st() == 0) {
        it->SeekToFirst();
    } else {
        it->Seek(request->st());
    }
    metric->set_sitime(::baidu::common::timer::get_micros());
    std::vector<std::pair<uint64_t, DataBlock*> > tmp;
    // reduce the times of memcpy in vector
    tmp.reserve(FLAGS_scan_reserve_size);
    uint32_t total_block_size = 0;
    uint64_t end_time = request->et();
    bool remove_duplicated_record = false;
    if (request->has_enable_remove_duplicated_record()) {
        remove_duplicated_record = request->enable_remove_duplicated_record();
    }
    PDLOG(DEBUG, "scan pk %s st %lld et %lld", request->pk().c_str(), request->st(), end_time);
    uint32_t scount = 0;
    uint64_t last_time = 0;
    end_time = std::max(end_time, table->GetExpireTime());
    PDLOG(DEBUG, "end_time %lu expire_time %lu", end_time, table->GetExpireTime());
    while (it->Valid()) {
        scount ++;
        PDLOG(DEBUG, "scan key %lld", it->GetKey());
        if (it->GetKey() <= end_time) {
            break;
        }
        // skip duplicate record 
        if (remove_duplicated_record && scount > 1 && last_time == it->GetKey()) {
            PDLOG(DEBUG, "filter duplicate record for key %s with ts %lld", request->pk().c_str(), it->GetKey());
            last_time = it->GetKey();
            it->Next();
            continue;
        }
        last_time = it->GetKey();
        tmp.push_back(std::make_pair(it->GetKey(), it->GetValue()));
        total_block_size += it->GetValue()->size;
        it->Next();
        if (request->limit() > 0 && request->limit() <= scount) {
            PDLOG(DEBUG, "reache the limit %u ", request->limit());
            break;
        }
    }
    delete it;
    metric->set_setime(::baidu::common::timer::get_micros());
    uint32_t total_size = tmp.size() * (8+4) + total_block_size;
    // check reach the max bytes size
    if (total_size > FLAGS_scan_max_bytes_size) {
        response->set_code(31);
        response->set_msg("reache the scan max bytes size " + ::rtidb::base::HumanReadableString(total_size));
        done->Run();
        return;
    }
    std::string* pairs = response->mutable_pairs();
    if (tmp.size() <= 0) {
        pairs->resize(0);
    }else {
        pairs->resize(total_size);
    }
    PDLOG(DEBUG, "scan count %d", tmp.size());
    char* rbuffer = reinterpret_cast<char*>(& ((*pairs)[0]));
    uint32_t offset = 0;
    std::vector<std::pair<uint64_t, DataBlock*> >::iterator lit = tmp.begin();
    for (; lit != tmp.end(); ++lit) {
        std::pair<uint64_t, DataBlock*>& pair = *lit;
        PDLOG(DEBUG, "decode key %lld value %s size %u", pair.first, pair.second->data, pair.second->size);
        ::rtidb::base::Encode(pair.first, pair.second, rbuffer, offset);
        offset += (4 + 8 + pair.second->size);
    }

    response->set_code(0);
    response->set_count(tmp.size());
    metric->set_sptime(::baidu::common::timer::get_micros()); 
    done->Run();
}

void TabletImpl::ChangeRole(RpcController* controller, 
            const ::rtidb::api::ChangeRoleRequest* request,
            ::rtidb::api::ChangeRoleResponse* response,
            Closure* done) {
	brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    bool is_leader = false;
    if (request->mode() == ::rtidb::api::TableMode::kTableLeader) {
        is_leader = true;
    }
    std::vector<std::string> vec;
    for (int idx = 0; idx < request->replicas_size(); idx++) {
        vec.push_back(request->replicas(idx).c_str());
    }
    if (is_leader) {
        if (ChangeToLeader(tid, pid, vec, request->term()) < 0) {
            response->set_code(-1);
            response->set_msg("table change to leader failed!");
            return;
        }
    } else {
        std::shared_ptr<Table> table = GetTable(tid, pid);
        if (!table) {
            response->set_code(-1);
            response->set_msg("table is not exist");
            return;
        }
        if (!table->IsLeader()) {
            PDLOG(WARNING, "table is follower. tid[%u] pid[%u]", tid, pid);
            response->set_code(0);
            response->set_msg("table is follower.");
            return;
        }
        if (table->GetTableStat() != ::rtidb::storage::kNormal) {
            PDLOG(WARNING, "table state[%u] can not change role. tid[%u] pid[%u]", 
                        table->GetTableStat(), tid, pid);
            response->set_code(-1);
            response->set_msg("can not change role");
            return;
        }
        std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
        if (!replicator) {
            response->set_code(-1);
            response->set_msg("replicator is not exist");
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

int TabletImpl::ChangeToLeader(uint32_t tid, uint32_t pid, const std::vector<std::string>& replicas, uint64_t term) {
    std::shared_ptr<Table> table;
    std::shared_ptr<LogReplicator> replicator;
    {
        std::lock_guard<std::mutex> lock(mu_);
        table = GetTableUnLock(tid, pid);
        if (!table) {
            PDLOG(WARNING, "table is not exist. tid[%u] pid[%u]", tid, pid);
            return -1;
        }
        if (table->IsLeader() || table->GetTableStat() != ::rtidb::storage::kNormal) {
            PDLOG(WARNING, "table is leader or  state[%u] can not change role. tid[%u] pid[%u]", 
                        table->GetTableStat(), tid, pid);
            return -1;
        }
        replicator = GetReplicatorUnLock(tid, pid);
        if (!replicator) {
            PDLOG(WARNING,"no replicator for table tid[%u] pid[%u]", tid, pid);
            return -1;
        }
        PDLOG(INFO, "change to leader. tid[%u] pid[%u] term[%lu]", tid, pid, term);
        table->SetLeader(true);
        table->SetReplicas(replicas);
        replicator->SetRole(ReplicatorRole::kLeaderNode);
        if (!FLAGS_zk_cluster.empty()) {
            replicator->SetLeaderTerm(term);
        }
    }
    if (!replicator->AddReplicateNode(replicas)) {
        PDLOG(WARNING,"add replicator failed. tid[%u] pid[%u]", tid, pid);
    }
    return 0;
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
        if (!table || !table->IsLeader()) {
            PDLOG(WARNING, "table not exist or table is not leader tid %u, pid %u", request->tid(),
                    request->pid());
            response->set_code(-1);
            response->set_msg("table not exist or table is leader");
            break;
        }
        std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
        if (!replicator) {
            response->set_code(-2);
            response->set_msg("no replicator for table");
            PDLOG(WARNING,"no replicator for table %u, pid %u", request->tid(), request->pid());
            break;
        }
        std::vector<std::string> vec;
        vec.push_back(request->endpoint());
        int ret = replicator->AddReplicateNode(vec);
        if (ret == 0) {
            response->set_code(0);
            response->set_msg("ok");
        } else if (ret < 0) {
            response->set_code(-3);
            PDLOG(WARNING, "fail to add endpoint for table %u pid %u", request->tid(), request->pid());
            response->set_msg("fail to add endpoint");
            break;
        } else {
            response->set_code(-4);
            response->set_msg("replica endpoint is exist");
            PDLOG(WARNING, "fail to add endpoint for table %u pid %u", request->tid(), request->pid());
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
        if (!table || !table->IsLeader()) {
            PDLOG(WARNING, "table not exist or table is not leader tid %u, pid %u", request->tid(),
                    request->pid());
            response->set_code(-1);
            response->set_msg("table not exist or table is leader");
            break;
        }
        std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
        if (!replicator) {
            response->set_code(-2);
            response->set_msg("no replicator for table");
            PDLOG(WARNING,"no replicator for table %u, pid %u", request->tid(), request->pid());
            break;
        }
        int ret = replicator->DelReplicateNode(request->endpoint());
        if (ret == 0) {
            response->set_code(0);
            response->set_msg("ok");
        } else if (ret < 0) {
            response->set_code(-3);
            PDLOG(WARNING, "replicator role is not leader. table %u pid %u", request->tid(), request->pid());
            response->set_msg("replicator role is not leader");
            break;
        } else {
            response->set_code(-4);
            PDLOG(WARNING, "fail to del endpoint for table %u pid %u", request->tid(), request->pid());
            response->set_msg("fail to del endpoint");
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
    if (!table || table->IsLeader()) {
        PDLOG(WARNING, "table not exist or table is leader tid %u, pid %u", request->tid(),
                request->pid());
        response->set_code(-1);
        response->set_msg("table not exist or table is leader");
        return;
    }
    if (table->GetTableStat() == ::rtidb::storage::kLoading) {
        response->set_code(-1);
        response->set_msg("table is loading now");
        PDLOG(WARNING, "table is loading now. tid %u, pid %u", request->tid(), request->pid());
        return;
    }    
    std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
    if (!replicator) {
        response->set_code(-1);
        response->set_msg("no replicator for table");
        return;
    }
    bool ok = replicator->AppendEntries(request, response);
    if (!ok) {
        response->set_code(-1);
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
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        response->set_code(-1);
        response->set_msg("table not found");
        PDLOG(WARNING, "fail to find table with tid %u, pid %u", request->tid(),
                request->pid());
        done->Run();
        return;
    }
    response->set_code(0);
    response->set_msg("ok");
    response->set_schema(table->GetSchema());
    done->Run();
}

void TabletImpl::GetTableStatus(RpcController* controller,
            const ::rtidb::api::GetTableStatusRequest* request,
            ::rtidb::api::GetTableStatusResponse* response,
            Closure* done) {
    std::lock_guard<std::mutex> lock(mu_);        
    Tables::iterator it = tables_.begin();
    for (; it != tables_.end(); ++it) {
        auto pit = it->second.begin();
        for (; pit != it->second.end(); ++pit) {
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
            status->set_time_offset(table->GetTimeOffset());
            status->set_is_expire(table->GetExpireStatus());
            status->set_name(table->GetName());
            if (::rtidb::api::TableState_IsValid(table->GetTableStat())) {
                status->set_state(::rtidb::api::TableState(table->GetTableStat()));
            }
            status->set_record_cnt(table->GetRecordCnt());
            status->set_record_byte_size(table->GetRecordByteSize());
            status->set_record_idx_byte_size(table->GetRecordIdxByteSize());
            status->set_record_pk_cnt(table->GetRecordPkCnt());
            uint64_t record_idx_cnt = 0;
            std::map<std::string, uint32_t>::iterator iit = table->GetMapping().begin();
            for (;iit != table->GetMapping().end(); ++iit) {
                ::rtidb::api::TsIdxStatus* ts_idx_status = status->add_ts_idx_status();
                ts_idx_status->set_idx_name(iit->first);
                uint64_t* stats = NULL;
                uint32_t size = 0;
                bool ok = table->GetRecordIdxCnt(iit->second, &stats, &size);
                if (ok) {
                    for (uint32_t i = 0; i < size; i++) {
                        ts_idx_status->add_seg_cnts(stats[i]); 
                        record_idx_cnt += stats[i];
                    }
                }
                delete stats;
            }
            status->set_idx_cnt(record_idx_cnt);
            std::shared_ptr<LogReplicator> replicator = GetReplicatorUnLock(table->GetId(), table->GetPid());
            if (replicator) {
                status->set_offset(replicator->GetOffset());
            }
        }
    }
    response->set_code(0);
    done->Run();
}

void TabletImpl::SetExpire(RpcController* controller,
            const ::rtidb::api::SetExpireRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(-1);
        response->set_msg("table not exist");
        done->Run();
        return;
    }
	table->SetExpire(request->is_expire());
    PDLOG(INFO, "set table expire[%d]. tid[%u] pid[%u]", request->is_expire(), request->tid(), request->pid());
	response->set_code(0);
	response->set_msg("ok");
	done->Run();
}

void TabletImpl::SetTTLClock(RpcController* controller,
            const ::rtidb::api::SetTTLClockRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(-1);
        response->set_msg("table not exist");
        done->Run();
        return;
    }
    int64_t cur_time = ::baidu::common::timer::get_micros() / 1000000;
    int64_t offset = (int64_t)request->timestamp() - cur_time;
	table->SetTimeOffset(offset);
    PDLOG(INFO, "set table virtual timestamp[%lu] cur timestamp[%lu] offset[%ld]. tid[%u] pid[%u]", 
                request->timestamp(), cur_time, offset, request->tid(), request->pid());
	response->set_code(0);
	response->set_msg("ok");
	done->Run();
}

void TabletImpl::MakeSnapshotInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::rtidb::api::TaskInfo> task) {
    std::shared_ptr<Table> table;
    std::shared_ptr<Snapshot> snapshot;
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
    uint64_t offset = 0;
    int ret = snapshot->MakeSnapshot(table, offset);
    if (ret == 0) {
        std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
        if (replicator) {
            replicator->SetSnapshotLogPartIndex(offset);
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
            response->set_code(-1);
            response->set_msg("snapshot is not exist!");
            PDLOG(WARNING, "snapshot is not exist! tid[%u] pid[%u]", tid, pid);
            break;
        }
        std::shared_ptr<Table> table = GetTableUnLock(request->tid(), request->pid());
        if (!table) {
            PDLOG(WARNING, "fail to find table with tid %u, pid %u", tid, pid);
            response->set_code(-1);
            response->set_msg("table not found");
            break;
        }
        if (table->GetTableStat() != ::rtidb::storage::kNormal) {
            response->set_code(-1);
            response->set_msg("table status is not normal");
            PDLOG(WARNING, "table state is %d, cannot make snapshot. %u, pid %u", 
                         table->GetTableStat(), tid, pid);
            break;
        }
        if (task_ptr) {       
            task_ptr->set_status(::rtidb::api::TaskStatus::kDoing);
        }    
        task_pool_.AddTask(boost::bind(&TabletImpl::MakeSnapshotInternal, this, tid, pid, task_ptr));
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
        task_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
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
    task_pool_.DelayTask(FLAGS_make_snapshot_check_interval + 60 * 60 * 1000, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
}

void TabletImpl::CreateStream(RpcController* controller,
            const ::rtidb::api::CreateStreamRequest* request,
            ::rtidb::api::GeneralResponse* response,
            Closure* done) {
	brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller*>(controller);
    std::lock_guard<std::mutex> lock(mu_);
    uint32_t tid = request->tid(); 
    uint32_t pid = request->pid(); 
    std::shared_ptr<Table> table = GetTableUnLock(tid, pid);
    if (table) {
        PDLOG(WARNING, "table is exist. tid %u, pid %u", tid, pid);
        response->set_code(-1);
        response->set_msg("table is exist");
        return;
    }
    StreamReceiver* stream_receiver = new StreamReceiver(request->file_name(), tid, pid);
	if (stream_receiver->Init() < 0) {
        delete stream_receiver;
        response->set_code(-1);
        response->set_msg("stream_receiver init failed");
        PDLOG(WARNING, "init stream_receiver failed. remote side %s file %s", 
                        butil::endpoint2str(cntl->remote_side()).c_str(), request->file_name().c_str());
        return;
    }
    brpc::StreamOptions stream_options;
    stream_options.handler = stream_receiver;
    brpc::StreamId sd;
    if (brpc::StreamAccept(&sd, *cntl, &stream_options) != 0) {
        delete stream_receiver;
        cntl->SetFailed("Fail to accept stream");
        PDLOG(WARNING, "accept stream from %s failed. file %s", 
                        butil::endpoint2str(cntl->remote_side()).c_str(), 
                        request->file_name().c_str());
        return;
    }
    PDLOG(INFO, "create stream succeed. remote side %s file %s tid %u pid %u", 
                 butil::endpoint2str(cntl->remote_side()).c_str(),
                 request->file_name().c_str(), tid, pid);
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
			response->set_code(-1);
			response->set_msg("snapshot is sending");
            break;
        }
		if (!table) {
			PDLOG(WARNING, "table not exist. tid %u, pid %u", tid, pid);
			response->set_code(-1);
			response->set_msg("table not exist");
			break;
		}
		if (!table->IsLeader()) {
			PDLOG(WARNING, "table with tid %u, pid %u is follower", tid, pid);
			response->set_code(-1);
			response->set_msg("table is follower");
			break;
		}
		if (table->GetTableStat() != ::rtidb::storage::kSnapshotPaused) {
			PDLOG(WARNING, "table with tid %u, pid %u is not kSnapshotPaused", tid, pid);
			response->set_code(-1);
			response->set_msg("table is unavailable now");
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
    do {
		// send table_meta file
		if (SendFile(endpoint, tid, pid, "table_meta.txt") < 0) {
			PDLOG(WARNING, "send table_meta.txt failed. tid[%u] pid[%u]", tid, pid);
			break;
		}
    	std::string manifest_file = FLAGS_db_root_path + "/" + std::to_string(tid) + "_" + 
									std::to_string(pid) + "/snapshot/MANIFEST";
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
        // send snapshot file
        if (SendFile(endpoint, tid, pid, snapshot_file) < 0) {
            PDLOG(WARNING, "send snapshot failed. tid[%u] pid[%u]", tid, pid);
            break;
        }
        // send manifest file
        if (SendFile(endpoint, tid, pid, "MANIFEST") < 0) {
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

int TabletImpl::SendFile(const std::string& endpoint, uint32_t tid, uint32_t pid,
            const std::string& file_name) {
    std::string full_path = FLAGS_db_root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid) + "/";
    if (file_name != "table_meta.txt") {
        full_path += "snapshot/";
    }
    full_path += file_name;
    uint64_t file_size = 0;
    if (::rtidb::base::GetSize(full_path, file_size) < 0) {
        PDLOG(WARNING, "get size failed. file[%s]", full_path.c_str());
        return -1;
    }
    PDLOG(INFO, "send file %s to %s. size[%lu]", full_path.c_str(), endpoint.c_str(), file_size);
    int try_times = FLAGS_send_file_max_try;   
    do {
        if (try_times < FLAGS_send_file_max_try) {
            std::this_thread::sleep_for(std::chrono::milliseconds((FLAGS_send_file_max_try - try_times) * 
                    FLAGS_retry_send_file_wait_time_ms));
            PDLOG(INFO, "retry to send file %s to %s. total size[%lu]", full_path.c_str(), endpoint.c_str(), file_size);
        }
        try_times--;
        brpc::Channel channel;
        brpc::ChannelOptions options;
        SleepRetryPolicy sleep_retry_policy;
        options.retry_policy = &sleep_retry_policy;
        options.protocol = brpc::PROTOCOL_BAIDU_STD;
        options.timeout_ms = FLAGS_request_timeout_ms;
        options.connect_timeout_ms = FLAGS_request_timeout_ms;
        options.max_retry = FLAGS_request_max_retry;
        if (channel.Init(endpoint.c_str(), "", &options) != 0) {
            PDLOG(WARNING, "init channel failed. endpoint[%s] tid[%u] pid[%u]", 
                            endpoint.c_str(), tid, pid);
            continue;
        }
        ::rtidb::api::TabletServer_Stub stub(&channel);
        brpc::Controller cntl;
        brpc::StreamId stream;
        cntl.set_timeout_ms(FLAGS_request_timeout_ms);
        cntl.set_max_retry(FLAGS_request_max_retry);
        if (brpc::StreamCreate(&stream, cntl, NULL) != 0) {
            PDLOG(WARNING, "create stream failed. endpoint[%s] tid[%u] pid[%u]", 
                            endpoint.c_str(), tid, pid);
            continue;
        }
        FILE* file = fopen(full_path.c_str(), "rb");
        if (file == NULL) {
            PDLOG(WARNING, "fail to open file %s", full_path.c_str());
            return -1;
        }
        ::rtidb::api::CreateStreamRequest request;
        ::rtidb::api::GeneralResponse response;
        request.set_tid(tid);
        request.set_pid(pid);
        request.set_file_name(file_name);
        stub.CreateStream(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            PDLOG(WARNING, "connect stream failed. tid[%u] pid[%u] error msg: %s", 
                            tid, pid, cntl.ErrorText().c_str());
            fclose(file);
            continue;
        }
        char buffer[FLAGS_stream_block_size];
        // compute the used time(microseconds) that send a block by limit bandwidth. 
        // limit_time = (FLAGS_stream_block_size / FLAGS_stream_bandwidth_limit) * 1000 * 1000 
        uint64_t limit_time = 0;
        if (FLAGS_stream_bandwidth_limit > 0) {
            limit_time = ((uint64_t)FLAGS_stream_block_size * 1000000) / FLAGS_stream_bandwidth_limit;
        }    
        uint64_t block_num = file_size / (uint64_t)FLAGS_stream_block_size + 1;
        uint64_t report_block_num = block_num / 100;
        int ret = 0;
        uint64_t block_count = 0;
        while (true) {
            size_t len = fread_unlocked(buffer, 1, FLAGS_stream_block_size, file);
            if (len < (uint32_t)FLAGS_stream_block_size) {
                if (feof(file)) {
                    if (len > 0) {
                        ret = StreamWrite(stream, buffer, len, limit_time);
                    }
                    break;
                }
                PDLOG(WARNING, "read file %s error. error message: %s", file_name.c_str(), strerror(errno));
                ret = -1;
                break;
            }
            if (StreamWrite(stream, buffer, len, limit_time) < 0) {
                PDLOG(WARNING, "stream write failed. tid[%u] pid[%u] file %s", tid, pid, file_name.c_str());
                ret = -1;
                break;
            }
            block_count++;
            if (report_block_num == 0 || block_count % report_block_num == 0) {
                PDLOG(INFO, "send block num[%lu] total block num[%lu]. tid[%u] pid[%u] file[%s]", 
                            block_count, block_num, tid, pid, file_name.c_str());
            }
        }
        fclose(file);
        brpc::StreamClose(stream);
        if (ret == -1) {
            continue;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_stream_close_wait_time_ms));
        // check file
        ::rtidb::api::CheckFileRequest check_request;
        check_request.set_tid(tid);
        check_request.set_pid(pid);
        check_request.set_file(file_name);
        check_request.set_size(file_size);
        brpc::Controller cntl1;
        stub.CheckFile(&cntl1, &check_request, &response, NULL);
        if (cntl1.Failed()) {
            PDLOG(WARNING, "check file[%s] request failed. tid[%u] pid[%u] error msg: %s", 
                            file_name.c_str(), tid, pid, cntl1.ErrorText().c_str());
            continue;
        }
        if (response.code() == 0) {
            PDLOG(INFO, "send file[%s] success. tid[%u] pid[%u]", file_name.c_str(), tid, pid);
            return 0;
        }
        PDLOG(WARNING, "check file[%s] failed. tid[%u] pid[%u]", file_name.c_str(), tid, pid);
    } while (try_times > 0);
    return -1;
}

int TabletImpl::StreamWrite(brpc::StreamId stream, char* buffer, size_t len, uint64_t limit_time) {
    if (buffer == NULL) {
        return -1;
    }
    uint64_t cur_time = ::baidu::common::timer::get_micros();
	butil::IOBuf data;
    data.clear();
    data.append(buffer, len);
    while (true) {
        int ret_code = brpc::StreamWrite(stream, data);
        if (ret_code == 0) {
            break;
        } else if (ret_code == EAGAIN) {
            const timespec duetime = butil::milliseconds_from_now(FLAGS_stream_wait_time_ms);
            if (brpc::StreamWait(stream, &duetime) == EINVAL) {
                PDLOG(WARNING, "stream closed!");
                return -1;
            }
            PDLOG(DEBUG, "ret_code is EAGAIN. wait %d milliseconds", FLAGS_stream_wait_time_ms);
        } else { 
            PDLOG(WARNING, "stream write failed");
            return -1;
        }
    }
    uint64_t time_used = ::baidu::common::timer::get_micros() - cur_time;
    if (limit_time > time_used && len > (uint64_t)FLAGS_stream_block_size / 2) {
        PDLOG(DEBUG, "sleep %lu us, limit_time %lu time_used %lu", limit_time - time_used, limit_time, time_used);
        std::this_thread::sleep_for(std::chrono::microseconds(limit_time - time_used));
    }
    return 0;
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
            PDLOG(WARNING, "table not exist. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(-1);
            response->set_msg("table not exist");
            break;
        }
        if (table->GetTableStat() == ::rtidb::storage::kSnapshotPaused) {
            PDLOG(INFO, "table status is kSnapshotPaused, need not pause. tid[%u] pid[%u]", 
                        request->tid(), request->pid());
        } else if (table->GetTableStat() != ::rtidb::storage::kNormal) {
            PDLOG(WARNING, "table status is [%u], cann't pause. tid[%u] pid[%u]", 
                            table->GetTableStat(), request->tid(), request->pid());
            response->set_code(-1);
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
            PDLOG(WARNING, "table not exist tid %u, pid %u", request->tid(), request->pid());
            response->set_code(-1);
            response->set_msg("table not exist");
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
                response->set_code(-1);
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
        if (!CheckTableMeta(&table_meta)) {
            response->set_code(8);
            response->set_msg("table name is empty");
            break;
        }
        uint32_t tid = table_meta.tid();
        uint32_t pid = table_meta.pid();

        std::string db_path = FLAGS_db_root_path + "/" + std::to_string(tid) + 
                        "_" + std::to_string(pid);
        if (!::rtidb::base::IsExists(db_path)) {
            PDLOG(WARNING, "no db data for table tid %u, pid %u", tid, pid);
            response->set_code(-1);
            response->set_msg("no db data for table");
            break;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::shared_ptr<Table> table = GetTableUnLock(tid, pid);
            if (!table) {
                UpdateTableMeta(db_path, &table_meta);
                if (WriteTableMeta(db_path, &table_meta) < 0) {
                    PDLOG(WARNING, "write table_meta failed. tid[%lu] pid[%lu]", tid, pid);
                    response->set_code(-1);
                    response->set_msg("write table_meta failed");
                    break;
                }
                std::string msg;
                if (CreateTableInternal(&table_meta, msg) < 0) {
                    response->set_code(-1);
                    response->set_msg(msg.c_str());
                    break;
                }
            } else {
                response->set_code(1);
                response->set_msg("table with tid and pid exists");
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
            if (table->GetTTL() > 0) {
                gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid));
            }
            io_pool_.DelayTask(FLAGS_binlog_sync_to_disk_interval, boost::bind(&TabletImpl::SchedSyncDisk, this, tid, pid));
            io_pool_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&TabletImpl::SchedDelBinlog, this, tid, pid));
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
    if (!table) {
        if (task_ptr) {
            std::lock_guard<std::mutex> lock(mu_);
            task_ptr->set_status(::rtidb::api::TaskStatus::kFailed);
        }        
        return -1;
    }
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

    std::string source_path = FLAGS_db_root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid);

    if (!::rtidb::base::IsExists(source_path)) {
        if (task_ptr) {
            std::lock_guard<std::mutex> lock(mu_);
            task_ptr->set_status(::rtidb::api::TaskStatus::kDone);
        }        
        PDLOG(INFO, "drop table ok. tid[%u] pid[%u]", tid, pid);
        return 0;
    }

    std::string recycle_path = FLAGS_recycle_bin_root_path + "/" + std::to_string(tid) + 
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
    const ::rtidb::api::TableMeta* table_meta = &request->table_meta();
    if (!CheckTableMeta(table_meta)) {
        response->set_code(8);
        response->set_msg("table name is empty");
        done->Run();
        return;
    }
    uint32_t tid = table_meta->tid();
    uint32_t pid = table_meta->pid();
    ::rtidb::api::TTLType type = table_meta->ttl_type();
    PDLOG(INFO, "start creating table tid[%u] pid[%u] with mode %s", tid, pid, ::rtidb::api::TableMode_Name(request->table_meta().mode()).c_str());
    uint64_t ttl = table_meta->ttl();
    std::string name = table_meta->name();
    uint32_t seg_cnt = 8;
    if (table_meta->seg_cnt() > 0) {
        seg_cnt = table_meta->seg_cnt();
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        std::shared_ptr<Table> table = GetTableUnLock(tid, pid);
        std::shared_ptr<Snapshot> snapshot = GetSnapshotUnLock(tid, pid);
        if (table || snapshot) {
            if (table) {
                PDLOG(WARNING, "table with tid[%u] and pid[%u] exists", tid, pid);
            }
            if (snapshot) {
                PDLOG(WARNING, "snapshot with tid[%u] and pid[%u] exists", tid, pid);
            }
            response->set_code(1);
            response->set_msg("table with tid and pid exists");
            done->Run();
            return;
        }       
    	std::string table_db_path = FLAGS_db_root_path + "/" + std::to_string(tid) +
                        "_" + std::to_string(pid);
		if (WriteTableMeta(table_db_path, table_meta) < 0) {
        	PDLOG(WARNING, "write table_meta failed. tid[%lu] pid[%lu]", tid, pid);
            response->set_code(-1);
            response->set_msg("write table_meta failed");
            done->Run();
            return;
		}
        std::string msg;
        if (CreateTableInternal(table_meta, msg) < 0) {
            response->set_code(-1);
            response->set_msg(msg.c_str());
            done->Run();
            return;
        }
    }
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
    std::shared_ptr<Table> table = GetTable(tid, pid);        
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
    io_pool_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&TabletImpl::SchedDelBinlog, this, tid, pid));
    PDLOG(INFO, "create table with id %u pid %u name %s seg_cnt %d ttl %llu type %s", tid, 
            pid, name.c_str(), seg_cnt, ttl, ::rtidb::api::TTLType_Name(type).c_str());
    if (ttl > 0) {
        gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid));
        PDLOG(INFO, "table %s with tid %u pid %u enable ttl %llu", name.c_str(), tid, pid, ttl);
    }
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
		response->set_code(-1);
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
    uint32_t seg_cnt = 8;
    std::string name = table_meta->name();
    if (table_meta->seg_cnt() > 0) {
        seg_cnt = table_meta->seg_cnt();
    }
    bool is_leader = false;
    if (table_meta->mode() == ::rtidb::api::TableMode::kTableLeader) {
        is_leader = true;
    }
    std::vector<std::string> endpoints;
    for (int32_t i = 0; i < table_meta->replicas_size(); i++) {
        endpoints.push_back(table_meta->replicas(i));
    }
    // config dimensions
    std::map<std::string, uint32_t> mapping;
    for (int32_t i = 0; i < table_meta->dimensions_size(); i++) {
        mapping.insert(std::make_pair(table_meta->dimensions(i), 
                       (uint32_t)i));
        PDLOG(INFO, "add index name %s, idx %d to table %s, tid %u, pid %u", table_meta->dimensions(i).c_str(),
                i, table_meta->name().c_str(), table_meta->tid(), table_meta->pid());
    }
    // add default dimension
    if (mapping.size() <= 0) {
        mapping.insert(std::make_pair("idx0", 0));
        PDLOG(INFO, "no index specified with default");
    }
    std::shared_ptr<Table> table = std::make_shared<Table>(table_meta->name(), 
                                                           table_meta->tid(),
                                                           table_meta->pid(), seg_cnt, 
                                                           mapping,
                                                           table_meta->ttl(), is_leader,
                                                           endpoints);
    table->Init();
    table->SetGcSafeOffset(FLAGS_gc_safe_offset * 60 * 1000);
    table->SetSchema(table_meta->schema());
    table->SetTTLType(table_meta->ttl_type());
    std::string table_db_path = FLAGS_db_root_path + "/" + std::to_string(table_meta->tid()) +
                "_" + std::to_string(table_meta->pid());
    std::shared_ptr<LogReplicator> replicator;
    if (table->IsLeader()) {
        replicator = std::make_shared<LogReplicator>(table_db_path, 
                                                     table->GetReplicas(), 
                                                     ReplicatorRole::kLeaderNode, 
                                                     table);
    }else {
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
    if (!FLAGS_zk_cluster.empty() && is_leader) {
        replicator->SetLeaderTerm(table_meta->term());
    }
    std::shared_ptr<Snapshot> snapshot = std::make_shared<Snapshot>(table_meta->tid(), table_meta->pid(), replicator->GetLogPart());
    ok = snapshot->Init();
    if (!ok) {
        PDLOG(WARNING, "fail to init snapshot for tid %u, pid %u", table_meta->tid(), table_meta->pid());
        msg.assign("fail to init snapshot");
        return -1;
    }
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
            response->set_code(-1);
            response->set_msg("table dose not exists");
            break;
        }
        if (table->GetTableStat() == ::rtidb::storage::kMakingSnapshot) {
            PDLOG(WARNING, "making snapshot task is running now. tid[%u] pid[%u]", tid, pid);
            response->set_code(-1);
            response->set_msg("table is making snapshot");
            break;
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
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : task_map_) {
        for (const auto& task_info : kv.second) {
            ::rtidb::api::TaskInfo* task = response->add_task();
            task->CopyFrom(*task_info);
        }
    }
    response->set_code(0);
    response->set_msg("ok");
    done->Run();
}

void TabletImpl::DeleteOPTask(RpcController* controller,
		const ::rtidb::api::DeleteTaskRequest* request,
		::rtidb::api::GeneralResponse* response,
		Closure* done) {
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
    done->Run();
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

void TabletImpl::GcTable(uint32_t tid, uint32_t pid) {
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        return;
    }
    table->SchedGc();
    gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid));
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

int32_t TabletImpl::CheckDimessionPut(const ::rtidb::api::PutRequest* request,
                                      std::shared_ptr<Table>& table) {
    for (int32_t i = 0; i < request->dimensions_size(); i++) {
        if (table->GetIdxCnt() <= request->dimensions(i).idx()) {
            PDLOG(WARNING, "invalid put request dimensions, request idx %u is greater than table idx cnt %u", 
                    request->dimensions(i).idx(), table->GetIdxCnt());
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
        io_pool_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&TabletImpl::SchedDelBinlog, this, tid, pid));
    }
}

}
}



