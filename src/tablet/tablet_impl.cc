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

#include "tablet/tablet_impl.h"

#include <stdio.h>
#include <stdlib.h>
#include <filesystem>
#include <memory>
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>

#include <algorithm>
#include <thread>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "boost/bind.hpp"
#include "boost/container/deque.hpp"
#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "base/hash.h"
#include "base/memory_stat.h"
#include "base/proto_util.h"
#include "base/status.h"
#include "base/strings.h"
#include "base/sys_info.h"
#include "brpc/controller.h"
#include "butil/iobuf.h"
#include "codec/codec.h"
#include "codec/row_codec.h"
#include "codec/sql_rpc_row_codec.h"
#include "common/timer.h"
#include "config.h"  // NOLINT
#include "gflags/gflags.h"
#include "glog/logging.h"
#ifdef TCMALLOC_ENABLE
#include "gperftools/malloc_extension.h"
#endif
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/text_format.h"
#include "nameserver/task.h"
#include "schema/schema_adapter.h"
#include "storage/binlog.h"
#include "storage/disk_table_snapshot.h"
#include "storage/segment.h"
#include "storage/table.h"
#include "tablet/file_sender.h"

using ::openmldb::base::ReturnCode;
using ::openmldb::storage::DiskTable;
using ::openmldb::storage::Table;

DECLARE_int32(gc_interval);
DECLARE_int32(gc_pool_size);
DECLARE_int32(disk_gc_interval);
DECLARE_int32(statdb_ttl);
DECLARE_uint32(scan_max_bytes_size);
DECLARE_uint32(scan_reserve_size);
DECLARE_uint32(max_memory_mb);
DECLARE_double(mem_release_rate);
DECLARE_int32(get_sys_mem_interval);
DECLARE_string(db_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_bool(binlog_notify_on_put);
DECLARE_int32(task_pool_size);
DECLARE_int32(io_pool_size);
DECLARE_int32(make_snapshot_time);
DECLARE_int32(make_snapshot_check_interval);
DECLARE_uint32(make_snapshot_offline_interval);
DECLARE_bool(recycle_bin_enabled);
DECLARE_uint32(recycle_ttl);
DECLARE_string(recycle_bin_root_path);
DECLARE_string(recycle_bin_ssd_root_path);
DECLARE_string(recycle_bin_hdd_root_path);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_uint32(get_table_diskused_interval);
DECLARE_uint32(get_memory_stat_interval);
DECLARE_uint32(task_check_interval);
DECLARE_uint32(load_index_max_wait_time);
DECLARE_bool(use_name);
DECLARE_bool(enable_distsql);
DECLARE_string(snapshot_compression);
DECLARE_string(file_compression);
DECLARE_int32(request_timeout_ms);

// cluster config
DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_string(zk_auth_schema);
DECLARE_string(zk_cert);

DECLARE_int32(binlog_sync_to_disk_interval);
DECLARE_int32(binlog_delete_interval);
DECLARE_uint32(absolute_ttl_max);
DECLARE_uint32(latest_ttl_max);
DECLARE_uint32(max_traverse_cnt);
DECLARE_uint32(snapshot_ttl_time);
DECLARE_uint32(snapshot_ttl_check_interval);
DECLARE_uint32(put_slow_log_threshold);
DECLARE_uint32(query_slow_log_threshold);
DECLARE_int32(snapshot_pool_size);

namespace openmldb {
namespace tablet {

static const uint32_t SEED = 0xe17a1465;

static constexpr const char DEPLOY_STATS[] = "deploy_stats";

TabletImpl::TabletImpl()
    : tables_(),
      mu_(),
      gc_pool_(FLAGS_gc_pool_size),
      replicators_(),
      snapshots_(),
      zk_client_(nullptr),
      trivial_task_pool_(1),
      task_pool_(FLAGS_task_pool_size),
      io_pool_(FLAGS_io_pool_size),
      snapshot_pool_(FLAGS_snapshot_pool_size),
      mode_root_paths_(),
      mode_recycle_root_paths_(),
      follower_(false),
      catalog_(new ::openmldb::catalog::TabletCatalog()),
      engine_(),
      zk_cluster_(),
      zk_path_(),
      endpoint_(),
      sp_cache_(std::shared_ptr<SpCache>(new SpCache())),
      notify_path_(),
      globalvar_changed_notify_path_(),
      startup_mode_(::openmldb::type::StartupMode::kStandalone) {}

TabletImpl::~TabletImpl() {
    task_pool_.Stop(true);
    trivial_task_pool_.Stop(true);
    gc_pool_.Stop(true);
    io_pool_.Stop(true);
    snapshot_pool_.Stop(true);
    if (zk_client_) {
        delete zk_client_;
    }
}

bool TabletImpl::Init(const std::string& real_endpoint) {
    return Init(FLAGS_zk_cluster, FLAGS_zk_root_path, FLAGS_endpoint, real_endpoint);
}

bool TabletImpl::Init(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint,
                      const std::string& real_endpoint) {
    zk_cluster_ = zk_cluster;
    zk_path_ = zk_path;
    endpoint_ = endpoint;
    notify_path_ = zk_path + "/table/notify";
    sp_root_path_ = zk_path + "/store_procedure/db_sp_data";
    globalvar_changed_notify_path_ = zk_path + "/notify/global_variable";
    global_variables_ = std::make_shared<std::map<std::string, std::string>>();
    global_variables_->emplace("execute_mode", "offline");
    global_variables_->emplace("enable_trace", "false");

    ::openmldb::base::SplitString(FLAGS_db_root_path, ",", mode_root_paths_[::openmldb::common::kMemory]);
    ::openmldb::base::SplitString(FLAGS_ssd_root_path, ",", mode_root_paths_[::openmldb::common::kSSD]);
    ::openmldb::base::SplitString(FLAGS_hdd_root_path, ",", mode_root_paths_[::openmldb::common::kHDD]);

    ::openmldb::base::SplitString(FLAGS_recycle_bin_root_path, ",",
                                  mode_recycle_root_paths_[::openmldb::common::kMemory]);
    ::openmldb::base::SplitString(FLAGS_recycle_bin_ssd_root_path, ",",
                                  mode_recycle_root_paths_[::openmldb::common::kSSD]);
    ::openmldb::base::SplitString(FLAGS_recycle_bin_hdd_root_path, ",",
                                  mode_recycle_root_paths_[::openmldb::common::kHDD]);
    // if want /brpc_metrics, prefix should be g_server_info_prefix+<port>(when no server_info_name), means
    // rpc_server_<port> if standalone, diy
    deploy_collector_ = std::make_unique<::openmldb::statistics::DeploymentMetricCollector>(
        "rpc_server_" + endpoint.substr(endpoint.find(":") + 1));

    if (!zk_cluster.empty()) {
        zk_client_ = new ZkClient(zk_cluster, real_endpoint, FLAGS_zk_session_timeout, endpoint, zk_path,
                FLAGS_zk_auth_schema, FLAGS_zk_cert);
        bool ok = zk_client_->Init();
        if (!ok) {
            PDLOG(ERROR, "fail to init zookeeper with cluster %s", zk_cluster.c_str());
            return false;
        }
        startup_mode_ = ::openmldb::type::StartupMode::kCluster;
    } else {
        PDLOG(INFO, "start with standalone mode");
        startup_mode_ = ::openmldb::type::StartupMode::kStandalone;
    }

    ::hybridse::vm::EngineOptions options;
    if (IsClusterMode()) {
        options.SetClusterOptimized(FLAGS_enable_distsql);
    } else {
        options.SetClusterOptimized(false);
    }
    engine_ = std::make_unique<::hybridse::vm::Engine>(catalog_, options);
    catalog_->SetLocalTablet(std::make_shared<::hybridse::vm::LocalTablet>(engine_.get(), sp_cache_));
    std::set<std::string> snapshot_compression_set{"off", "zlib", "snappy"};
    if (snapshot_compression_set.find(FLAGS_snapshot_compression) == snapshot_compression_set.end()) {
        LOG(ERROR) << "wrong snapshot_compression: " << FLAGS_snapshot_compression;
        return false;
    }
    std::set<std::string> file_compression_set{"off", "zlib", "lz4"};
    if (file_compression_set.find(FLAGS_file_compression) == file_compression_set.end()) {
        LOG(ERROR) << "wrong FLAGS_file_compression: " << FLAGS_file_compression;
        return false;
    }
    if (FLAGS_make_snapshot_time < 0 || FLAGS_make_snapshot_time > 23) {
        PDLOG(ERROR, "make_snapshot_time[%d] is illegal.", FLAGS_make_snapshot_time);
        return false;
    }

    if (FLAGS_db_root_path != "") {
        if (!CreateMultiDir(mode_root_paths_[::openmldb::common::kMemory])) {
            PDLOG(ERROR, "fail to create db root path %s", FLAGS_db_root_path.c_str());
            return false;
        }
    } else {
        PDLOG(ERROR, "db_root_path is required");
        return false;
    }

    if (FLAGS_ssd_root_path != "") {
        if (!CreateMultiDir(mode_root_paths_[::openmldb::common::kSSD])) {
            PDLOG(ERROR, "fail to create ssd root path %s", FLAGS_ssd_root_path.c_str());
            return false;
        }
    } else {
        PDLOG(WARNING, "ssd_root_path is not set");
    }

    if (FLAGS_hdd_root_path != "") {
        if (!CreateMultiDir(mode_root_paths_[::openmldb::common::kHDD])) {
            PDLOG(ERROR, "fail to create hdd root path %s", FLAGS_hdd_root_path.c_str());
            return false;
        }
    } else {
        PDLOG(WARNING, "hdd_root_path is not set");
    }

    if (FLAGS_recycle_bin_enabled) {
        // FLAGS_db_root_path is guaranteed to be not empty
        if (FLAGS_recycle_bin_root_path != "") {
            if (!CreateMultiDir(mode_recycle_root_paths_[::openmldb::common::kMemory])) {
                PDLOG(ERROR, "fail to create recycle bin root path %s", FLAGS_recycle_bin_root_path.c_str());
                return false;
            }
        } else {
            PDLOG(ERROR, "recycle_bin_root_path is not configured. Deleted table is not recycled");
        }

        if (FLAGS_ssd_root_path != "") {
            if (FLAGS_recycle_bin_ssd_root_path != "") {
                if (!CreateMultiDir(mode_recycle_root_paths_[::openmldb::common::kSSD])) {
                    PDLOG(ERROR, "fail to create recycle bin root path %s", FLAGS_recycle_bin_ssd_root_path.c_str());
                    return false;
                }
            } else {
                PDLOG(ERROR, "recycle_bin_ssd_root_path is not configured. Deleted table is not recycled.");
            }
        }

        if (FLAGS_hdd_root_path != "") {
            if (FLAGS_recycle_bin_hdd_root_path != "") {
                if (!CreateMultiDir(mode_recycle_root_paths_[::openmldb::common::kHDD])) {
                    PDLOG(WARNING, "fail to create recycle bin root path %s", FLAGS_recycle_bin_hdd_root_path.c_str());
                    return false;
                }
            } else {
                PDLOG(ERROR, "recycle_bin_hdd_root_path is not configured. Deleted table is not recycled.");
            }
        }
    }

    std::map<std::string, std::string> real_endpoint_map = {{endpoint, real_endpoint}};
    if (!catalog_->UpdateClient(real_endpoint_map)) {
        PDLOG(ERROR, "update client failed");
        return false;
    }

    if (IsClusterMode()) {
        RecoverExternalFunction();
    }

    snapshot_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
    task_pool_.AddTask(boost::bind(&TabletImpl::GetDiskused, this));
    if (FLAGS_max_memory_mb > 0) {
        LOG(INFO) << "max memory is " << FLAGS_max_memory_mb << " MB";
        task_pool_.AddTask(boost::bind(&TabletImpl::GetMemoryStat, this));
    }
    if (FLAGS_recycle_ttl != 0) {
        task_pool_.DelayTask(FLAGS_recycle_ttl * 60 * 1000, boost::bind(&TabletImpl::SchedDelRecycle, this));
    }
#ifdef TCMALLOC_ENABLE
    MallocExtension* tcmalloc = MallocExtension::instance();
    tcmalloc->SetMemoryReleaseRate(FLAGS_mem_release_rate);
#endif
#if defined(__linux__)
    trivial_task_pool_.DelayTask(FLAGS_get_sys_mem_interval, boost::bind(&TabletImpl::UpdateMemoryUsage, this));
#endif
    return true;
}

void TabletImpl::UpdateTTL(RpcController* ctrl, const ::openmldb::api::UpdateTTLRequest* request,
                           ::openmldb::api::UpdateTTLResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);

    if (!table) {
        PDLOG(WARNING, "table does not exist. tid %u, pid %u", tid, pid);
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    ::openmldb::common::TTLSt ttl(request->ttl());
    uint64_t abs_ttl = ttl.abs_ttl();
    uint64_t lat_ttl = ttl.lat_ttl();
    const auto& index_name = request->index_name();
    if (!index_name.empty()) {
        auto index = table->GetIndex(request->index_name());
        if (!index) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", index_name.c_str(), tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kIdxNameNotFound);
            response->set_msg("idx name not found");
            return;
        }
    }
    // different ttl type is ok
    // no ttl value limit check in tablet, do it in nameserver before send request
    ::openmldb::storage::TTLSt ttl_st(ttl);
    table->SetTTL(::openmldb::storage::UpdateTTLMeta(ttl_st, request->index_name()));
    std::string db_root_path;
    if (!ChooseDBRootPath(tid, pid, table->GetStorageMode(), db_root_path)) {
        base::SetResponseStatus(base::ReturnCode::kFailToGetDbRootPath, "fail to get db root path", response);
        PDLOG(WARNING, "fail to get table db root path for tid %u, pid %u", tid, pid);
        return;
    }
    std::string db_path = GetDBPath(db_root_path, tid, pid);
    if (!::openmldb::base::IsExists(db_path)) {
        PDLOG(WARNING, "table db path doesn't exist. tid %u, pid %u", tid, pid);
        base::SetResponseStatus(base::ReturnCode::kTableDbPathIsNotExist, "table db path does not exist", response);
        return;
    }
    if (WriteTableMeta(db_path, table->GetTableMeta().get()) < 0) {
        PDLOG(WARNING, "write table_meta failed. tid[%u] pid[%u]", tid, pid);
        base::SetResponseStatus(base::ReturnCode::kWriteDataFailed, "write meta data failed", response);
        return;
    }
    PDLOG(INFO, "update table tid %u pid %u ttl to abs_ttl %lu lat_ttl %lu index_name %s", tid, pid, abs_ttl, lat_ttl,
          index_name.c_str());
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

bool TabletImpl::RegisterZK() {
    if (IsClusterMode()) {
        if (zk_client_ == nullptr) {
            return false;
        }
        if (FLAGS_use_name) {
            if (!zk_client_->RegisterName()) {
                return false;
            }
        }
        if (!zk_client_->Register(true)) {
            PDLOG(WARNING, "fail to register tablet with endpoint %s", endpoint_.c_str());
            return false;
        }
        PDLOG(INFO, "tablet with endpoint %s register to zk cluster %s ok", endpoint_.c_str(), zk_cluster_.c_str());
        if (zk_client_->IsExistNode(notify_path_) != 0) {
            zk_client_->CreateNode(notify_path_, "1");
        }
        if (zk_client_->IsExistNode(globalvar_changed_notify_path_) != 0) {
            zk_client_->CreateNode(globalvar_changed_notify_path_, "1");
        }
        if (!zk_client_->WatchItem(globalvar_changed_notify_path_,
                                   boost::bind(&TabletImpl::UpdateGlobalVarTable, this))) {
            LOG(WARNING) << "add global var changed watcher failed";
            return false;
        }
        if (!zk_client_->WatchItem(notify_path_, boost::bind(&TabletImpl::RefreshTableInfo, this))) {
            LOG(WARNING) << "add notify watcher failed";
            return false;
        }
        trivial_task_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&TabletImpl::CheckZkClient, this));
    }
    return true;
}

bool TabletImpl::CheckGetDone(::openmldb::api::GetType type, uint64_t ts, uint64_t target_ts) {
    switch (type) {
        case openmldb::api::GetType::kSubKeyEq:
            if (ts == target_ts) {
                return true;
            }
            break;
        case openmldb::api::GetType::kSubKeyLe:
            if (ts <= target_ts) {
                return true;
            }
            break;
        case openmldb::api::GetType::kSubKeyLt:
            if (ts < target_ts) {
                return true;
            }
            break;
        case openmldb::api::GetType::kSubKeyGe:
            if (ts >= target_ts) {
                return true;
            }
            break;
        case openmldb::api::GetType::kSubKeyGt:
            if (ts > target_ts) {
                return true;
            }
    }
    return false;
}

void TabletImpl::UpdateMemoryUsage() {
    base::SysInfo info;
    if (auto status = base::GetSysMem(&info); status.OK()) {
        if (info.mem_total > 0) {
            system_memory_usage_rate_.store(info.mem_used * 100 / info.mem_total, std::memory_order_relaxed);
            DEBUGLOG("system_memory_usage_rate is %u", system_memory_usage_rate_.load(std::memory_order_relaxed));
        } else {
            PDLOG(WARNING, "total memory is zero");
        }
    } else {
        PDLOG(WARNING, "GetSysMem run failed. error message %s", status.GetMsg().c_str());
    }
    trivial_task_pool_.DelayTask(FLAGS_get_sys_mem_interval, boost::bind(&TabletImpl::UpdateMemoryUsage, this));
}

int32_t TabletImpl::GetIndex(const ::openmldb::api::GetRequest* request, const ::openmldb::api::TableMeta& meta,
                             const std::map<int32_t, std::shared_ptr<Schema>>& vers_schema, CombineIterator* it,
                             std::string* value, uint64_t* ts) {
    if (it == nullptr || value == nullptr || ts == nullptr) {
        PDLOG(WARNING, "invalid args");
        return -1;
    }
    uint64_t st = request->ts();
    openmldb::api::GetType st_type = request->type();
    uint64_t et = request->et();
    const openmldb::api::GetType& et_type = request->et_type();
    if (st_type == ::openmldb::api::kSubKeyEq && et_type == ::openmldb::api::kSubKeyEq && st != et) {
        return -1;
    }
    ::openmldb::api::GetType real_et_type = et_type;
    ::openmldb::storage::TTLType ttl_type = it->GetTTLType();
    uint64_t expire_time = it->GetExpireTime();
    if (ttl_type == ::openmldb::storage::TTLType::kAbsoluteTime ||
        ttl_type == ::openmldb::storage::TTLType::kAbsOrLat) {
        et = std::max(et, expire_time);
    }
    if (et < expire_time && et_type == ::openmldb::api::GetType::kSubKeyGt) {
        real_et_type = ::openmldb::api::GetType::kSubKeyGe;
    }
    bool enable_project = false;
    openmldb::codec::RowProject row_project(vers_schema, request->projection());
    if (request->projection().size() > 0) {
        bool ok = row_project.Init();
        if (!ok) {
            PDLOG(WARNING, "invalid project list");
            return -1;
        }
        enable_project = true;
    }
    if (st > 0 && st < et) {
        DEBUGLOG("invalid args for st %lu less than et %lu or expire time %lu", st, et, expire_time);
        return -1;
    }
    if (it->Valid()) {
        *ts = it->GetTs();
        if (st_type == ::openmldb::api::GetType::kSubKeyEq && st > 0 && *ts != st) {
            return 1;
        }
        bool jump_out = false;
        if (st_type == ::openmldb::api::GetType::kSubKeyGe || st_type == ::openmldb::api::GetType::kSubKeyGt) {
            ::openmldb::base::Slice it_value = it->GetValue();
            if (enable_project) {
                int8_t* ptr = nullptr;
                uint32_t size = 0;
                openmldb::base::Slice data = it->GetValue();
                const int8_t* row_ptr = reinterpret_cast<const int8_t*>(data.data());
                bool ok = row_project.Project(row_ptr, data.size(), &ptr, &size);
                if (!ok) {
                    PDLOG(WARNING, "fail to make a projection");
                    return -4;
                }
                value->assign(reinterpret_cast<char*>(ptr), size);
                delete[] ptr;
            } else {
                value->assign(it_value.data(), it_value.size());
            }
            return 0;
        }
        switch (real_et_type) {
            case ::openmldb::api::GetType::kSubKeyEq:
                if (*ts != et) {
                    jump_out = true;
                }
                break;

            case ::openmldb::api::GetType::kSubKeyGt:
                if (*ts <= et) {
                    jump_out = true;
                }
                break;

            case ::openmldb::api::GetType::kSubKeyGe:
                if (*ts < et) {
                    jump_out = true;
                }
                break;

            default:
                PDLOG(WARNING, "invalid et type %s", ::openmldb::api::GetType_Name(et_type).c_str());
                return -2;
        }
        if (jump_out) {
            return 1;
        }
        if (enable_project) {
            int8_t* ptr = nullptr;
            uint32_t size = 0;
            openmldb::base::Slice data = it->GetValue();
            const int8_t* row_ptr = reinterpret_cast<const int8_t*>(data.data());
            bool ok = row_project.Project(row_ptr, data.size(), &ptr, &size);
            if (!ok) {
                PDLOG(WARNING, "fail to make a projection");
                return -4;
            }
            value->assign(reinterpret_cast<char*>(ptr), size);
            delete[] ptr;
        } else {
            value->assign(it->GetValue().data(), it->GetValue().size());
        }
        return 0;
    }
    // not found
    return 1;
}

void TabletImpl::Refresh(RpcController* controller, const ::openmldb::api::RefreshRequest* request,
                         ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (IsClusterMode()) {
        if (RefreshSingleTable(request->tid())) {
            PDLOG(INFO, "refresh success. tid %u", request->tid());
        }
    }
}

void TabletImpl::RecoverExternalFunction() {
    std::string external_function_path = zk_path_ + "/data/function";
    std::vector<std::string> functions;
    if (zk_client_->IsExistNode(external_function_path) == 0) {
        if (!zk_client_->GetChildren(external_function_path, functions)) {
            LOG(WARNING) << "fail to get function list with path " << external_function_path;
            return;
        }
    }
    if (functions.empty()) {
        LOG(INFO) << "no external functions to recover";
        return;
    }
    for (const auto& name : functions) {
        std::string value;
        if (!zk_client_->GetNodeValue(external_function_path + "/" + name, value)) {
            LOG(WARNING) << "fail to get function data. function: " << name;
            continue;
        }
        ::openmldb::common::ExternalFun fun;
        if (!fun.ParseFromString(value)) {
            LOG(WARNING) << "fail to parse external function. function: " << name << " value: " << value;
            continue;
        }
        if (CreateFunctionInternal(fun).OK()) {
            LOG(INFO) << "recover " << name << " function success";
        }
    }
}

void TabletImpl::Get(RpcController* controller, const ::openmldb::api::GetRequest* request,
                     ::openmldb::api::GetResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t start_time = ::baidu::common::timer::get_micros();
    uint32_t tid = request->tid();
    uint32_t pid_num = 1;
    if (request->pid_group_size() > 0) {
        pid_num = request->pid_group_size();
    }
    std::vector<QueryIt> query_its(pid_num);
    std::shared_ptr<::openmldb::storage::TTLSt> ttl;
    ::openmldb::storage::TTLSt expired_value;
    for (uint32_t idx = 0; idx < pid_num; idx++) {
        uint32_t pid = 0;
        if (request->pid_group_size() > 0) {
            pid = request->pid_group(idx);
        } else {
            pid = request->pid();
        }
        auto table = GetTable(tid, pid);
        if (auto status = CheckTable(tid, pid, false, table); !status.OK()) {
            SetResponseStatus(status, response);
            return;
        }
        std::string index_name;
        if (request->has_idx_name() && request->idx_name().size() > 0) {
            index_name = request->idx_name();
        } else {
            index_name = table->GetPkIndex()->GetName();
        }
        auto index_def = table->GetIndex(index_name);
        if (!index_def || !index_def->IsReady()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", index_name.c_str(), tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kIdxNameNotFound);
            response->set_msg("idx name not found");
            return;
        }
        uint32_t index = index_def->GetId();
        if (!ttl) {
            ttl = index_def->GetTTL();
            expired_value = *ttl;
            expired_value.abs_ttl = table->GetExpireTime(expired_value);
        }
        GetIterator(table, request->key(), index, &query_its[idx].it, &query_its[idx].ticket);
        if (!query_its[idx].it) {
            response->set_code(::openmldb::base::ReturnCode::kTsNameNotFound);
            response->set_msg("ts name not found");
            return;
        }
        query_its[idx].table = table;
    }
    auto table_meta = query_its.begin()->table->GetTableMeta();
    const std::map<int32_t, std::shared_ptr<Schema>> vers_schema = query_its.begin()->table->GetAllVersionSchema();
    CombineIterator combine_it(std::move(query_its), request->ts(), request->type(), expired_value);
    combine_it.SeekToFirst();
    std::string* value = response->mutable_value();
    uint64_t ts = 0;
    int32_t code = GetIndex(request, *table_meta, vers_schema, &combine_it, value, &ts);
    response->set_ts(ts);
    response->set_code(code);
    uint64_t end_time = ::baidu::common::timer::get_micros();
    if (start_time + FLAGS_query_slow_log_threshold < end_time) {
        std::string index_name;
        if (request->has_idx_name() && request->idx_name().size() > 0) {
            index_name = request->idx_name();
        }
        PDLOG(INFO, "slow log[get]. key %s index_name %s time %lu. tid %u, pid %u", request->key().c_str(),
              index_name.c_str(), end_time - start_time, request->tid(), request->pid());
    }
    switch (code) {
        case 1:
            response->set_code(::openmldb::base::ReturnCode::kKeyNotFound);
            response->set_msg("key not found");
            return;
        case 0:
            return;
        case -1:
            response->set_msg("invalid args");
            response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
            return;
        case -2:
            response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
            response->set_msg("st/et sub key type is invalid");
            return;
        default:
            return;
    }
}

void TabletImpl::Put(RpcController* controller, const ::openmldb::api::PutRequest* request,
                     ::openmldb::api::PutResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (follower_.load(std::memory_order_relaxed)) {
        response->set_code(::openmldb::base::ReturnCode::kIsFollowerCluster);
        response->set_msg("is follower cluster");
        return;
    }
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    auto table = GetTable(tid, pid);
    if (auto status = CheckTable(tid, pid, true, table); !status.OK()) {
        SetResponseStatus(status, response);
        return;
    }
    uint64_t start_time = ::baidu::common::timer::get_micros();
    DLOG(INFO) << "request dimension size " << request->dimensions_size() << " request time " << request->time();
    if (table->GetStorageMode() == ::openmldb::common::StorageMode::kMemory &&
        memory_used_.load(std::memory_order_relaxed) > FLAGS_max_memory_mb) {
        PDLOG(WARNING, "current memory %lu MB exceed max memory limit %lu MB. tid %u, pid %u",
              memory_used_.load(std::memory_order_relaxed), FLAGS_max_memory_mb, tid, pid);
        response->set_code(::openmldb::base::ReturnCode::kExceedMaxMemory);
        response->set_msg("exceed max memory");
        return;
    }
    ::openmldb::api::LogEntry entry;
    entry.set_pk(request->pk());
    entry.set_ts(request->time());
    if (table->GetCompressType() == openmldb::type::CompressType::kSnappy) {
        const auto& raw_val = request->value();
        std::string* val = entry.mutable_value();
        ::snappy::Compress(raw_val.c_str(), raw_val.length(), val);
    } else {
        entry.set_value(request->value());
    }
    if (request->dimensions_size() > 0) {
        entry.mutable_dimensions()->CopyFrom(request->dimensions());
    }
    if (request->ts_dimensions_size() > 0) {
        entry.mutable_ts_dimensions()->CopyFrom(request->ts_dimensions());
    }

    absl::Status st;
    if (request->dimensions_size() > 0) {
        int32_t ret_code = CheckDimessionPut(request, table->GetIdxCnt());
        if (ret_code != 0) {
            response->set_code(::openmldb::base::ReturnCode::kInvalidDimensionParameter);
            response->set_msg("invalid dimension parameter");
            return;
        }
        DLOG(INFO) << "put data to tid " << tid << " pid " << pid << " with key " << request->dimensions(0).key();
        // 1. normal put: ok, invalid data
        // 2. put if absent: ok, exists but ignore, invalid data
        st = table->Put(entry.ts(), entry.value(), entry.dimensions(), request->put_if_absent());
    }

    if (!st.ok()) {
        if (request->put_if_absent() && absl::IsAlreadyExists(st)) {
            // not a failure but shounld't write log entry
            response->set_code(::openmldb::base::ReturnCode::kOk);
            response->set_msg("exists but ignore");
            return;
        }
        LOG(WARNING) << st.ToString();
        response->set_code(::openmldb::base::ReturnCode::kPutFailed);
        response->set_msg(st.ToString());
        return;
    }

    response->set_code(::openmldb::base::ReturnCode::kOk);
    std::shared_ptr<LogReplicator> replicator;
    bool ok = false;
    do {
        replicator = GetReplicator(request->tid(), request->pid());
        if (!replicator) {
            PDLOG(WARNING, "fail to find table tid %u pid %u leader's log replicator", tid, pid);
            break;
        }
        entry.set_term(replicator->GetLeaderTerm());

        // Aggregator update assumes that binlog_offset is strictly increasing
        // so the update should be protected within the replicator lock
        // in case there will be other Put jump into the middle
        auto update_aggr = [this, &request, &ok, &entry]() {
            ok =
                UpdateAggrs(request->tid(), request->pid(), request->value(), request->dimensions(), entry.log_index());
        };
        UpdateAggrClosure closure(update_aggr);
        replicator->AppendEntry(entry, &closure);
        if (!ok) {
            response->set_code(::openmldb::base::ReturnCode::kError);
            response->set_msg("update aggr failed");
            return;
        }
    } while (false);

    uint64_t end_time = ::baidu::common::timer::get_micros();
    if (start_time + FLAGS_put_slow_log_threshold < end_time) {
        std::string key;
        if (request->dimensions_size() > 0) {
            for (int idx = 0; idx < request->dimensions_size(); idx++) {
                if (!key.empty()) {
                    key.append(", ");
                }
                key.append(std::to_string(request->dimensions(idx).idx()));
                key.append(":");
                key.append(request->dimensions(idx).key());
            }
        } else {
            key = request->pk();
        }
        PDLOG(INFO, "slow log[put]. key %s time %lu. tid %u, pid %u", key.c_str(), end_time - start_time, tid, pid);
    }

    if (replicator) {
        if (FLAGS_binlog_notify_on_put) {
            replicator->Notify();
        }
    }
    // update global var in standalone mode
    if (!IsClusterMode() && table->GetDB() == openmldb::nameserver::INFORMATION_SCHEMA_DB &&
        table->GetName() == openmldb::nameserver::GLOBAL_VARIABLES) {
        UpdateGlobalVarTable();
    }
}

int TabletImpl::CheckTableMeta(const openmldb::api::TableMeta* table_meta, std::string& msg) {
    msg.clear();
    if (table_meta->name().empty()) {
        msg = "table name is empty";
        return -1;
    }
    if (table_meta->tid() <= 0) {
        msg = "tid <= 0, invalid";
        return -1;
    }
    std::map<std::string, ::openmldb::type::DataType> column_map;
    if (table_meta->column_desc_size() > 0) {
        for (const auto& column_desc : table_meta->column_desc()) {
            if (column_map.find(column_desc.name()) != column_map.end()) {
                msg = "has repeated column name " + column_desc.name();
                return -1;
            }
            column_map.insert(std::make_pair(column_desc.name(), column_desc.data_type()));
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
                if (iter->second == ::openmldb::type::kFloat || iter->second == ::openmldb::type::kDouble) {
                    msg = "float or double column can not be index" + column_name;
                    return -1;
                }
            }
            if (!has_col) {
                auto iter = column_map.find(column_key.index_name());
                if (iter == column_map.end()) {
                    msg = "index must member of columns when column key col name is empty";
                    return -1;
                } else {
                    if (iter->second == ::openmldb::type::kFloat || iter->second == ::openmldb::type::kDouble) {
                        msg = "indxe name column type can not float or column";
                        return -1;
                    }
                }
            }
            if (!column_key.ts_name().empty()) {
                auto iter = column_map.find(column_key.ts_name());
                if (iter == column_map.end()) {
                    msg = "not found column name " + column_key.ts_name();
                    return -1;
                }
            }
            if (column_key.has_ttl()) {
                if (column_key.ttl().abs_ttl() > FLAGS_absolute_ttl_max ||
                    column_key.ttl().lat_ttl() > FLAGS_latest_ttl_max) {
                    msg = "ttl is greater than conf value. max abs_ttl is " + std::to_string(FLAGS_absolute_ttl_max) +
                          ", max lat_ttl is " + std::to_string(FLAGS_latest_ttl_max);
                    return -1;
                }
            }
        }
    }
    if (table_meta->storage_mode() == common::kUnknown) {
        msg = "storage_mode is unknown";
        return -1;
    }
    return 0;
}

int32_t TabletImpl::ScanIndex(const ::openmldb::api::ScanRequest* request, const ::openmldb::api::TableMeta& meta,
                              const std::map<int32_t, std::shared_ptr<Schema>>& vers_schema, bool use_attachment,
                              CombineIterator* combine_it, butil::IOBuf* io_buf, uint32_t* count, bool* is_finish) {
    uint32_t limit = request->limit();
    if (combine_it == nullptr || io_buf == nullptr || count == nullptr || is_finish == nullptr) {
        PDLOG(WARNING, "invalid args");
        return -1;
    }
    uint64_t st = request->st();
    uint64_t et = request->et();
    ::openmldb::storage::TTLType ttl_type = combine_it->GetTTLType();
    uint64_t expire_time = combine_it->GetExpireTime();
    if (ttl_type == ::openmldb::storage::TTLType::kAbsoluteTime ||
        ttl_type == ::openmldb::storage::TTLType::kAbsOrLat) {
        et = std::max(et, expire_time);
    }

    if (st > 0 && st < et) {
        PDLOG(WARNING, "invalid args for st %lu less than et %lu or expire time %lu", st, et, expire_time);
        return -1;
    }

    bool enable_project = false;
    ::openmldb::codec::RowProject row_project(vers_schema, request->projection());
    if (request->projection().size() > 0) {
        if (!row_project.Init()) {
            PDLOG(WARNING, "invalid project list");
            return -1;
        }
        enable_project = true;
    }
    bool remove_duplicated_record = request->enable_remove_duplicated_record();
    uint64_t last_time = 0;
    uint32_t total_block_size = 0;
    uint32_t record_count = 0;
    uint32_t skip_record_num = request->skip_record_num();
    combine_it->SeekToFirst();
    while (combine_it->Valid()) {
        if (limit > 0 && record_count >= limit) {
            *is_finish = false;
            break;
        }
        if (remove_duplicated_record && record_count > 0 && last_time == combine_it->GetTs()) {
            combine_it->Next();
            continue;
        }
        if (combine_it->GetTs() == st && skip_record_num > 0) {
            skip_record_num--;
            combine_it->Next();
            continue;
        }
        uint64_t ts = combine_it->GetTs();
        if (ts <= et) {
            break;
        }
        last_time = ts;
        if (enable_project) {
            int8_t* ptr = nullptr;
            uint32_t size = 0;
            openmldb::base::Slice data = combine_it->GetValue();
            const int8_t* row_ptr = reinterpret_cast<const int8_t*>(data.data());
            bool ok = row_project.Project(row_ptr, data.size(), &ptr, &size);
            if (!ok) {
                PDLOG(WARNING, "fail to make a projection");
                return -4;
            }
            if (use_attachment) {
                io_buf->append(reinterpret_cast<void*>(ptr), size);
            } else {
                ::openmldb::codec::Encode(ts, reinterpret_cast<char*>(ptr), size, io_buf);
            }
            total_block_size += size;
        } else {
            openmldb::base::Slice data = combine_it->GetValue();
            if (use_attachment) {
                io_buf->append(reinterpret_cast<const void*>(data.data()), data.size());
            } else {
                ::openmldb::codec::Encode(ts, data.data(), data.size(), io_buf);
            }
            total_block_size += data.size();
        }
        record_count++;
        if (FLAGS_scan_max_bytes_size > 0 && total_block_size > FLAGS_scan_max_bytes_size) {
            *is_finish = false;
            break;
        }
        combine_it->Next();
    }
    *count = record_count;
    return 0;
}

int32_t TabletImpl::CountIndex(uint64_t expire_time, uint64_t expire_cnt, ::openmldb::storage::TTLType ttl_type,
                               ::openmldb::storage::TableIterator* it, const ::openmldb::api::CountRequest* request,
                               uint32_t* count) {
    uint64_t st = request->st();
    const openmldb::api::GetType& st_type = request->st_type();
    uint64_t et = request->et();
    const openmldb::api::GetType& et_type = request->et_type();
    bool remove_duplicated_record =
        request->has_enable_remove_duplicated_record() && request->enable_remove_duplicated_record();
    if (it == nullptr || count == nullptr) {
        PDLOG(WARNING, "invalid args");
        return -1;
    }
    openmldb::api::GetType real_st_type = st_type;
    openmldb::api::GetType real_et_type = et_type;
    if (et < expire_time && et_type == ::openmldb::api::GetType::kSubKeyGt) {
        real_et_type = ::openmldb::api::GetType::kSubKeyGe;
    }
    if (ttl_type == ::openmldb::storage::TTLType::kAbsoluteTime ||
        ttl_type == ::openmldb::storage::TTLType::kAbsOrLat) {
        et = std::max(et, expire_time);
    }
    if (st_type == ::openmldb::api::GetType::kSubKeyEq) {
        real_st_type = ::openmldb::api::GetType::kSubKeyLe;
    }
    if (st_type != ::openmldb::api::GetType::kSubKeyEq && st_type != ::openmldb::api::GetType::kSubKeyLe &&
        st_type != ::openmldb::api::GetType::kSubKeyLt) {
        PDLOG(WARNING, "invalid st type %s", ::openmldb::api::GetType_Name(st_type).c_str());
        return -2;
    }
    uint32_t cnt = 0;
    if (st > 0) {
        if (st < et) {
            return -1;
        }
        if (expire_cnt == 0) {
            Seek(it, st, real_st_type);
        } else {
            switch (ttl_type) {
                case ::openmldb::storage::TTLType::kAbsoluteTime:
                    Seek(it, st, real_st_type);
                    break;
                case ::openmldb::storage::TTLType::kAbsAndLat:
                    if (!SeekWithCount(it, st, real_st_type, expire_cnt, &cnt)) {
                        Seek(it, st, real_st_type);
                    }
                    break;
                default:
                    SeekWithCount(it, st, real_st_type, expire_cnt, &cnt);
                    break;
            }
        }
    } else {
        it->SeekToFirst();
    }

    uint64_t last_key = 0;
    uint32_t internal_cnt = 0;

    while (it->Valid()) {
        if (remove_duplicated_record && internal_cnt > 0 && last_key == it->GetKey()) {
            cnt++;
            it->Next();
            continue;
        }
        if (ttl_type == ::openmldb::storage::TTLType::kAbsoluteTime) {
            if (expire_time != 0 && it->GetKey() <= expire_time) {
                break;
            }
        } else if (ttl_type == ::openmldb::storage::TTLType::kLatestTime) {
            if (expire_cnt != 0 && cnt >= expire_cnt) {
                break;
            }
        } else if (ttl_type == ::openmldb::storage::TTLType::kAbsAndLat) {
            if ((expire_cnt != 0 && cnt >= expire_cnt) && (expire_time != 0 && it->GetKey() <= expire_time)) {
                break;
            }
        } else {
            if ((expire_cnt != 0 && cnt >= expire_cnt) || (expire_time != 0 && it->GetKey() <= expire_time)) {
                break;
            }
        }
        ++cnt;
        bool jump_out = false;
        last_key = it->GetKey();
        switch (real_et_type) {
            case ::openmldb::api::GetType::kSubKeyEq:
                if (it->GetKey() != et) {
                    jump_out = true;
                }
                break;
            case ::openmldb::api::GetType::kSubKeyGt:
                if (it->GetKey() <= et) {
                    jump_out = true;
                }
                break;
            case ::openmldb::api::GetType::kSubKeyGe:
                if (it->GetKey() < et) {
                    jump_out = true;
                }
                break;
            default:
                PDLOG(WARNING, "invalid et type %s", ::openmldb::api::GetType_Name(et_type).c_str());
                return -2;
        }
        if (jump_out) break;
        last_key = it->GetKey();
        internal_cnt++;
        it->Next();
    }
    *count = internal_cnt;
    return 0;
}

void TabletImpl::Scan(RpcController* controller, const ::openmldb::api::ScanRequest* request,
                      ::openmldb::api::ScanResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t start_time = ::baidu::common::timer::get_micros();
    if (request->st() < request->et()) {
        response->set_code(::openmldb::base::ReturnCode::kStLessThanEt);
        response->set_msg("starttime less than endtime");
        return;
    }
    uint32_t tid = request->tid();
    uint32_t pid_num = 1;
    if (request->pid_group_size() > 0) {
        pid_num = request->pid_group_size();
    }
    std::vector<QueryIt> query_its(pid_num);
    std::shared_ptr<::openmldb::storage::TTLSt> ttl;
    ::openmldb::storage::TTLSt expired_value;
    for (uint32_t idx = 0; idx < pid_num; idx++) {
        uint32_t pid = 0;
        if (request->pid_group_size() > 0) {
            pid = request->pid_group(idx);
        } else {
            pid = request->pid();
        }
        auto table = GetTable(tid, pid);
        if (auto status = CheckTable(tid, pid, false, table); !status.OK()) {
            SetResponseStatus(status, response);
            return;
        }
        uint32_t index = 0;
        std::string index_name;
        if (request->has_idx_name() && !request->idx_name().empty()) {
            index_name = request->idx_name();
        } else {
            index_name = table->GetPkIndex()->GetName();
        }
        std::shared_ptr<IndexDef> index_def;
        index_def = table->GetIndex(index_name);
        if (!index_def || !index_def->IsReady()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", index_name.c_str(), tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kIdxNameNotFound);
            response->set_msg("idx name not found");
            return;
        }
        index = index_def->GetId();
        if (!ttl) {
            ttl = index_def->GetTTL();
            expired_value = *ttl;
            expired_value.abs_ttl = table->GetExpireTime(expired_value);
        }
        GetIterator(table, request->pk(), index, &query_its[idx].it, &query_its[idx].ticket);
        if (!query_its[idx].it) {
            response->set_code(::openmldb::base::ReturnCode::kTsNameNotFound);
            response->set_msg("ts name not found");
            return;
        }
        query_its[idx].table = table;
    }
    auto table_meta = query_its.begin()->table->GetTableMeta();
    const std::map<int32_t, std::shared_ptr<Schema>> vers_schema = query_its.begin()->table->GetAllVersionSchema();
    CombineIterator combine_it(std::move(query_its), request->st(), openmldb::api::GetType::kSubKeyLe, expired_value);
    uint32_t count = 0;
    int32_t code = 0;
    bool is_finish = true;
    if (!request->has_use_attachment() || !request->use_attachment()) {
        butil::IOBuf buf;
        code = ScanIndex(request, *table_meta, vers_schema, false, &combine_it, &buf, &count, &is_finish);
        buf.copy_to(response->mutable_pairs());
    } else {
        auto* cntl = dynamic_cast<brpc::Controller*>(controller);
        butil::IOBuf& buf = cntl->response_attachment();
        code = ScanIndex(request, *table_meta, vers_schema, true, &combine_it, &buf, &count, &is_finish);
        response->set_buf_size(buf.size());
        DLOG(INFO) << " scan " << request->pk() << " with buf size " << buf.size();
    }
    response->set_code(code);
    response->set_count(count);
    response->set_is_finish(is_finish);
    uint64_t end_time = ::baidu::common::timer::get_micros();
    if (start_time + FLAGS_query_slow_log_threshold < end_time) {
        std::string index_name;
        if (request->has_idx_name() && request->idx_name().size() > 0) {
            index_name = request->idx_name();
        }
        PDLOG(INFO, "slow log[scan]. key %s index_name %s time %lu. tid %u, pid %u", request->pk().c_str(),
              index_name.c_str(), end_time - start_time, request->tid(), request->pid());
    }
    switch (code) {
        case 0:
            return;
        case -1:
            response->set_msg("invalid args");
            response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
            return;
        case -2:
            response->set_msg("st/et sub key type is invalid");
            response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
            return;
        case -4:
            response->set_msg("fail to encode data rows");
            response->set_code(::openmldb::base::ReturnCode::kEncodeError);
            return;
        default:
            return;
    }
}

void TabletImpl::Count(RpcController* controller, const ::openmldb::api::CountRequest* request,
                       ::openmldb::api::CountResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    auto table = GetTable(tid, pid);
    if (auto status = CheckTable(tid, pid, false, table); !status.OK()) {
        SetResponseStatus(status, response);
        return;
    }
    uint32_t index = 0;
    ::openmldb::storage::TTLSt ttl;
    std::shared_ptr<IndexDef> index_def;
    std::string index_name;
    if (request->has_idx_name() && !request->idx_name().empty()) {
        index_name = request->idx_name();
    } else {
        index_name = table->GetPkIndex()->GetName();
    }
    index_def = table->GetIndex(index_name);
    if (!index_def || !index_def->IsReady()) {
        PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(), tid, pid);
        response->set_code(::openmldb::base::ReturnCode::kIdxNameNotFound);
        response->set_msg("idx name not found");
        return;
    }
    index = index_def->GetId();
    ttl = *index_def->GetTTL();
    if (!request->filter_expired_data()) {
        uint64_t count = 0;
        if (table->GetCount(index, request->key(), count) < 0) {
            count = 0;
        }
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        response->set_count(count);
        return;
    }
    ::openmldb::storage::Ticket ticket;
    ::openmldb::storage::TableIterator* it = table->NewIterator(index, request->key(), ticket);
    if (it == nullptr) {
        response->set_code(::openmldb::base::ReturnCode::kTsNameNotFound);
        response->set_msg("ts name not found");
        return;
    }
    uint32_t count = 0;
    int32_t code = 0;
    code = CountIndex(table->GetExpireTime(ttl), ttl.lat_ttl, index_def->GetTTLType(), it, request, &count);
    delete it;
    response->set_code(code);
    response->set_count(count);
    switch (code) {
        case 0:
            return;
        case -1:
            response->set_msg("invalid args");
            response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
            return;
        case -2:
            response->set_msg("st/et sub key type is invalid");
            response->set_code(::openmldb::base::ReturnCode::kInvalidParameter);
            return;
        case -3:
            response->set_code(::openmldb::base::ReturnCode::kReacheTheScanMaxBytesSize);
            response->set_msg("reach the max scan byte size");
            return;
        case -4:
            response->set_msg("fail to encode data rows");
            response->set_code(::openmldb::base::ReturnCode::kFailToUpdateTtlFromTablet);
            return;
        default:
            return;
    }
}

void TabletImpl::Traverse(RpcController* controller, const ::openmldb::api::TraverseRequest* request,
                          ::openmldb::api::TraverseResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    auto table = GetTable(tid, pid);
    if (auto status = CheckTable(tid, pid, false, table); !status.OK()) {
        SetResponseStatus(status, response);
        return;
    }
    std::string index_name;
    if (request->has_idx_name() && !request->idx_name().empty()) {
        index_name = request->idx_name();
    } else {
        index_name = table->GetPkIndex()->GetName();
    }
    auto index_def = table->GetIndex(index_name);
    if (!index_def || !index_def->IsReady()) {
        PDLOG(WARNING, "idx name %s not found in table. tid %u, pid %u", index_name.c_str(), tid, pid);
        response->set_code(::openmldb::base::ReturnCode::kIdxNameNotFound);
        response->set_msg("idx name not found");
        return;
    }
    ::openmldb::storage::TableIterator* it = table->NewTraverseIterator(index_def->GetId());
    if (it == nullptr) {
        response->set_code(::openmldb::base::ReturnCode::kTsNameNotFound);
        response->set_msg("create iterator failed");
        return;
    }
    uint64_t last_time = 0;
    std::string last_pk;
    uint32_t ts_pos = 0;
    if (request->has_pk() && request->pk().size() > 0) {
        DLOG(INFO) << "tid " << tid << ", pid " << pid << " seek pk " << request->pk() << " ts " << request->ts();
        it->Seek(request->pk(), request->ts());
        last_pk = request->pk();
        last_time = request->ts();
        if (request->has_ts_pos()) {
            ts_pos = request->ts_pos();
        }
        auto traverse_it = dynamic_cast<::openmldb::storage::TraverseIterator*>(it);
        if (traverse_it && traverse_it->Valid() && traverse_it->GetPK() == last_pk) {
            if (request->skip_current_pk()) {
                traverse_it->NextPK();
            } else if (traverse_it->GetKey() == last_time) {
                uint32_t skip_cnt = request->has_ts_pos() ? request->ts_pos() : 1;
                while (skip_cnt > 0 && traverse_it->Valid() && traverse_it->GetPK() == last_pk &&
                       traverse_it->GetKey() == last_time) {
                    traverse_it->Next();
                    skip_cnt--;
                }
            }
        }
    } else {
        DEBUGLOG("tid %u, pid %u seek to first", tid, pid);
        it->SeekToFirst();
    }
    bool remove_duplicated_record = false;
    if (request->has_enable_remove_duplicated_record()) {
        remove_duplicated_record = request->enable_remove_duplicated_record();
    }
    uint32_t scount = 0;
    butil::IOBuf buf;
    for (; it->Valid(); it->Next()) {
        if (request->limit() > 0 && scount > request->limit() - 1) {
            DEBUGLOG("reache the limit %u ", request->limit());
            break;
        }
        DEBUGLOG("traverse pk %s ts %lu", it->GetPK().c_str(), it->GetKey());
        if (last_pk != it->GetPK()) {
            last_pk = it->GetPK();
            last_time = it->GetKey();
            ts_pos = 1;
        } else if (last_time != it->GetKey()) {
            last_time = it->GetKey();
            ts_pos = 1;
        } else {
            ts_pos++;
            // skip duplicate record
            if (remove_duplicated_record) {
                DEBUGLOG("filter duplicate record for key %s with ts %lu", last_pk.c_str(), last_time);
                continue;
            }
        }
        openmldb::base::Slice value = it->GetValue();
        DLOG(INFO) << "encode pk " << it->GetPK() << " ts " << it->GetKey() << " size " << value.size();
        ::openmldb::codec::EncodeFull(it->GetPK(), it->GetKey(), value.data(), value.size(), &buf);
        scount++;
        if (FLAGS_max_traverse_cnt > 0 && it->GetCount() >= FLAGS_max_traverse_cnt) {
            DEBUGLOG("traverse cnt %lu max %lu, key %s ts %lu", it->GetCount(), FLAGS_max_traverse_cnt, last_pk.c_str(),
                     last_time);
            break;
        }
    }
    bool is_finish = false;
    if (FLAGS_max_traverse_cnt > 0 && it->GetCount() >= FLAGS_max_traverse_cnt) {
        DEBUGLOG("traverse cnt %lu is great than max %lu, key %s ts %lu", it->GetCount(), FLAGS_max_traverse_cnt,
                 last_pk.c_str(), last_time);
        last_pk = it->GetPK();
        last_time = it->GetKey();
        if (last_pk.empty()) {
            is_finish = true;
        }
    } else if (scount < request->limit()) {
        is_finish = true;
    }
    buf.copy_to(response->mutable_pairs());
    delete it;
    DLOG(INFO) << "tid " << tid << " pid " << pid << " traverse count " << scount << " last_pk " << last_pk
               << " last_time " << last_time << " ts_pos " << ts_pos;
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_count(scount);
    response->set_pk(last_pk);
    response->set_ts(last_time);
    response->set_is_finish(is_finish);
    response->set_ts_pos(ts_pos);
}

base::Status TabletImpl::CheckTable(uint32_t tid, uint32_t pid, bool check_leader,
        const std::shared_ptr<Table>& table) {
    if (!table) {
        PDLOG(WARNING, "table does not exist. tid %u, pid %u", tid, pid);
        return {base::ReturnCode::kTableIsNotExist, "table does not exist"};
    }
    if (check_leader && !table->IsLeader()) {
        DEBUGLOG("table is follower. tid %u, pid %u", tid, pid);
        return {base::ReturnCode::kTableIsFollower, "table is follower"};
    }
    if (table->GetTableStat() == ::openmldb::storage::kLoading) {
        PDLOG(WARNING, "table is loading. tid %u, pid %u", tid, pid);
        return {base::ReturnCode::kTableIsLoading, "table is loading"};
    }
    return {};
}

base::Status TabletImpl::DeleteAllIndex(const std::shared_ptr<storage::Table>& table,
                                        const std::shared_ptr<IndexDef>& cur_index,
                                        const std::string& key,
                                        std::optional<uint64_t> start_ts,
                                        std::optional<uint64_t> end_ts,
                                        bool skip_cur_ts_col,
                                        const std::shared_ptr<catalog::TableClientManager>& client_manager,
                                        uint32_t partition_num) {
    storage::Ticket ticket;
    std::unique_ptr<storage::TableIterator> iter(table->NewIterator(cur_index->GetId(), key, ticket));
    if (start_ts.has_value()) {
        iter->Seek(start_ts.value());
    } else {
        iter->SeekToFirst();
    }
    auto indexs = table->GetAllIndex();
    while (iter->Valid()) {
        DEBUGLOG("cur ts %lu cur index pos %u", iter->GetKey(), cur_index->GetId());
        if (end_ts.has_value() && iter->GetKey() <= end_ts.value()) {
            break;
        }
        auto value = iter->GetValue();
        uint32_t data_length = value.size();
        const int8_t* data = reinterpret_cast<const int8_t*>(value.data());
        std::string uncompress_data;
        if (table->GetCompressType() == openmldb::type::kSnappy) {
            snappy::Uncompress(value.data(), value.size(), &uncompress_data);
            data = reinterpret_cast<const int8_t*>(uncompress_data.data());
            data_length = uncompress_data.length();
        }
        if (data_length < codec::HEADER_LENGTH) {
            return {base::ReturnCode::kDeleteFailed, "invalid value"};
        }
        uint8_t version = codec::RowView::GetSchemaVersion(data);
        auto decoder = table->GetVersionDecoder(version);
        if (decoder == nullptr) {
            return {base::ReturnCode::kDeleteFailed, "invalid schema version"};
        }
        for (const auto& index : indexs) {
            if (!index->IsReady()) {
                continue;
            }
            if (cur_index && index->GetId() == cur_index->GetId()) {
                continue;
            }
            auto ts_col = index->GetTsColumn();
            if (skip_cur_ts_col && ts_col->GetId() == cur_index->GetTsColumn()->GetId()) {
                continue;
            }
            sdk::DeleteOption option;
            option.idx = index->GetId();
            if (ts_col->IsAutoGenTs()) {
                option.start_ts = iter->GetKey();
            } else {
                int64_t ts = 0;
                if (decoder->GetInteger(data, ts_col->GetId(), ts_col->GetType(), &ts) != 0) {
                    return {base::ReturnCode::kDeleteFailed, "get ts value failed"};
                }
                option.ts_name = ts_col->GetName();
                option.start_ts = ts;
            }
            if (option.start_ts.value() > 1) {
                option.end_ts = option.start_ts.value() - 1;
            }
            const auto& cols = index->GetColumns();
            if (cols.size() == 1) {
                const auto& col = cols.front();
                if (decoder->IsNULL(data, col.GetId())) {
                    option.key = hybridse::codec::NONETOKEN;
                } else if (decoder->GetStrValue(data, col.GetId(), &option.key) != 0) {
                    return {base::ReturnCode::kDeleteFailed, "get key failed"};
                }
                if (option.key.empty()) {
                    option.key = hybridse::codec::EMPTY_STRING;
                }
            } else {
                for (const auto& col : cols) {
                    std::string tmp;
                    if (decoder->IsNULL(data, col.GetId())) {
                        tmp = hybridse::codec::NONETOKEN;
                    } else if (decoder->GetStrValue(data, col.GetId(), &tmp) != 0) {
                        return {base::ReturnCode::kDeleteFailed, "get key failed"};
                    }
                    if (tmp.empty()) {
                        tmp = hybridse::codec::EMPTY_STRING;
                    }
                    if (!option.key.empty()) {
                        option.key.append("|");
                    }
                    option.key.append(tmp);
                }
            }
            uint32_t cur_pid = static_cast<uint32_t>(base::hash64(option.key)) % partition_num;
            auto tablet = client_manager->GetTablet(cur_pid);
            if (tablet == nullptr) {
                return {base::ReturnCode::kDeleteFailed, absl::StrCat("tablet is nullptr, pid ", cur_pid)};
            }
            auto client = tablet->GetClient();
            if (client == nullptr) {
                return {base::ReturnCode::kDeleteFailed, absl::StrCat("client is nullptr, pid ", cur_pid)};
            }
            DEBUGLOG("delete idx %u pid %u pk %s ts %lu end_ts %lu",
                    option.idx.value(), cur_pid, option.key.c_str(), option.start_ts.value(), option.end_ts.value());
            std::string msg;
            // do not delete other index data
            option.enable_decode_value = false;
            if (auto status = client->Delete(table->GetId(), cur_pid, option, FLAGS_request_timeout_ms); !status.OK()) {
                return {base::ReturnCode::kDeleteFailed,
                    absl::StrCat("delete failed. key ", option.key, " pid ", cur_pid, " msg: ", status.GetMsg())};
            }
        }

        iter->Next();
    }
    return {};
}

void TabletImpl::Delete(RpcController* controller, const ::openmldb::api::DeleteRequest* request,
                        openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    if (follower_.load(std::memory_order_relaxed)) {
        response->set_code(::openmldb::base::ReturnCode::kIsFollowerCluster);
        response->set_msg("is follower cluster");
        return;
    }
    auto table = GetTable(tid, pid);
    if (auto status = CheckTable(tid, pid, true, table); !status.OK()) {
        SetResponseStatus(status, response);
        return;
    }
    auto replicator = GetReplicator(tid, pid);
    if (!replicator) {
        PDLOG(WARNING, "fail to find table tid %u pid %u leader's log replicator", tid, pid);
        return;
    }
    ::openmldb::api::LogEntry entry;
    entry.set_term(replicator->GetLeaderTerm());
    entry.set_method_type(::openmldb::api::MethodType::kDelete);
    if (request->dimensions_size() > 0) {
        entry.add_dimensions()->CopyFrom(request->dimensions(0));
        auto index_def = table->GetIndex(request->dimensions(0).idx());
        if (!index_def || !index_def->IsReady()) {
            PDLOG(WARNING, "index %s not found in table tid %u, pid %u", request->dimensions(0).idx(), tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kIdxNameNotFound);
            response->set_msg("index not found");
            return;
        }
    } else if (request->has_idx_name() && !request->idx_name().empty()) {
        auto index_def = table->GetIndex(request->idx_name());
        if (!index_def || !index_def->IsReady()) {
            PDLOG(WARNING, "idx name %s not found in table tid %u, pid %u", request->idx_name().c_str(), tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kIdxNameNotFound);
            response->set_msg("index not found");
            return;
        }
        uint32_t idx = index_def->GetId();
        if (request->has_key()) {
            auto dimension = entry.add_dimensions();
            dimension->set_key(request->key());
            dimension->set_idx(idx);
        }
    }
    if (request->has_ts()) {
        entry.set_ts(request->ts());
    }
    if (request->has_end_ts()) {
        entry.set_end_ts(request->end_ts());
    }
    if (request->has_ts_name()) {
        entry.set_ts_name(request->ts_name());
    }
    if (entry.dimensions_size() == 0 && !entry.has_ts() && !entry.has_end_ts()) {
        response->set_code(base::ReturnCode::kInvalidArgs);
        response->set_msg("invalid args");
        PDLOG(WARNING, "invalid args. tid %u, pid %u", tid, pid);
        return;
    }
    bool delete_others = false;
    if (request->has_enable_decode_value() && request->enable_decode_value()) {
        auto indexs = table->GetAllIndex();
        if (entry.dimensions_size() > 0) {
            if (indexs.size() > 1) {
                delete_others = true;
            }
        } else if (request->has_ts_name()) {
            for (const auto& index : indexs) {
                if (!index->IsReady()) {
                    continue;
                }
                if (index->GetTsColumn()->GetName() != request->ts_name()) {
                    delete_others = true;
                    break;
                }
            }
        }
    }
    auto aggrs = GetAggregators(tid, pid);
    if (!aggrs && !delete_others) {
        if (table->Delete(entry)) {
            DEBUGLOG("delete ok. tid %u, pid %u, key %s", tid, pid, request->key().c_str());
        } else {
            response->set_code(::openmldb::base::ReturnCode::kDeleteFailed);
            response->set_msg("delete failed");
            return;
        }
    } else {
        auto get_aggregator = [this](const std::shared_ptr<Aggrs>& aggrs, uint32_t idx) -> std::shared_ptr<Aggregator> {
            if (aggrs) {
                for (const auto& aggr : *aggrs) {
                    if (aggr->GetIndexPos() == idx) {
                        return aggr;
                    }
                }
            }
            return {};
        };
        std::optional<uint64_t> start_ts = entry.has_ts() ? std::optional<uint64_t>{entry.ts()} : std::nullopt;
        std::optional<uint64_t> end_ts = entry.has_end_ts() ? std::optional<uint64_t>{entry.end_ts()} : std::nullopt;
        auto handler = catalog_->GetTable(table->GetDB(), table->GetName());
        if (!handler) {
            response->set_code(::openmldb::base::ReturnCode::kDeleteFailed);
            response->set_msg("no TableHandler");
            PDLOG(WARNING, "no TableHandler. tid %u, pid %u", tid, pid);
            return;
        }
        auto tablet_table_handler = std::dynamic_pointer_cast<catalog::TabletTableHandler>(handler);
        if (!tablet_table_handler) {
            response->set_code(::openmldb::base::ReturnCode::kDeleteFailed);
            response->set_msg("convert TabletTableHandler failed");
            PDLOG(WARNING, "convert TabletTableHandler failed. tid %u, pid %u", tid, pid);
            return;
        }
        uint32_t pid_num = tablet_table_handler->GetPartitionNum();
        auto table_client_manager = tablet_table_handler->GetTableClientManager();
        if (entry.dimensions_size() > 0) {
            const auto& dimension = entry.dimensions(0);
            uint32_t idx = dimension.idx();
            auto index_def = table->GetIndex(idx);
            const auto& key = dimension.key();
            if (delete_others) {
                auto status = DeleteAllIndex(table, index_def, key, start_ts, end_ts, false,
                        table_client_manager, pid_num);
                if (!status.OK()) {
                    SET_RESP_AND_WARN(response, status.GetCode(), status.GetMsg());
                    return;
                }
            }
            if (!table->Delete(idx, key, start_ts, end_ts)) {
                response->set_code(::openmldb::base::ReturnCode::kDeleteFailed);
                response->set_msg("delete failed");
                return;
            }
            auto aggr = get_aggregator(aggrs, idx);
            if (aggr) {
                if (!aggr->Delete(key, start_ts, end_ts)) {
                    PDLOG(WARNING, "delete from aggr failed. base table: tid[%u] pid[%u] index[%u] key[%s]. "
                            "aggr table: tid[%u]",
                          tid, pid, idx, key.c_str(), aggr->GetAggrTid());
                    response->set_code(::openmldb::base::ReturnCode::kDeleteFailed);
                    response->set_msg("delete from associated pre-aggr table failed");
                    return;
                }
            }
            DEBUGLOG("delete ok. tid %u, pid %u, key %s", tid, pid, key.c_str());
        } else {
            bool is_first_hit_index = true;
            for (const auto& index_def : table->GetAllIndex()) {
                if (!index_def || !index_def->IsReady()) {
                    continue;
                }
                if (index_def->GetTsColumn()->GetName() != request->ts_name()) {
                    continue;
                }
                uint32_t idx = index_def->GetId();
                std::unique_ptr<storage::TraverseIterator> iter(table->NewTraverseIterator(idx));
                iter->SeekToFirst();
                while (iter->Valid()) {
                    auto pk = iter->GetPK();
                    if (delete_others && is_first_hit_index) {
                        auto status = DeleteAllIndex(table, index_def, pk, start_ts, end_ts, true,
                                table_client_manager, pid_num);
                        if (!status.OK()) {
                            SET_RESP_AND_WARN(response, status.GetCode(), status.GetMsg());
                            return;
                        }
                    }
                    iter->NextPK();
                    if (!table->Delete(idx, pk, start_ts, end_ts)) {
                        response->set_code(::openmldb::base::ReturnCode::kDeleteFailed);
                        response->set_msg("delete failed");
                        return;
                    }
                    auto aggr = get_aggregator(aggrs, idx);
                    if (aggr) {
                        if (!aggr->Delete(pk, start_ts, end_ts)) {
                            PDLOG(WARNING, "delete from aggr failed. base table: tid[%u] pid[%u] index[%u] key[%s]. "
                                    "aggr table: tid[%u]", tid, pid, idx, pk.c_str(), aggr->GetAggrTid());
                            response->set_code(::openmldb::base::ReturnCode::kDeleteFailed);
                            response->set_msg("delete from associated pre-aggr table failed");
                            return;
                        }
                    }
                }
                is_first_hit_index = false;
            }
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");

    replicator->AppendEntry(entry);
    if (FLAGS_binlog_notify_on_put) {
        replicator->Notify();
    }
}

void TabletImpl::Query(RpcController* ctrl, const openmldb::api::QueryRequest* request,
                       openmldb::api::QueryResponse* response, Closure* done) {
    DLOG(INFO) << "handle query request begin!";
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(ctrl);
    butil::IOBuf& buf = cntl->response_attachment();
    ProcessQuery(true, ctrl, request, response, &buf);
}

void TabletImpl::ProcessQuery(bool is_sub, RpcController* ctrl, const openmldb::api::QueryRequest* request,
                              ::openmldb::api::QueryResponse* response, butil::IOBuf* buf) {
    auto start = absl::Now();
    absl::Cleanup deploy_collect_task = [this, is_sub, request, start]() {
        if (this->IsCollectDeployStatsEnabled()) {
            if (!is_sub && request->is_procedure() && request->has_db() && request->has_sp_name()) {
                this->TryCollectDeployStats(request->db(), request->sp_name(), start);
            }
        }
    };

    ::hybridse::base::Status status;
    if (request->is_batch()) {
        // convert repeated openmldb:type::DataType into hybridse::codec::Schema
        hybridse::codec::Schema parameter_schema;
        for (int i = 0; i < request->parameter_types().size(); i++) {
            auto column = parameter_schema.Add();
            hybridse::type::Type hybridse_type;

            if (!openmldb::schema::SchemaAdapter::ConvertType(request->parameter_types(i), &hybridse_type)) {
                response->set_msg("Invalid parameter type: " +
                                  openmldb::type::DataType_Name(request->parameter_types(i)));
                response->set_code(::openmldb::base::kSQLCompileError);
                return;
            }
            column->set_type(hybridse_type);
        }
        ::hybridse::vm::BatchRunSession session;
        if (request->is_debug()) {
            session.EnableDebug();
        }
        session.SetParameterSchema(parameter_schema);
        {
            bool ok = engine_->Get(request->sql(), request->db(), session, status);
            if (!ok) {
                response->set_msg(status.msg);
                response->set_code(::openmldb::base::kSQLCompileError);
                DLOG(WARNING) << "fail to compile sql " << request->sql() << ", message: " << status.msg;
                return;
            }
        }

        ::hybridse::codec::Row parameter_row;
        auto& request_buf = static_cast<brpc::Controller*>(ctrl)->request_attachment();
        if (request->parameter_row_size() > 0 &&
            !codec::DecodeRpcRow(request_buf, 0, request->parameter_row_size(), request->parameter_row_slices(),
                                 &parameter_row)) {
            response->set_code(::openmldb::base::kSQLRunError);
            response->set_msg("fail to decode parameter row");
            return;
        }
        std::vector<::hybridse::codec::Row> output_rows;
        int32_t run_ret = session.Run(parameter_row, output_rows);
        if (run_ret != 0) {
            response->set_msg(status.msg);
            response->set_code(::openmldb::base::kSQLRunError);
            DLOG(WARNING) << "fail to run sql: " << request->sql();
            return;
        }
        uint32_t byte_size = 0;
        uint32_t count = 0;
        for (auto& output_row : output_rows) {
            if (FLAGS_scan_max_bytes_size > 0 && byte_size > FLAGS_scan_max_bytes_size) {
                LOG(WARNING) << "reach the max byte size " << FLAGS_scan_max_bytes_size << " truncate result";
                response->set_schema(session.GetEncodedSchema());
                response->set_byte_size(byte_size);
                response->set_count(count);
                response->set_code(::openmldb::base::kOk);
                return;
            }
            byte_size += output_row.size();
            buf->append(reinterpret_cast<void*>(output_row.buf()), output_row.size());
            count += 1;
        }
        response->set_schema(session.GetEncodedSchema());
        response->set_byte_size(byte_size);
        response->set_count(count);
        response->set_code(::openmldb::base::kOk);
        DLOG(INFO) << "handle batch sql " << request->sql() << " with record cnt " << count << " byte size "
                   << byte_size;
    } else {
        ::hybridse::vm::RequestRunSession session;
        if (request->is_debug()) {
            session.EnableDebug();
        }
        if (request->is_procedure()) {
            const std::string& db_name = request->db();
            const std::string& sp_name = request->sp_name();
            std::shared_ptr<hybridse::vm::CompileInfo> request_compile_info;
            {
                hybridse::base::Status status;
                request_compile_info = sp_cache_->GetRequestInfo(db_name, sp_name, status);
                if (!status.isOK()) {
                    response->set_code(::openmldb::base::ReturnCode::kProcedureNotFound);
                    response->set_msg(status.msg);
                    PDLOG(WARNING, status.msg.c_str());
                    return;
                }
            }
            session.SetCompileInfo(request_compile_info);
            session.SetSpName(sp_name);
            RunRequestQuery(ctrl, *request, session, *response, *buf);
        } else {
            bool ok = engine_->Get(request->sql(), request->db(), session, status);
            if (!ok || session.GetCompileInfo() == nullptr) {
                response->set_msg(status.msg);
                response->set_code(::openmldb::base::kSQLCompileError);
                DLOG(WARNING) << "fail to compile sql in request mode:\n" << request->sql();
                return;
            }
            RunRequestQuery(ctrl, *request, session, *response, *buf);
        }
        const std::string& sql = session.GetCompileInfo()->GetSql();
        if (response->code() != ::openmldb::base::kOk) {
            DLOG(WARNING) << "fail to run sql " << sql << " error msg: " << response->msg();
        } else {
            DLOG(INFO) << "handle request sql " << sql;
        }
    }
}

void TabletImpl::SubQuery(RpcController* ctrl, const openmldb::api::QueryRequest* request,
                          openmldb::api::QueryResponse* response, Closure* done) {
    DLOG(INFO) << "handle subquery request begin!";
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(ctrl);
    butil::IOBuf& buf = cntl->response_attachment();
    // subquery don't need to collect deploy stats
    ProcessQuery(true, ctrl, request, response, &buf);
}

void TabletImpl::SQLBatchRequestQuery(RpcController* ctrl, const openmldb::api::SQLBatchRequestQueryRequest* request,
                                      openmldb::api::SQLBatchRequestQueryResponse* response, Closure* done) {
    DLOG(INFO) << "handle query batch request begin!";
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(ctrl);
    butil::IOBuf& buf = cntl->response_attachment();
    return ProcessBatchRequestQuery(false, ctrl, request, response, buf);
}
void TabletImpl::ProcessBatchRequestQuery(bool is_sub, RpcController* ctrl,
                                          const openmldb::api::SQLBatchRequestQueryRequest* request,
                                          openmldb::api::SQLBatchRequestQueryResponse* response, butil::IOBuf& buf) {
    absl::Time start = absl::Now();
    absl::Cleanup deploy_collect_task = [this, is_sub, request, start]() {
        if (this->IsCollectDeployStatsEnabled()) {
            if (!is_sub && request->is_procedure() && request->has_db() && request->has_sp_name()) {
                this->TryCollectDeployStats(request->db(), request->sp_name(), start);
            }
        }
    };

    ::hybridse::base::Status status;
    ::hybridse::vm::BatchRequestRunSession session;
    // run session
    if (request->is_debug()) {
        session.EnableDebug();
    }
    bool is_procedure = request->is_procedure();

    if (is_procedure) {
        std::shared_ptr<hybridse::vm::CompileInfo> request_compile_info;
        {
            hybridse::base::Status status;
            request_compile_info = sp_cache_->GetBatchRequestInfo(request->db(), request->sp_name(), status);
            if (!status.isOK()) {
                response->set_code(::openmldb::base::ReturnCode::kProcedureNotFound);
                response->set_msg(status.msg);
                PDLOG(WARNING, status.msg.c_str());
                return;
            }
            session.SetCompileInfo(request_compile_info);
            session.SetSpName(request->sp_name());
        }
    } else {
        size_t common_column_num = request->common_column_indices().size();
        for (size_t i = 0; i < common_column_num; ++i) {
            auto col_idx = request->common_column_indices().Get(i);
            session.AddCommonColumnIdx(col_idx);
        }
        bool ok = engine_->Get(request->sql(), request->db(), session, status);
        if (!ok || session.GetCompileInfo() == nullptr) {
            response->set_msg(status.msg);
            response->set_code(::openmldb::base::kSQLCompileError);
            DLOG(WARNING) << "fail to get sql engine: \n" << request->sql() << "\n" << status.str();
            return;
        }
    }

    // fill input data
    auto compile_info = session.GetCompileInfo();
    if (compile_info == nullptr) {
        response->set_msg("compile info is null, should never happen");
        response->set_code(::openmldb::base::kSQLCompileError);
        return;
    }
    const auto& batch_request_info = compile_info->GetBatchRequestInfo();
    size_t common_column_num = batch_request_info.common_column_indices.size();
    bool has_common_and_uncommon_row = !request->has_task_id() && common_column_num > 0 &&
                                       common_column_num < static_cast<size_t>(session.GetRequestSchema().size());
    size_t input_row_num = request->row_sizes().size();
    if (request->common_slices() > 0 && input_row_num > 0) {
        input_row_num -= 1;
    } else if (has_common_and_uncommon_row) {
        response->set_msg("input common row is missing");
        response->set_code(::openmldb::base::kSQLRunError);
        return;
    }

    auto& io_buf = static_cast<brpc::Controller*>(ctrl)->request_attachment();
    size_t buf_offset = 0;
    std::vector<::hybridse::codec::Row> input_rows(input_row_num);
    if (has_common_and_uncommon_row) {
        size_t common_size = request->row_sizes().Get(0);
        ::hybridse::codec::Row common_row;
        if (!codec::DecodeRpcRow(io_buf, buf_offset, common_size, request->common_slices(), &common_row)) {
            response->set_msg("decode input common row failed");
            response->set_code(::openmldb::base::kSQLRunError);
            return;
        }
        buf_offset += common_size;
        for (size_t i = 0; i < input_row_num; ++i) {
            ::hybridse::codec::Row non_common_row;
            size_t non_common_size = request->row_sizes().Get(i + 1);
            if (!codec::DecodeRpcRow(io_buf, buf_offset, non_common_size, request->non_common_slices(),
                                     &non_common_row)) {
                response->set_msg("decode input non common row failed");
                response->set_code(::openmldb::base::kSQLRunError);
                return;
            }
            buf_offset += non_common_size;
            input_rows[i] = ::hybridse::codec::Row(1, common_row, 1, non_common_row);
        }
    } else {
        for (size_t i = 0; i < input_row_num; ++i) {
            size_t non_common_size = request->row_sizes().Get(i);
            if (!codec::DecodeRpcRow(io_buf, buf_offset, non_common_size, request->non_common_slices(),
                                     &input_rows[i])) {
                response->set_msg("decode input non common row failed");
                response->set_code(::openmldb::base::kSQLRunError);
                return;
            }
            buf_offset += non_common_size;
        }
    }
    std::vector<::hybridse::codec::Row> output_rows;
    int32_t run_ret = 0;
    if (request->has_task_id()) {
        run_ret = session.Run(request->task_id(), input_rows, output_rows);
    } else {
        run_ret = session.Run(input_rows, output_rows);
    }
    if (run_ret != 0) {
        response->set_msg(status.msg);
        response->set_code(::openmldb::base::kSQLRunError);
        DLOG(WARNING) << "fail to run sql: " << request->sql();
        return;
    }

    // fill output data
    size_t output_col_num = session.GetSchema().size();
    auto& output_common_indices = batch_request_info.output_common_column_indices;
    bool has_common_and_uncomon_slice =
        !request->has_task_id() && !output_common_indices.empty() && output_common_indices.size() < output_col_num;

    if (has_common_and_uncomon_slice && !output_rows.empty()) {
        const auto& first_row = output_rows[0];
        if (first_row.GetRowPtrCnt() != 2) {
            response->set_msg("illegal row ptrs: expect 2");
            response->set_code(::openmldb::base::kSQLRunError);
            LOG(WARNING) << "illegal row ptrs: expect 2";
            return;
        }
        buf.append(first_row.buf(0), first_row.size(0));
        response->add_row_sizes(first_row.size(0));
        response->set_common_slices(1);
    } else {
        response->set_common_slices(0);
    }
    response->set_non_common_slices(1);
    for (auto& output_row : output_rows) {
        if (has_common_and_uncomon_slice) {
            if (output_row.GetRowPtrCnt() != 2) {
                response->set_msg("illegal row ptrs: expect 2");
                response->set_code(::openmldb::base::kSQLRunError);
                LOG(WARNING) << "illegal row ptrs: expect 2";
                return;
            }
            buf.append(output_row.buf(1), output_row.size(1));
            response->add_row_sizes(output_row.size(1));
        } else {
            if (output_row.GetRowPtrCnt() != 1) {
                response->set_msg("illegal row ptrs: expect 1");
                response->set_code(::openmldb::base::kSQLRunError);
                LOG(WARNING) << "illegal row ptrs: expect 1";
                return;
            }
            buf.append(output_row.buf(0), output_row.size(0));
            response->add_row_sizes(output_row.size(0));
        }
    }

    // fill response
    for (size_t idx : output_common_indices) {
        response->add_common_column_indices(idx);
    }
    response->set_schema(session.GetEncodedSchema());
    response->set_count(output_rows.size());
    response->set_code(::openmldb::base::kOk);
    DLOG(INFO) << "handle batch request sql " << request->sql() << " with record cnt " << output_rows.size()
               << " with schema size " << session.GetSchema().size();
}
void TabletImpl::SubBatchRequestQuery(RpcController* ctrl, const openmldb::api::SQLBatchRequestQueryRequest* request,
                                      openmldb::api::SQLBatchRequestQueryResponse* response, Closure* done) {
    DLOG(INFO) << "handle subquery batch request begin!";
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(ctrl);
    butil::IOBuf& buf = cntl->response_attachment();
    return ProcessBatchRequestQuery(true, ctrl, request, response, buf);
}

void TabletImpl::ChangeRole(RpcController* controller, const ::openmldb::api::ChangeRoleRequest* request,
                            ::openmldb::api::ChangeRoleResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    auto table = GetTable(tid, pid);
    if (auto status = CheckTable(tid, pid, false, table); !status.OK()) {
        SetResponseStatus(status, response);
        return;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        response->set_code(::openmldb::base::ReturnCode::kReplicatorIsNotExist);
        response->set_msg("replicator does not exist");
        return;
    }
    bool is_leader = false;
    if (request->mode() == ::openmldb::api::TableMode::kTableLeader) {
        is_leader = true;
    }
    std::map<std::string, std::string> real_ep_map;
    for (int idx = 0; idx < request->replicas_size(); idx++) {
        real_ep_map.insert(std::make_pair(request->replicas(idx), ""));
    }
    if (FLAGS_use_name) {
        if (!GetRealEp(tid, pid, &real_ep_map)) {
            response->set_code(::openmldb::base::ReturnCode::kServerNameNotFound);
            response->set_msg("name not found in real_ep_map");
            return;
        }
    }
    if (is_leader) {
        {
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            if (table->IsLeader()) {
                PDLOG(WARNING, "table is leader. tid[%u] pid[%u]", tid, pid);
                response->set_code(::openmldb::base::ReturnCode::kTableIsLeader);
                response->set_msg("table is leader");
                return;
            }
            table->SetLeader(true);
            replicator->SetRole(ReplicatorRole::kLeaderNode);
            if (!zk_cluster_.empty()) {
                replicator->SetLeaderTerm(request->term());
            }
        }
        PDLOG(INFO, "change to leader. tid[%u] pid[%u] term[%lu]", tid, pid, request->term());
        if (catalog_->AddTable(*(table->GetTableMeta()), table)) {
            LOG(INFO) << "add table " << table->GetName() << " to catalog with db " << table->GetDB();
        } else {
            LOG(WARNING) << "fail to add table " << table->GetName() << " to catalog with db " << table->GetDB();
        }
        if (replicator->AddReplicateNode(real_ep_map) < 0) {
            PDLOG(WARNING, "add replicator failed. tid[%u] pid[%u]", tid, pid);
        }
        for (auto& e : request->endpoint_tid()) {
            std::map<std::string, std::string> r_real_ep_map;
            r_real_ep_map.insert(std::make_pair(e.endpoint(), ""));
            if (FLAGS_use_name) {
                if (!GetRealEp(tid, pid, &r_real_ep_map)) {
                    response->set_code(::openmldb::base::ReturnCode::kServerNameNotFound);
                    response->set_msg("name not found in r_real_ep_map");
                    return;
                }
            }
            replicator->AddReplicateNode(r_real_ep_map, e.tid());
        }
    } else {
        {
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            if (!table->IsLeader()) {
                PDLOG(WARNING, "table is follower. tid[%u] pid[%u]", tid, pid);
                response->set_code(::openmldb::base::ReturnCode::kOk);
                response->set_msg("table is follower");
                return;
            }
            replicator->DelAllReplicateNode();
            replicator->SetRole(ReplicatorRole::kFollowerNode);
            table->SetLeader(false);
        }
        PDLOG(INFO, "change to follower. tid[%u] pid[%u]", tid, pid);
        if (!table->GetDB().empty()) {
            catalog_->DeleteTable(table->GetDB(), table->GetName(), tid, pid);
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void TabletImpl::AddReplica(RpcController* controller, const ::openmldb::api::ReplicaRequest* request,
                            ::openmldb::api::AddReplicaResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kAddReplica, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    do {
        if (!table) {
            PDLOG(WARNING, "table does not exist. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
            response->set_msg("table does not exist");
            break;
        }
        if (!table->IsLeader()) {
            PDLOG(WARNING, "table is follower. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(::openmldb::base::ReturnCode::kTableIsFollower);
            response->set_msg("table is follower");
            break;
        }
        std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
        if (!replicator) {
            response->set_code(::openmldb::base::ReturnCode::kReplicatorIsNotExist);
            response->set_msg("replicator does not exist");
            PDLOG(WARNING, "replicator does not exist. tid %u, pid %u", request->tid(), request->pid());
            break;
        }
        std::map<std::string, std::string> real_ep_map;
        real_ep_map.insert(std::make_pair(request->endpoint(), ""));
        if (FLAGS_use_name) {
            if (!GetRealEp(request->tid(), request->pid(), &real_ep_map)) {
                response->set_code(::openmldb::base::ReturnCode::kServerNameNotFound);
                response->set_msg("name not found in real_ep_map");
                break;
            }
        }
        int ret = -1;
        if (request->has_remote_tid()) {
            ret = replicator->AddReplicateNode(real_ep_map, request->remote_tid());
        } else {
            ret = replicator->AddReplicateNode(real_ep_map);
        }
        if (ret == 0) {
            response->set_code(::openmldb::base::ReturnCode::kOk);
            response->set_msg("ok");
        } else if (ret < 0) {
            response->set_code(::openmldb::base::ReturnCode::kFailToAddReplicaEndpoint);
            PDLOG(WARNING, "fail to add replica endpoint. tid %u pid %u", request->tid(), request->pid());
            response->set_msg("fail to add replica endpoint");
            break;
        } else {
            response->set_code(::openmldb::base::ReturnCode::kReplicaEndpointAlreadyExists);
            response->set_msg("replica endpoint already exists");
            PDLOG(WARNING, "replica endpoint already exists. tid %u pid %u", request->tid(), request->pid());
        }
        if (task_ptr) {
            std::lock_guard<std::mutex> lock(mu_);
            task_ptr->set_status(::openmldb::api::TaskStatus::kDone);
        }
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

void TabletImpl::DelReplica(RpcController* controller, const ::openmldb::api::ReplicaRequest* request,
                            ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kDelReplica, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    do {
        if (!table) {
            PDLOG(WARNING, "table does not exist. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
            response->set_msg("table does not exist");
            break;
        }
        if (!table->IsLeader()) {
            PDLOG(WARNING, "table is follower. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(::openmldb::base::ReturnCode::kTableIsFollower);
            response->set_msg("table is follower");
            break;
        }
        std::shared_ptr<LogReplicator> replicator = GetReplicator(request->tid(), request->pid());
        if (!replicator) {
            response->set_code(::openmldb::base::ReturnCode::kReplicatorIsNotExist);
            response->set_msg("replicator does not exist");
            PDLOG(WARNING, "replicator does not exist. tid %u, pid %u", request->tid(), request->pid());
            break;
        }
        int ret = replicator->DelReplicateNode(request->endpoint());
        if (ret == 0) {
            response->set_code(::openmldb::base::ReturnCode::kOk);
            response->set_msg("ok");
        } else if (ret < 0) {
            response->set_code(::openmldb::base::ReturnCode::kReplicatorRoleIsNotLeader);
            PDLOG(WARNING, "replicator role is not leader. table %u pid %u", request->tid(), request->pid());
            response->set_msg("replicator role is not leader");
            break;
        } else {
            response->set_code(::openmldb::base::ReturnCode::kOk);
            PDLOG(WARNING,
                  "fail to del endpoint for table %u pid %u. replica does not "
                  "exist",
                  request->tid(), request->pid());
            response->set_msg("replica does not exist");
        }
        if (task_ptr) {
            std::lock_guard<std::mutex> lock(mu_);
            task_ptr->set_status(::openmldb::api::TaskStatus::kDone);
        }
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

void TabletImpl::AppendEntries(RpcController* controller, const ::openmldb::api::AppendEntriesRequest* request,
                               ::openmldb::api::AppendEntriesResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    auto table = GetTable(tid, pid);
    if (auto status = CheckTable(tid, pid, false, table); !status.OK()) {
        SetResponseStatus(status, response);
        return;
    }
    if (!follower_.load(std::memory_order_relaxed) && table->IsLeader()) {
        PDLOG(WARNING, "table is leader. tid %u, pid %u", tid, pid);
        response->set_code(::openmldb::base::ReturnCode::kTableIsLeader);
        response->set_msg("table is leader");
        return;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        response->set_code(::openmldb::base::ReturnCode::kReplicatorIsNotExist);
        response->set_msg("replicator does not exist");
        return;
    }
    uint64_t term = replicator->GetLeaderTerm();
    if (!follower_.load(std::memory_order_relaxed)) {
        if (!FLAGS_zk_cluster.empty() && request->term() < term) {
            PDLOG(WARNING, "leader id not match. request term  %lu, cur term %lu, tid %u, pid %u", request->term(),
                  term, tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kFailToAppendEntriesToReplicator);
            response->set_msg("fail to append entries to replicator");
            return;
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
    uint64_t last_log_offset = replicator->GetOffset();
    if (request->pre_log_index() == 0 && request->entries_size() == 0) {
        response->set_log_offset(last_log_offset);
        if (!FLAGS_zk_cluster.empty() && request->term() > term) {
            replicator->SetLeaderTerm(request->term());
            PDLOG(INFO, "get log_offset %lu and set term %lu. tid %u, pid %u", last_log_offset, request->term(), tid,
                  pid);
            return;
        }
        PDLOG(INFO, "first sync log_index! log_offset[%lu] tid[%u] pid[%u]", last_log_offset, tid, pid);
        return;
    }
    for (int32_t i = 0; i < request->entries_size(); i++) {
        const auto& entry = request->entries(i);
        if (entry.log_index() <= last_log_offset) {
            PDLOG(WARNING, "entry log_index %lu cur log_offset %lu tid %u pid %u", request->entries(i).log_index(),
                  last_log_offset, tid, pid);
            continue;
        }
        if (!replicator->ApplyEntry(entry)) {
            PDLOG(WARNING, "fail to write binlog. tid %u pid %u", tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kFailToAppendEntriesToReplicator);
            response->set_msg("fail to append entries to replicator");
            return;
        }
        if (entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete) {
            table->Delete(entry);         // TODO(hw): error handle
        } else if (!table->Put(entry)) {  // put if type is not delete
            PDLOG(WARNING, "fail to put entry. tid %u pid %u", tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kFailToAppendEntriesToReplicator);
            response->set_msg("fail to append entry to table");
            return;
        }
    }
    response->set_log_offset(replicator->GetOffset());
}

void TabletImpl::GetTableSchema(RpcController* controller, const ::openmldb::api::GetTableSchemaRequest* request,
                                ::openmldb::api::GetTableSchemaResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        PDLOG(WARNING, "table does not exist. tid %u, pid %u", request->tid(), request->pid());
        return;
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
    response->mutable_table_meta()->CopyFrom(*table->GetTableMeta());
}

void TabletImpl::UpdateTableMetaForAddField(RpcController* controller,
                                            const ::openmldb::api::UpdateTableMetaForAddFieldRequest* request,
                                            ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    std::map<uint32_t, std::shared_ptr<Table>> table_map;
    {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        auto it = tables_.find(tid);
        if (it == tables_.end()) {
            response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
            response->set_msg("table doesn`t exist");
            PDLOG(WARNING, "table tid %u doesn`t exist.", tid);
            return;
        }
        table_map = it->second;
    }
    for (auto pit = table_map.begin(); pit != table_map.end(); ++pit) {
        uint32_t pid = pit->first;
        std::shared_ptr<Table> table = pit->second;
        // judge if field exists
        ::openmldb::api::TableMeta table_meta(*table->GetTableMeta());
        if (request->has_column_desc()) {
            const auto& col = request->column_desc();
            if (table->CheckFieldExist(col.name())) {
                continue;
            }
            openmldb::common::ColumnDesc* column_desc = table_meta.add_added_column_desc();
            column_desc->CopyFrom(col);
        } else {
            bool do_continue = false;
            const auto& cols = request->column_descs();
            for (const auto& col : cols) {
                if (table->CheckFieldExist(col.name())) {
                    do_continue = true;
                    break;
                }
            }
            if (do_continue) {
                continue;
            }
            for (const auto& col : cols) {
                openmldb::common::ColumnDesc* column_desc = table_meta.add_added_column_desc();
                column_desc->CopyFrom(col);
            }
        }
        if (request->has_version_pair()) {
            openmldb::common::VersionPair* pair = table_meta.add_schema_versions();
            pair->CopyFrom(request->version_pair());
            LOG(INFO) << "add version pair";
        }
        table->SetTableMeta(table_meta);
        // update TableMeta.txt
        std::string db_root_path;
        ::openmldb::common::StorageMode mode = table_meta.storage_mode();
        bool ok = ChooseDBRootPath(tid, pid, mode, db_root_path);
        if (!ok) {
            response->set_code(ReturnCode::kFailToGetDbRootPath);
            response->set_msg("fail to get db root path");
            LOG(WARNING) << "fail to get table db root path for tid " << tid << " pid " << pid;
            return;
        }
        std::string db_path = GetDBPath(db_root_path, tid, pid);
        if (!::openmldb::base::IsExists(db_path)) {
            LOG(WARNING) << "table db path doesn't exist. tid " << tid << " pid " << pid;
            response->set_code(ReturnCode::kTableDbPathIsNotExist);
            response->set_msg("table db path does not exist");
            return;
        }
        UpdateTableMeta(db_path, &table_meta, true);
        if (WriteTableMeta(db_path, &table_meta) < 0) {
            LOG(WARNING) << " write table_meta failed. tid[" << tid << "] pid [" << pid << "]";
            response->set_code(ReturnCode::kWriteDataFailed);
            response->set_msg("write data failed");
            return;
        }
        LOG(INFO) << "success update table meta for table " << tid << " pid " << pid;
    }
    response->set_code(ReturnCode::kOk);
    response->set_msg("ok");
}

void TabletImpl::GetTableStatus(RpcController* controller, const ::openmldb::api::GetTableStatusRequest* request,
                                ::openmldb::api::GetTableStatusResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
    for (auto it = tables_.begin(); it != tables_.end(); ++it) {
        if (request->has_tid() && request->tid() != it->first) {
            continue;
        }
        for (auto pit = it->second.begin(); pit != it->second.end(); ++pit) {
            if (request->has_pid() && request->pid() != pit->first) {
                continue;
            }
            std::shared_ptr<Table> table = pit->second;
            ::openmldb::api::TableStatus* status = response->add_all_table_status();
            status->set_mode(::openmldb::api::TableMode::kTableFollower);
            if (table->IsLeader()) {
                status->set_mode(::openmldb::api::TableMode::kTableLeader);
            }
            status->set_tid(table->GetId());
            status->set_pid(table->GetPid());
            status->set_compress_type(table->GetCompressType());
            status->set_storage_mode(table->GetStorageMode());
            status->set_name(table->GetName());
            status->set_diskused(table->GetDiskused());
            if (::openmldb::api::TableState_IsValid(table->GetTableStat())) {
                status->set_state(::openmldb::api::TableState(table->GetTableStat()));
            }
            std::shared_ptr<LogReplicator> replicator = GetReplicatorUnLock(table->GetId(), table->GetPid());
            if (replicator) {
                status->set_offset(replicator->GetOffset());
            }
            status->set_record_cnt(table->GetRecordCnt());
            if (table->GetStorageMode() == common::kMemory) {
                if (MemTable* mem_table = dynamic_cast<MemTable*>(table.get())) {
                    status->set_is_expire(mem_table->GetExpireStatus());
                    status->set_record_byte_size(mem_table->GetRecordByteSize());
                    status->set_record_idx_byte_size(mem_table->GetRecordIdxByteSize());
                    status->set_record_pk_cnt(mem_table->GetRecordPkCnt());
                    status->set_skiplist_height(mem_table->GetKeyEntryHeight());
                    uint64_t record_idx_cnt = 0;
                    auto indexs = table->GetAllIndex();
                    for (const auto& index_def : indexs) {
                        ::openmldb::api::TsIdxStatus* ts_idx_status = status->add_ts_idx_status();
                        ts_idx_status->set_idx_name(index_def->GetName());
                        uint64_t* stats = nullptr;
                        uint32_t size = 0;
                        bool ok = mem_table->GetRecordIdxCnt(index_def->GetId(), &stats, &size);
                        if (ok) {
                            for (uint32_t i = 0; i < size; i++) {
                                ts_idx_status->add_seg_cnts(stats[i]);
                                record_idx_cnt += stats[i];
                            }
                        }
                        delete[] stats;
                    }
                    status->set_idx_cnt(record_idx_cnt);
                }
            } else {
                // status about disk table's data paths
                // snapshot path from DiskTableSnapshot
                auto snapshot = GetSnapshotUnLock(table->GetId(), table->GetPid());
                if (snapshot) {
                    status->set_snapshot_path(snapshot->GetSnapshotPath());
                } else {
                    LOG(WARNING) << "snapshot is null. tid " << table->GetId() << " pid " << table->GetPid();
                }

                // binlog path from LogReplicator
                auto log_rep = GetReplicatorUnLock(table->GetId(), table->GetPid());
                if (log_rep) {
                    // LogPath may be relative path, so we need to convert it to absolute path
                    std::filesystem::path p = log_rep->GetLogPath();
                    std::error_code ec;
                    auto abs_path = std::filesystem::absolute(p, ec);
                    if (ec) {
                        LOG(WARNING) << "log_rep path is not absolute path. tid " << table->GetId() << " pid "
                                     << table->GetPid();
                    } else {
                        status->set_binlog_path(abs_path);
                    }
                } else {
                    LOG(WARNING) << "log_rep is null. tid " << table->GetId() << " pid " << table->GetPid();
                }
            }
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
}

void TabletImpl::SetExpire(RpcController* controller, const ::openmldb::api::SetExpireRequest* request,
                           ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table does not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    if (table->GetStorageMode() == common::kMemory) {
        MemTable* mem_table = dynamic_cast<MemTable*>(table.get());
        if (mem_table != nullptr) {
            mem_table->SetExpire(request->is_expire());
            PDLOG(INFO, "set table expire[%d]. tid[%u] pid[%u]", request->is_expire(), request->tid(), request->pid());
        }
    } else {
        PDLOG(WARNING, "table is not memtable. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(::openmldb::base::ReturnCode::kOperatorNotSupport);
        response->set_msg("table is not memtable");
        return;
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void TabletImpl::MakeSnapshotInternal(uint32_t tid, uint32_t pid, uint64_t end_offset,
        std::shared_ptr<::openmldb::api::TaskInfo> task, bool is_force) {
    PDLOG(INFO, "MakeSnapshotInternal begin, tid[%u] pid[%u]", tid, pid);
    std::shared_ptr<Table> table;
    std::shared_ptr<Snapshot> snapshot;
    std::shared_ptr<LogReplicator> replicator;
    bool has_error = true;
    do {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        table = GetTableUnLock(tid, pid);
        if (!table) {
            PDLOG(WARNING, "table does not exist. tid[%u] pid[%u]", tid, pid);
            break;
        }
        if (table->GetTableStat() != ::openmldb::storage::kNormal) {
            PDLOG(WARNING, "table state is %d, cannot make snapshot. %u, pid %u", table->GetTableStat(), tid, pid);
            break;
        }
        snapshot = GetSnapshotUnLock(tid, pid);
        if (!snapshot) {
            PDLOG(WARNING, "snapshot does not exist. tid[%u] pid[%u]", tid, pid);
            break;
        }
        replicator = GetReplicatorUnLock(tid, pid);
        if (!replicator) {
            PDLOG(WARNING, "replicator does not exist. tid[%u] pid[%u]", tid, pid);
            break;
        }
        has_error = false;
    } while (0);
    if (has_error) {
        if (task) {
            std::lock_guard<std::mutex> lock(mu_);
            task->set_status(::openmldb::api::kFailed);
        }
        return;
    }
    {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        table->SetTableStat(::openmldb::storage::kMakingSnapshot);
    }
    uint64_t cur_offset = replicator->GetOffset();
    uint64_t snapshot_offset = snapshot->GetOffset();
    int ret = 0;
    if (!is_force && cur_offset < snapshot_offset + FLAGS_make_snapshot_threshold_offset && end_offset == 0) {
        PDLOG(INFO,
              "offset can't reach the threshold. tid[%u] pid[%u] "
              "cur_offset[%lu], snapshot_offset[%lu] end_offset[%lu]",
              tid, pid, cur_offset, snapshot_offset, end_offset);
    } else {
        uint64_t offset = 0;
        ret = snapshot->MakeSnapshot(table, offset, end_offset, replicator->GetLeaderTerm());
        if (ret == 0) {
            replicator->SetSnapshotLogPartIndex(offset);
        }
    }
    {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        table->SetTableStat(::openmldb::storage::kNormal);
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (task) {
            if (ret == 0) {
                task->set_status(::openmldb::api::kDone);
                auto right_now = std::chrono::system_clock::now().time_since_epoch();
                int64_t ts = std::chrono::duration_cast<std::chrono::seconds>(right_now).count();
                table->SetMakeSnapshotTime(ts);
            } else {
                task->set_status(::openmldb::api::kFailed);
            }
        }
    }
    PDLOG(INFO, "MakeSnapshotInternal finish, tid[%u] pid[%u]", tid, pid);
}

void TabletImpl::MakeSnapshot(RpcController* controller, const ::openmldb::api::GeneralRequest* request,
                              ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kMakeSnapshot, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    uint64_t offset = 0;
    if (request->has_offset() && request->offset() > 0) {
        offset = request->offset();
    }
    do {
        {
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            std::shared_ptr<Snapshot> snapshot = GetSnapshotUnLock(tid, pid);
            if (!snapshot) {
                response->set_code(::openmldb::base::ReturnCode::kSnapshotIsNotExist);
                response->set_msg("snapshot does not exist");
                PDLOG(WARNING, "snapshot does not exist. tid[%u] pid[%u]", tid, pid);
                break;
            }
            std::shared_ptr<Table> table = GetTableUnLock(request->tid(), request->pid());
            if (!table) {
                PDLOG(WARNING, "table does not exist. tid %u, pid %u", tid, pid);
                response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
                response->set_msg("table does not exist");
                break;
            }
            if (table->GetTableStat() != ::openmldb::storage::kNormal) {
                response->set_code(::openmldb::base::ReturnCode::kTableStatusIsNotKnormal);
                response->set_msg("table status is not kNormal");
                PDLOG(WARNING, "table state is %d, cannot make snapshot. %u, pid %u", table->GetTableStat(), tid, pid);
                break;
            }
        }
        snapshot_pool_.AddTask(boost::bind(&TabletImpl::MakeSnapshotInternal, this, tid, pid, offset, task_ptr, false));
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

void TabletImpl::SchedMakeSnapshot() {
    int now_hour = ::openmldb::base::GetNowHour();
    if (now_hour != FLAGS_make_snapshot_time) {
        snapshot_pool_.DelayTask(FLAGS_make_snapshot_check_interval, boost::bind(&TabletImpl::SchedMakeSnapshot, this));
        return;
    }
    std::vector<std::pair<uint32_t, uint32_t>> table_set;
    {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        auto right_now = std::chrono::system_clock::now().time_since_epoch();
        int64_t ts = std::chrono::duration_cast<std::chrono::seconds>(right_now).count();
        for (auto iter = tables_.begin(); iter != tables_.end(); ++iter) {
            for (auto inner = iter->second.begin(); inner != iter->second.end(); ++inner) {
                if (iter->first == 0 && inner->first == 0) {
                    continue;
                }
                if (ts - inner->second->GetMakeSnapshotTime() <= FLAGS_make_snapshot_offline_interval &&
                    !zk_cluster_.empty()) {
                    continue;
                }
                table_set.push_back(std::make_pair(iter->first, inner->first));
            }
        }
    }
    for (auto iter = table_set.begin(); iter != table_set.end(); ++iter) {
        PDLOG(INFO, "start make snapshot tid[%u] pid[%u]", iter->first, iter->second);
        MakeSnapshotInternal(iter->first, iter->second, 0, std::shared_ptr<::openmldb::api::TaskInfo>(), false);
    }
    // delay task one hour later avoid execute  more than one time
    snapshot_pool_.DelayTask(FLAGS_make_snapshot_check_interval + 60 * 60 * 1000,
                             boost::bind(&TabletImpl::SchedMakeSnapshot, this));
}

void TabletImpl::SendData(RpcController* controller, const ::openmldb::api::SendDataRequest* request,
                          ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::string db_root_path;
    ::openmldb::common::StorageMode mode = ::openmldb::common::kMemory;
    if (request->has_storage_mode()) {
        mode = request->storage_mode();
    }
    bool ok = ChooseDBRootPath(tid, pid, mode, db_root_path);
    if (!ok) {
        response->set_code(::openmldb::base::ReturnCode::kFailToGetDbRootPath);
        response->set_msg("fail to get db root path");
        PDLOG(WARNING, "fail to get table db root path for tid %u, pid %u", tid, pid);
        return;
    }
    std::string combine_key = std::to_string(tid) + "_" + std::to_string(pid) + "_" + request->file_name();
    std::shared_ptr<FileReceiver> receiver;
    std::shared_ptr<Table> table;
    if (request->block_id() == 0) {
        table = GetTable(tid, pid);
    }
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = file_receiver_map_.find(combine_key);
        if (request->block_id() == 0) {
            if (table && request->dir_name() != "index") {
                PDLOG(WARNING, "table already exists. tid %u, pid %u", tid, pid);
                response->set_code(::openmldb::base::ReturnCode::kTableAlreadyExists);
                response->set_msg("table already exists");
                return;
            }
            if (iter == file_receiver_map_.end()) {
                std::string path = GetDBPath(db_root_path, tid, pid) + "/";
                std::string dir_name;
                if (request->has_dir_name() && request->dir_name().size() > 0) {
                    dir_name = request->dir_name();
                    if (dir_name != "index") {
                        path.append("snapshot/");
                    }
                    path.append(request->dir_name() + "/");
                } else if (request->file_name() != "table_meta.txt") {
                    path.append("snapshot/");
                }
                file_receiver_map_.insert(
                    std::make_pair(combine_key, std::make_shared<FileReceiver>(request->file_name(), dir_name, path)));
                iter = file_receiver_map_.find(combine_key);
            }
            if (!iter->second->Init()) {
                PDLOG(WARNING, "file receiver init failed. tid %u, pid %u, file_name %s", tid, pid,
                      request->file_name().c_str());
                response->set_code(::openmldb::base::ReturnCode::kFileReceiverInitFailed);
                response->set_msg("file receiver init failed");
                file_receiver_map_.erase(iter);
                return;
            }
            PDLOG(INFO, "file receiver init ok. tid %u, pid %u, file_name %s", tid, pid, request->file_name().c_str());
            response->set_code(::openmldb::base::ReturnCode::kOk);
            response->set_msg("ok");
        } else if (iter == file_receiver_map_.end()) {
            PDLOG(WARNING, "cannot find receiver. tid %u, pid %u, file_name %s", tid, pid,
                  request->file_name().c_str());
            response->set_code(::openmldb::base::ReturnCode::kCannotFindReceiver);
            response->set_msg("cannot find receiver");
            return;
        }
        receiver = iter->second;
    }
    if (!receiver) {
        PDLOG(WARNING, "cannot find receiver. tid %u, pid %u, file_name %s", tid, pid, request->file_name().c_str());
        response->set_code(::openmldb::base::ReturnCode::kCannotFindReceiver);
        response->set_msg("cannot find receiver");
        return;
    }
    if (receiver->GetBlockId() == request->block_id()) {
        response->set_msg("ok");
        response->set_code(::openmldb::base::ReturnCode::kOk);
        return;
    }
    if (request->block_id() != receiver->GetBlockId() + 1) {
        response->set_msg("block_id mismatch");
        PDLOG(WARNING,
              "block_id mismatch. tid %u, pid %u, file_name %s, request "
              "block_id %lu cur block_id %lu",
              tid, pid, request->file_name().c_str(), request->block_id(), receiver->GetBlockId());
        response->set_code(::openmldb::base::ReturnCode::kBlockIdMismatch);
        return;
    }
    std::string data = cntl->request_attachment().to_string();
    if (data.length() != request->block_size()) {
        PDLOG(WARNING,
              "receive data error. tid %u, pid %u, file_name %s, expected "
              "length %u real length %u",
              tid, pid, request->file_name().c_str(), request->block_size(), data.length());
        response->set_code(::openmldb::base::ReturnCode::kReceiveDataError);
        response->set_msg("receive data error");
        return;
    }
    if (receiver->WriteData(data, request->block_id()) < 0) {
        PDLOG(WARNING, "receiver write data failed. tid %u, pid %u, file_name %s", tid, pid,
              request->file_name().c_str());
        response->set_code(::openmldb::base::ReturnCode::kWriteDataFailed);
        response->set_msg("write data failed");
        return;
    }
    if (request->eof()) {
        receiver->SaveFile();
        std::lock_guard<std::mutex> lock(mu_);
        file_receiver_map_.erase(combine_key);
    }
    response->set_msg("ok");
    response->set_code(::openmldb::base::ReturnCode::kOk);
}

void TabletImpl::SendSnapshot(RpcController* controller, const ::openmldb::api::SendSnapshotRequest* request,
                              ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kSendSnapshot, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::string sync_snapshot_key = request->endpoint() + "_" + std::to_string(tid) + "_" + std::to_string(pid);
    do {
        {
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            std::shared_ptr<Table> table = GetTableUnLock(tid, pid);
            if (!table) {
                PDLOG(WARNING, "table does not exist. tid %u, pid %u", tid, pid);
                response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
                response->set_msg("table does not exist");
                break;
            }
            if (!table->IsLeader()) {
                PDLOG(WARNING, "table is follower. tid %u, pid %u", tid, pid);
                response->set_code(::openmldb::base::ReturnCode::kTableIsFollower);
                response->set_msg("table is follower");
                break;
            }
            if (table->GetTableStat() != ::openmldb::storage::kSnapshotPaused) {
                PDLOG(WARNING, "table status is not kSnapshotPaused. tid %u, pid %u", tid, pid);
                response->set_code(::openmldb::base::ReturnCode::kTableStatusIsNotKsnapshotpaused);
                response->set_msg("table status is not kSnapshotPaused");
                break;
            }
        }
        std::lock_guard<std::mutex> lock(mu_);
        if (sync_snapshot_set_.find(sync_snapshot_key) != sync_snapshot_set_.end()) {
            PDLOG(WARNING, "snapshot is sending. tid %u pid %u endpoint %s", tid, pid, request->endpoint().c_str());
            response->set_code(::openmldb::base::ReturnCode::kSnapshotIsSending);
            response->set_msg("snapshot is sending");
            break;
        }
        sync_snapshot_set_.insert(sync_snapshot_key);
        task_pool_.AddTask(boost::bind(&TabletImpl::SendSnapshotInternal, this, request->endpoint(), tid, pid,
                                       request->remote_tid(), task_ptr));
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

void TabletImpl::SendSnapshotInternal(const std::string& endpoint, uint32_t tid, uint32_t pid, uint32_t remote_tid,
                                      std::shared_ptr<::openmldb::api::TaskInfo> task) {
    bool has_error = true;
    do {
        std::shared_ptr<Table> table = GetTable(tid, pid);
        if (!table) {
            PDLOG(WARNING, "table does not exist. tid %u, pid %u", tid, pid);
            break;
        }
        std::string db_root_path;
        bool ok = ChooseDBRootPath(tid, pid, table->GetStorageMode(), db_root_path);
        if (!ok) {
            PDLOG(WARNING, "fail to get db root path for table tid %u, pid %u", tid, pid);
            break;
        }
        std::string real_endpoint = endpoint;
        if (FLAGS_use_name) {
            auto tmp_map = std::atomic_load_explicit(&real_ep_map_, std::memory_order_acquire);
            auto iter = tmp_map->find(endpoint);
            if (iter == tmp_map->end()) {
                PDLOG(WARNING,
                      "name %s not found in real_ep_map."
                      "tid[%u] pid[%u]",
                      endpoint.c_str(), tid, pid);
                break;
            }
            real_endpoint = iter->second;
        }
        FileSender sender(remote_tid, pid, table->GetStorageMode(), real_endpoint);
        if (!sender.Init()) {
            PDLOG(WARNING, "Init FileSender failed. tid[%u] pid[%u] endpoint[%s]", tid, pid, endpoint.c_str());
            break;
        }
        // send table_meta file
        std::string full_path = GetDBPath(db_root_path, tid, pid) + "/";
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
                PDLOG(WARNING, "[%s] does not exist", manifest_file.c_str());
                has_error = false;
                break;
            }
            google::protobuf::io::FileInputStream fileInput(fd);
            fileInput.SetCloseOnDelete(true);
            ::openmldb::api::Manifest manifest;
            if (!google::protobuf::TextFormat::Parse(&fileInput, &manifest)) {
                PDLOG(WARNING, "parse manifest failed. tid[%u] pid[%u]", tid, pid);
                break;
            }
            snapshot_file = manifest.name();
        }
        if (table->GetStorageMode() == common::kMemory) {
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
    } while (0);
    std::lock_guard<std::mutex> lock(mu_);
    if (task) {
        if (has_error) {
            task->set_status(::openmldb::api::kFailed);
        } else {
            task->set_status(::openmldb::api::kDone);
        }
    }
    std::string sync_snapshot_key = endpoint + "_" + std::to_string(tid) + "_" + std::to_string(pid);
    sync_snapshot_set_.erase(sync_snapshot_key);
}

void TabletImpl::PauseSnapshot(RpcController* controller, const ::openmldb::api::GeneralRequest* request,
                               ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kPauseSnapshot, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    do {
        {
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            std::shared_ptr<Table> table = GetTableUnLock(request->tid(), request->pid());
            if (!table) {
                PDLOG(WARNING, "table does not exist. tid %u, pid %u", request->tid(), request->pid());
                response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
                response->set_msg("table does not exist");
                break;
            }
            if (table->GetTableStat() == ::openmldb::storage::kSnapshotPaused) {
                PDLOG(INFO,
                      "table status is kSnapshotPaused, need not pause. "
                      "tid[%u] pid[%u]",
                      request->tid(), request->pid());
            } else if (table->GetTableStat() != ::openmldb::storage::kNormal) {
                PDLOG(WARNING, "table status is [%u], cann't pause. tid[%u] pid[%u]", table->GetTableStat(),
                      request->tid(), request->pid());
                response->set_code(::openmldb::base::ReturnCode::kTableStatusIsNotKnormal);
                response->set_msg("table status is not kNormal");
                break;
            } else {
                table->SetTableStat(::openmldb::storage::kSnapshotPaused);
                PDLOG(INFO, "table status has set[%u]. tid[%u] pid[%u]", table->GetTableStat(), request->tid(),
                      request->pid());
            }
        }
        if (task_ptr) {
            std::lock_guard<std::mutex> lock(mu_);
            task_ptr->set_status(::openmldb::api::TaskStatus::kDone);
        }
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

void TabletImpl::RecoverSnapshot(RpcController* controller, const ::openmldb::api::GeneralRequest* request,
                                 ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kRecoverSnapshot, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    do {
        {
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            std::shared_ptr<Table> table = GetTableUnLock(request->tid(), request->pid());
            if (!table) {
                PDLOG(WARNING, "table does not exist. tid %u, pid %u", request->tid(), request->pid());
                response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
                response->set_msg("table does not exist");
                break;
            }
            if (table->GetTableStat() == openmldb::storage::kNormal) {
                PDLOG(INFO,
                      "table status is already kNormal, need not recover. "
                      "tid[%u] pid[%u]",
                      request->tid(), request->pid());

            } else if (table->GetTableStat() != ::openmldb::storage::kSnapshotPaused) {
                PDLOG(WARNING, "table status is [%u], cann't recover. tid[%u] pid[%u]", table->GetTableStat(),
                      request->tid(), request->pid());
                response->set_code(::openmldb::base::ReturnCode::kTableStatusIsNotKsnapshotpaused);
                response->set_msg("table status is not kSnapshotPaused");
                break;
            } else {
                table->SetTableStat(::openmldb::storage::kNormal);
                PDLOG(INFO, "table status has set[%u]. tid[%u] pid[%u]", table->GetTableStat(), request->tid(),
                      request->pid());
            }
        }
        std::lock_guard<std::mutex> lock(mu_);
        if (task_ptr) {
            task_ptr->set_status(::openmldb::api::TaskStatus::kDone);
        }
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

void TabletImpl::LoadTable(RpcController* controller, const ::openmldb::api::LoadTableRequest* request,
                           ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kLoadTable, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    do {
        ::openmldb::api::TableMeta table_meta;
        table_meta.CopyFrom(request->table_meta());
        uint32_t tid = table_meta.tid();
        uint32_t pid = table_meta.pid();
        std::string msg;
        if (CheckTableMeta(&table_meta, msg) != 0) {
            response->set_code(::openmldb::base::ReturnCode::kTableMetaIsIllegal);
            response->set_msg(msg);
            PDLOG(WARNING, "CheckTableMeta failed. tid %u, pid %u", tid, pid);
            break;
        }
        std::string root_path;
        bool ok = ChooseDBRootPath(tid, pid, table_meta.storage_mode(), root_path);
        if (!ok) {
            response->set_code(::openmldb::base::ReturnCode::kFailToGetDbRootPath);
            response->set_msg("fail to get table db root path");
            PDLOG(WARNING, "table db path is not found. tid %u, pid %u", tid, pid);
            break;
        }

        std::string db_path = GetDBPath(root_path, tid, pid);
        if (!::openmldb::base::IsExists(db_path)) {
            PDLOG(WARNING, "table db path does not exist, but still load. tid %u, pid %u, path %s",
                    tid, pid, db_path.c_str());
        }

        std::shared_ptr<Table> table = GetTable(tid, pid);
        if (table) {
            PDLOG(WARNING, "table with tid[%u] and pid[%u] exists", tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kTableAlreadyExists);
            response->set_msg("table already exists");
            break;
        }

        UpdateTableMeta(db_path, &table_meta);
        if (WriteTableMeta(db_path, &table_meta) < 0) {
            PDLOG(WARNING, "write table_meta failed. tid[%lu] pid[%lu]", tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kWriteDataFailed);
            response->set_msg("write data failed");
            break;
        }

        if (table_meta.storage_mode() == openmldb::common::kMemory) {
            if (CreateTableInternal(&table_meta, msg) < 0) {
                response->set_code(::openmldb::base::ReturnCode::kCreateTableFailed);
                response->set_msg(msg.c_str());
                break;
            }
            std::string name = table_meta.name();
            uint32_t seg_cnt = 8;
            if (table_meta.seg_cnt() > 0) {
                seg_cnt = table_meta.seg_cnt();
            }
            PDLOG(INFO, "start to recover table with id %u pid %u name %s seg_cnt %d ", tid, pid, name.c_str(),
                  seg_cnt);
            task_pool_.AddTask(boost::bind(&TabletImpl::LoadTableInternal, this, tid, pid, task_ptr));
        } else {
            task_pool_.AddTask(boost::bind(&TabletImpl::LoadDiskTableInternal, this, tid, pid, table_meta, task_ptr));
            PDLOG(INFO, "load table tid[%u] pid[%u] storage mode[%s]", tid, pid,
                  ::openmldb::common::StorageMode_Name(table_meta.storage_mode()).c_str());
        }

        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

int TabletImpl::LoadTableInternal(uint32_t tid, uint32_t pid, std::shared_ptr<::openmldb::api::TaskInfo> task_ptr) {
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
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            table->SetTableStat(::openmldb::storage::kLoading);
        }
        uint64_t latest_offset = 0;
        uint64_t snapshot_offset = 0;
        std::string db_root_path;
        bool ok = ChooseDBRootPath(tid, pid, table->GetStorageMode(), db_root_path);
        if (!ok) {
            PDLOG(WARNING, "fail to find db root path for table tid %u pid %u storage_mode %s", tid, pid,
                  common::StorageMode_Name(table->GetStorageMode()));
            break;
        }
        std::string binlog_path = GetDBPath(db_root_path, tid, pid) + "/binlog/";
        ::openmldb::storage::Binlog binlog(replicator->GetLogPart(), binlog_path);
        if (snapshot->Recover(table, snapshot_offset) &&
            binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset)) {
            // recover aggregator if exists
            std::string aggr_path = GetDBPath(db_root_path, tid, pid) + "/aggr_info.txt";
            if (::openmldb::base::IsExists(aggr_path)) {
                int fd = open(aggr_path.c_str(), O_RDONLY);
                ::openmldb::api::CreateAggregatorRequest request;
                if (fd < 0) {
                    PDLOG(ERROR, "open file failed: [%s] ", aggr_path.c_str());
                } else {
                    google::protobuf::io::FileInputStream fileInput(fd);
                    fileInput.SetCloseOnDelete(true);
                    if (!google::protobuf::TextFormat::Parse(&fileInput, &request)) {
                        PDLOG(WARNING, "parse create aggregator meta failed");
                    } else {
                        std::string msg;
                        bool ok = CreateAggregatorInternal(&request, msg);
                        if (!ok) {
                            PDLOG(WARNING, "create aggregator failed. msg %s", msg.c_str());
                        }
                    }
                }
            } else {
                // init aggregator related to base table if need.
                auto aggrs = GetAggregators(tid, pid);
                if (aggrs != nullptr) {
                    for (auto& aggr : *aggrs) {
                        if (!aggr->Init(replicator)) {
                            PDLOG(WARNING, "aggregator init failed");
                        }
                    }
                }
            }

            table->SetTableStat(::openmldb::storage::kNormal);
            replicator->SetOffset(latest_offset);
            replicator->SetSnapshotLogPartIndex(snapshot->GetOffset());
            replicator->StartSyncing();
            table->SchedGc();
            gc_pool_.DelayTask(FLAGS_gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid, false));
            io_pool_.DelayTask(FLAGS_binlog_sync_to_disk_interval,
                               boost::bind(&TabletImpl::SchedSyncDisk, this, tid, pid));
            task_pool_.DelayTask(FLAGS_binlog_delete_interval,
                                 boost::bind(&TabletImpl::SchedDelBinlog, this, tid, pid));
            PDLOG(INFO, "load table success. tid %u pid %u", tid, pid);
            if (task_ptr) {
                std::lock_guard<std::mutex> lock(mu_);
                task_ptr->set_status(::openmldb::api::TaskStatus::kDone);
                return 0;
            }
        } else {
            DeleteTableInternal(tid, pid, std::shared_ptr<::openmldb::api::TaskInfo>());
        }
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
    return -1;
}

int TabletImpl::LoadDiskTableInternal(uint32_t tid, uint32_t pid, const ::openmldb::api::TableMeta& table_meta,
                                      std::shared_ptr<::openmldb::api::TaskInfo> task_ptr) {
    do {
        std::string db_root_path;
        bool ok = ChooseDBRootPath(tid, pid, table_meta.storage_mode(), db_root_path);
        if (!ok) {
            PDLOG(WARNING, "fail to find db root path for table tid %u pid %u storage_mode %s", tid, pid,
                  common::StorageMode_Name(table_meta.storage_mode()));
            break;
        }
        std::string table_path = db_root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid);
        std::string snapshot_path = table_path + "/snapshot/";
        ::openmldb::api::Manifest manifest;
        uint64_t snapshot_offset = 0;
        std::string data_path = table_path + "/data";
        if (::openmldb::base::IsExists(data_path)) {
            std::string old_data_path = table_path + "/old_data";
            PDLOG(INFO, "rename dir %s to %s. tid %u pid %u", data_path.c_str(), old_data_path.c_str(), tid, pid);
            if (!::openmldb::base::Rename(data_path, old_data_path)) {
                PDLOG(WARNING, "rename dir failed. tid %u pid %u path %s", tid, pid, data_path.c_str());
                break;
            }
        }
        std::string manifest_file = snapshot_path + "MANIFEST";
        if (Snapshot::GetLocalManifest(manifest_file, manifest) == 0) {
            std::string snapshot_dir = snapshot_path + manifest.name();
            if (::openmldb::base::IsExists(snapshot_dir)) {
                PDLOG(INFO, "hardlink dir %s to %s (tid %u pid %u)", snapshot_dir.c_str(), data_path.c_str(), tid, pid);
                if (::openmldb::base::HardLinkDir(snapshot_dir, data_path)) {
                    PDLOG(WARNING, "hardlink snapshot dir %s to data dir failed (tid %u pid %u)", snapshot_dir.c_str(),
                          tid, pid);
                    break;
                }
                snapshot_offset = manifest.offset();
            } else {
                PDLOG(WARNING, "snapshot_dir %s with tid %u pid %u not exists", snapshot_dir.c_str(), tid, pid);
            }
        }
        std::string msg;
        if (CreateTableInternal(&table_meta, msg) < 0) {
            PDLOG(WARNING, "create table failed. tid %u pid %u msg %s", tid, pid, msg.c_str());
            break;
        }
        // load snapshot data
        std::shared_ptr<Table> table = GetTable(tid, pid);
        if (!table) {
            PDLOG(WARNING, "table with tid %u and pid %u does not exist", tid, pid);
            break;
        }
        DiskTable* disk_table = dynamic_cast<DiskTable*>(table.get());
        if (disk_table == nullptr) {
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
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            table->SetTableStat(::openmldb::storage::kLoading);
        }
        uint64_t latest_offset = 0;
        std::string binlog_path = table_path + "/binlog/";
        ::openmldb::storage::Binlog binlog(replicator->GetLogPart(), binlog_path);
        if (binlog.RecoverFromBinlog(table, snapshot_offset, latest_offset)) {
            table->SetTableStat(::openmldb::storage::kNormal);
            replicator->SetOffset(latest_offset);
            replicator->SetSnapshotLogPartIndex(snapshot->GetOffset());
            replicator->StartSyncing();
            disk_table->SetOffset(latest_offset);
            table->SchedGc();
            gc_pool_.DelayTask(FLAGS_disk_gc_interval * 60 * 1000,
                               boost::bind(&TabletImpl::GcTable, this, tid, pid, false));
            io_pool_.DelayTask(FLAGS_binlog_sync_to_disk_interval,
                               boost::bind(&TabletImpl::SchedSyncDisk, this, tid, pid));
            task_pool_.DelayTask(FLAGS_binlog_delete_interval,
                                 boost::bind(&TabletImpl::SchedDelBinlog, this, tid, pid));
            PDLOG(INFO, "load table success. tid %u pid %u", tid, pid);
            std::string old_data_path = table_path + "/old_data";
            if (::openmldb::base::IsExists(old_data_path)) {
                if (!::openmldb::base::RemoveDir(old_data_path)) {
                    PDLOG(WARNING, "remove dir failed. tid %u pid %u path %s", tid, pid, old_data_path.c_str());
                    break;
                }
            }
            if (task_ptr) {
                std::lock_guard<std::mutex> lock(mu_);
                task_ptr->set_status(::openmldb::api::TaskStatus::kDone);
                return 0;
            }
            PDLOG(INFO, "Recover table with tid %u and pid %u from binlog offset %u to %u", tid, pid, snapshot_offset,
                  latest_offset);
        } else {
            DeleteTableInternal(tid, pid, std::shared_ptr<::openmldb::api::TaskInfo>());
        }
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
    return -1;
}

int32_t TabletImpl::DeleteTableInternal(uint32_t tid, uint32_t pid,
                                        std::shared_ptr<::openmldb::api::TaskInfo> task_ptr) {
    std::string root_path;
    std::string recycle_bin_root_path;
    int32_t code = -1;
    do {
        std::shared_ptr<Table> table = GetTable(tid, pid);
        if (!table) {
            PDLOG(WARNING, "table does not exist. tid %u pid %u", tid, pid);
            break;
        }

        bool ok = ChooseDBRootPath(tid, pid, table->GetStorageMode(), root_path);
        if (!ok) {
            PDLOG(WARNING, "fail to get db root path. tid %u pid %u", tid, pid);
            break;
        }
        ok = ChooseRecycleBinRootPath(tid, pid, table->GetStorageMode(), recycle_bin_root_path);
        if (!ok) {
            PDLOG(WARNING, "fail to get recycle bin root path. tid %u pid %u", tid, pid);
            break;
        }
        std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
        {
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            tables_[tid].erase(pid);
            replicators_[tid].erase(pid);
            snapshots_[tid].erase(pid);
            if (tables_[tid].empty()) {
                tables_.erase(tid);
            }
            if (replicators_[tid].empty()) {
                replicators_.erase(tid);
            }
            if (snapshots_[tid].empty()) {
                snapshots_.erase(tid);
            }
        }
        engine_->ClearCacheLocked("");
        if (replicator) {
            replicator->DelAllReplicateNode();
            PDLOG(INFO, "drop replicator for tid %u, pid %u", tid, pid);
        }
        if (!table->GetDB().empty()) {
            catalog_->DeleteTable(table->GetDB(), table->GetName(), tid, pid);
        }
        // delete related aggregator
        uint32_t base_tid = table->GetTableMeta()->base_table_tid();
        if (base_tid > 0) {
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            uint64_t uid = (uint64_t)base_tid << 32 | pid;
            if (auto it = aggregators_.find(uid); it != aggregators_.end()) {
                auto& aggrs = *it->second;
                for (auto it = aggrs.begin(); it != aggrs.end(); it++) {
                    if ((*it)->GetAggrTid() == tid) {
                        aggrs.erase(it);
                        break;
                    }
                }
            }
        }
        // bulk load data receiver should be destroyed too, and can't do table and data receiver destroy at the same
        // time. So keep data receiver destroy before table destroy.
        bulk_load_mgr_.RemoveReceiver(tid, pid);
        code = 0;
    } while (false);
    if (code < 0) {
        if (task_ptr) {
            std::lock_guard<std::mutex> lock(mu_);
            task_ptr->set_status(::openmldb::api::TaskStatus::kFailed);
        }
        return code;
    }

    std::string source_path = GetDBPath(root_path, tid, pid);
    if (!::openmldb::base::IsExists(source_path)) {
        if (task_ptr) {
            std::lock_guard<std::mutex> lock(mu_);
            task_ptr->set_status(::openmldb::api::TaskStatus::kDone);
        }
        PDLOG(INFO, "drop table ok. tid[%u] pid[%u]", tid, pid);
        return 0;
    }

    if (FLAGS_recycle_bin_enabled) {
        std::string recycle_path = recycle_bin_root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid) + "_" +
                                   ::openmldb::base::GetNowTime();
        ::openmldb::base::Rename(source_path, recycle_path);
    } else {
        ::openmldb::base::RemoveDirRecursive(source_path);
    }

    if (task_ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        task_ptr->set_status(::openmldb::api::TaskStatus::kDone);
    }

    PDLOG(INFO, "drop table ok. tid[%u] pid[%u]", tid, pid);
    return 0;
}

void TabletImpl::CreateTable(RpcController* controller, const ::openmldb::api::CreateTableRequest* request,
                             ::openmldb::api::CreateTableResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const ::openmldb::api::TableMeta* table_meta = &request->table_meta();
    std::string msg;
    uint32_t tid = table_meta->tid();
    uint32_t pid = table_meta->pid();
    if (CheckTableMeta(table_meta, msg) != 0) {
        response->set_code(::openmldb::base::ReturnCode::kTableMetaIsIllegal);
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
        response->set_code(::openmldb::base::ReturnCode::kTableAlreadyExists);
        response->set_msg("table already exists");
        return;
    }
    std::string name = table_meta->name();
    PDLOG(INFO, "start creating table tid[%u] pid[%u] with mode %s", tid, pid,
          ::openmldb::api::TableMode_Name(request->table_meta().mode()).c_str());
    std::string db_root_path;
    bool ok = ChooseDBRootPath(tid, pid, table_meta->storage_mode(), db_root_path);
    if (!ok) {
        PDLOG(WARNING, "fail to find db root path tid[%u] pid[%u] storage_mode[%s]", tid, pid,
              common::StorageMode_Name(table_meta->storage_mode()));
        response->set_code(::openmldb::base::ReturnCode::kFailToGetDbRootPath);
        response->set_msg("fail to find db root path");
        return;
    }
    std::string table_db_path = GetDBPath(db_root_path, tid, pid);

    if (WriteTableMeta(table_db_path, table_meta) < 0) {
        PDLOG(WARNING, "write table_meta failed. tid[%u] pid[%u]", tid, pid);
        response->set_code(::openmldb::base::ReturnCode::kWriteDataFailed);
        response->set_msg("write data failed");
        return;
    }
    if (CreateTableInternal(table_meta, msg) < 0) {
        response->set_code(::openmldb::base::ReturnCode::kCreateTableFailed);
        response->set_msg(msg.c_str());
        return;
    }
    table = GetTable(tid, pid);
    if (!table) {
        response->set_code(::openmldb::base::ReturnCode::kCreateTableFailed);
        response->set_msg("table does not exist");
        PDLOG(WARNING, "table with tid %u and pid %u does not exist", tid, pid);
        return;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        response->set_code(::openmldb::base::ReturnCode::kCreateTableFailed);
        response->set_msg("replicator does not exist");
        PDLOG(WARNING, "replicator with tid %u and pid %u does not exist", tid, pid);
        return;
    }

    table->SetTableStat(::openmldb::storage::kNormal);
    replicator->StartSyncing();
    io_pool_.DelayTask(FLAGS_binlog_sync_to_disk_interval, boost::bind(&TabletImpl::SchedSyncDisk, this, tid, pid));
    task_pool_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&TabletImpl::SchedDelBinlog, this, tid, pid));
    PDLOG(INFO, "create table with id %u pid %u name %s", tid, pid, name.c_str());

    int gc_interval = table->GetStorageMode() == common::kMemory ? FLAGS_gc_interval : FLAGS_disk_gc_interval;
    gc_pool_.DelayTask(gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid, false));
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void TabletImpl::TruncateTable(RpcController* controller, const ::openmldb::api::TruncateTableRequest* request,
        ::openmldb::api::TruncateTableResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    if (auto status = TruncateTableInternal(tid, pid); !status.OK()) {
        base::SetResponseStatus(status, response);
        return;
    }
    auto aggrs = GetAggregators(tid, pid);
    if (aggrs) {
        for (const auto& aggr : *aggrs) {
            auto agg_table = aggr->GetAggTable();
            if (!agg_table) {
                PDLOG(WARNING, "aggrate table does not exist. tid[%u] pid[%u] index pos[%u]",
                        tid, pid, aggr->GetIndexPos());
                response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
                response->set_msg("aggrate table does not exist");
                return;
            }
            uint32_t agg_tid = agg_table->GetId();
            uint32_t agg_pid = agg_table->GetPid();
            if (auto status = TruncateTableInternal(agg_tid, agg_pid); !status.OK()) {
                PDLOG(WARNING, "truncate aggrate table failed. tid[%u] pid[%u] index pos[%u]",
                        agg_tid, agg_pid, aggr->GetIndexPos());
                base::SetResponseStatus(status, response);
                return;
            }
            PDLOG(INFO, "truncate aggrate table success. tid[%u] pid[%u] index pos[%u]",
                        agg_tid, agg_pid, aggr->GetIndexPos());
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

base::Status TabletImpl::TruncateTableInternal(uint32_t tid, uint32_t pid) {
    std::shared_ptr<Table> table;
    std::shared_ptr<Snapshot> snapshot;
    std::shared_ptr<LogReplicator> replicator;
    {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        table = GetTableUnLock(tid, pid);
        if (!table) {
            DEBUGLOG("table does not exist. tid %u pid %u", tid, pid);
            return {::openmldb::base::ReturnCode::kTableIsNotExist, "table not found"};
        }
        snapshot = GetSnapshotUnLock(tid, pid);
        if (!snapshot) {
            PDLOG(WARNING, "snapshot does not exist. tid[%u] pid[%u]", tid, pid);
            return {::openmldb::base::ReturnCode::kSnapshotIsNotExist, "snapshot not found"};
        }
        replicator = GetReplicatorUnLock(tid, pid);
        if (!replicator) {
            PDLOG(WARNING, "replicator does not exist. tid[%u] pid[%u]", tid, pid);
            return {::openmldb::base::ReturnCode::kReplicatorIsNotExist, "replicator not found"};
        }
    }
    if (replicator->GetOffset() == 0) {
        PDLOG(INFO, "table is empty, truncate success. tid[%u] pid[%u]", tid, pid);
        return {};
    }
    if (table->GetTableStat() == ::openmldb::storage::kMakingSnapshot) {
        PDLOG(WARNING, "making snapshot task is running now. tid[%u] pid[%u]", tid, pid);
        return {::openmldb::base::ReturnCode::kTableStatusIsKmakingsnapshot, "table status is kMakingSnapshot"};
    } else if (table->GetTableStat() == ::openmldb::storage::kLoading) {
        PDLOG(WARNING, "table is loading now. tid[%u] pid[%u]", tid, pid);
        return {::openmldb::base::ReturnCode::kTableIsLoading, "table is loading data"};
    }
    if (table->GetStorageMode() == openmldb::common::kMemory) {
        auto table_meta = table->GetTableMeta();
        std::shared_ptr<Table> new_table;
        new_table = std::make_shared<MemTable>(*table_meta);
        if (!new_table->Init()) {
            PDLOG(WARNING, "fail to init table. tid %u, pid %u", tid, pid);
            return {::openmldb::base::ReturnCode::kTableMetaIsIllegal, "fail to init table"};
        }
        new_table->SetTableStat(::openmldb::storage::kNormal);
        if (table_meta->mode() == ::openmldb::api::TableMode::kTableLeader) {
            if (catalog_->AddTable(*table_meta, new_table)) {
                LOG(INFO) << "add table " << table_meta->name() << " to catalog with db " << table_meta->db();
            } else {
                LOG(WARNING) << "fail to add table " << table_meta->name()
                    << " to catalog with db " << table_meta->db();
                return {::openmldb::base::ReturnCode::kCatalogUpdateFailed, "fail to update catalog"};
            }
        }
        {
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            tables_[tid].insert_or_assign(pid, new_table);
        }
        auto mem_snapshot = std::dynamic_pointer_cast<storage::MemTableSnapshot>(snapshot);
        mem_snapshot->Truncate(replicator->GetOffset(), replicator->GetLeaderTerm());
        // running ResetTable after this function return
        task_pool_.DelayTask(10, boost::bind(&TabletImpl::ResetTable, this, table));
    } else {
        auto disk_table = std::dynamic_pointer_cast<DiskTable>(table);
        if (auto status = disk_table->Truncate(); !status.OK()) {
            return {::openmldb::base::ReturnCode::kTruncateTableFailed, status.GetMsg()};
        }
        snapshot_pool_.AddTask(boost::bind(&TabletImpl::MakeSnapshotInternal, this, tid, pid, 0, nullptr, true));
    }
    PDLOG(INFO, "truncate table success. tid[%u] pid[%u]", tid, pid);
    return {};
}

void TabletImpl::ExecuteGc(RpcController* controller, const ::openmldb::api::ExecuteGcRequest* request,
                           ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        DEBUGLOG("table does not exist. tid %u pid %u", tid, pid);
        response->set_code(-1);
        response->set_msg("table not found");
        return;
    }
    gc_pool_.AddTask(boost::bind(&TabletImpl::GcTable, this, tid, pid, true));
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
    PDLOG(INFO, "ExecuteGc. tid %u pid %u", tid, pid);
}

void TabletImpl::GetTableFollower(RpcController* controller, const ::openmldb::api::GetTableFollowerRequest* request,
                                  ::openmldb::api::GetTableFollowerResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        DEBUGLOG("table does not exist. tid %u pid %u", tid, pid);
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    if (!table->IsLeader()) {
        DEBUGLOG("table is follower. tid %u, pid %u", tid, pid);
        response->set_msg("table is follower");
        response->set_code(::openmldb::base::ReturnCode::kTableIsFollower);
        return;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        DEBUGLOG("replicator does not exist. tid %u pid %u", tid, pid);
        response->set_msg("replicator does not exist");
        response->set_code(::openmldb::base::ReturnCode::kReplicatorIsNotExist);
        return;
    }
    response->set_offset(replicator->GetOffset());
    std::map<std::string, uint64_t> info_map;
    replicator->GetReplicateInfo(info_map);
    if (info_map.empty()) {
        response->set_msg("has no follower");
        response->set_code(::openmldb::base::ReturnCode::kNoFollower);
        return;
    }
    for (const auto& kv : info_map) {
        ::openmldb::api::FollowerInfo* follower_info = response->add_follower_info();
        follower_info->set_endpoint(kv.first);
        follower_info->set_offset(kv.second);
    }
    response->set_msg("ok");
    response->set_code(::openmldb::base::ReturnCode::kOk);
}

int32_t TabletImpl::GetSnapshotOffset(uint32_t tid, uint32_t pid, openmldb::common::StorageMode storageMode,
                                      std::string& msg, uint64_t& term, uint64_t& offset) {
    std::string db_root_path;
    bool ok = ChooseDBRootPath(tid, pid, storageMode, db_root_path);
    if (!ok) {
        msg = "fail to get db root path";
        PDLOG(WARNING, "fail to get table db root path");
        return 138;
    }
    std::string db_path = GetDBPath(db_root_path, tid, pid);
    std::string manifest_file = db_path + "/snapshot/MANIFEST";
    int fd = open(manifest_file.c_str(), O_RDONLY);
    if (fd < 0) {
        PDLOG(WARNING, "[%s] does not exist", manifest_file.c_str());
        return 0;
    }
    google::protobuf::io::FileInputStream fileInput(fd);
    fileInput.SetCloseOnDelete(true);
    ::openmldb::api::Manifest manifest;
    if (!google::protobuf::TextFormat::Parse(&fileInput, &manifest)) {
        PDLOG(WARNING, "parse manifest failed");
        return 0;
    }
    std::string snapshot_file = db_path + "/snapshot/" + manifest.name();
    if (!::openmldb::base::IsExists(snapshot_file)) {
        PDLOG(WARNING, "snapshot file[%s] does not exist", snapshot_file.c_str());
        return 0;
    }
    offset = manifest.offset();
    term = manifest.term();
    return 0;
}
void TabletImpl::GetAllSnapshotOffset(RpcController* controller, const ::openmldb::api::EmptyRequest* request,
                                      ::openmldb::api::TableSnapshotOffsetResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::map<uint32_t, openmldb::common::StorageMode> table_sm;
    std::map<uint32_t, std::vector<uint32_t>> tid_pid;
    {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        for (auto table_iter = tables_.begin(); table_iter != tables_.end(); table_iter++) {
            if (table_iter->second.empty()) {
                continue;
            }
            uint32_t tid = table_iter->first;
            std::vector<uint32_t> pids;
            auto part_iter = table_iter->second.begin();
            openmldb::common::StorageMode sm = part_iter->second->GetStorageMode();
            for (; part_iter != table_iter->second.end(); part_iter++) {
                pids.push_back(part_iter->first);
            }
            table_sm.insert(std::make_pair(tid, sm));
            tid_pid.insert(std::make_pair(tid, pids));
        }
    }
    std::string msg;
    for (auto iter = tid_pid.begin(); iter != tid_pid.end(); iter++) {
        uint32_t tid = iter->first;
        auto table = response->add_tables();
        table->set_tid(tid);
        for (auto pid : iter->second) {
            uint64_t term = 0, offset = 0;
            openmldb::common::StorageMode sm = table_sm.find(tid)->second;
            int32_t code = GetSnapshotOffset(tid, pid, sm, msg, term, offset);
            if (code != 0) {
                continue;
            }
            auto partition = table->add_parts();
            partition->set_offset(offset);
            partition->set_pid(pid);
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
}

void TabletImpl::GetCatalog(RpcController* controller, const ::openmldb::api::GetCatalogRequest* request,
                            ::openmldb::api::GetCatalogResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (catalog_) {
        ::openmldb::common::CatalogInfo* catalog_info = response->mutable_catalog();
        catalog_info->set_version(catalog_->GetVersion());
        catalog_info->set_endpoint(FLAGS_endpoint);
        response->set_code(0);
        response->set_msg("ok");
        return;
    }
    response->set_code(-1);
    response->set_msg("catalog does not exist");
    PDLOG(WARNING, "catalog does not exist");
}

void TabletImpl::GetTermPair(RpcController* controller, const ::openmldb::api::GetTermPairRequest* request,
                             ::openmldb::api::GetTermPairResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (zk_cluster_.empty()) {
        response->set_code(-1);
        response->set_msg("tablet is not run in cluster mode");
        PDLOG(WARNING, "tablet is not run in cluster mode");
        return;
    }
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    ::openmldb::common::StorageMode mode = ::openmldb::common::kMemory;
    if (request->has_storage_mode()) {
        mode = request->storage_mode();
    }
    if (!table) {
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_has_table(false);
        response->set_msg("table does not exist");
        std::string msg;
        uint64_t term = 0, offset = 0;
        int32_t code = GetSnapshotOffset(tid, pid, mode, msg, term, offset);
        response->set_code(code);
        if (code == 0) {
            response->set_term(term);
            response->set_offset(offset);
        } else {
            response->set_msg(msg);
        }
        return;
    }
    std::shared_ptr<LogReplicator> replicator = GetReplicator(tid, pid);
    if (!replicator) {
        response->set_code(::openmldb::base::ReturnCode::kReplicatorIsNotExist);
        response->set_msg("replicator does not exist");
        return;
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
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

void TabletImpl::DeleteBinlog(RpcController* controller, const ::openmldb::api::GeneralRequest* request,
                              ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::string db_root_path;
    ::openmldb::common::StorageMode mode = ::openmldb::common::kMemory;
    if (request->has_storage_mode()) {
        mode = request->storage_mode();
    }
    bool ok = ChooseDBRootPath(tid, pid, mode, db_root_path);
    if (!ok) {
        response->set_code(::openmldb::base::ReturnCode::kFailToGetDbRootPath);
        response->set_msg("fail to get db root path");
        PDLOG(WARNING, "fail to get table db root path");
        return;
    }
    std::string db_path = GetDBPath(db_root_path, tid, pid);
    std::string binlog_path = db_path + "/binlog";
    if (::openmldb::base::IsExists(binlog_path)) {
        if (FLAGS_recycle_bin_enabled) {
            std::string recycle_bin_root_path;
            ok = ChooseRecycleBinRootPath(tid, pid, mode, recycle_bin_root_path);
            if (!ok) {
                response->set_code(::openmldb::base::ReturnCode::kFailToGetRecycleRootPath);
                response->set_msg("fail to get recycle root path");
                PDLOG(WARNING, "fail to get table recycle root path");
                return;
            }
            std::string recycle_path = recycle_bin_root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid) +
                                       "_binlog_" + ::openmldb::base::GetNowTime();
            ::openmldb::base::Rename(binlog_path, recycle_path);
            PDLOG(INFO, "binlog has moved form %s to %s. tid %u pid %u", binlog_path.c_str(), recycle_path.c_str(), tid,
                  pid);
        } else {
            ::openmldb::base::RemoveDirRecursive(binlog_path);
            PDLOG(INFO, "binlog %s has removed. tid %u pid %u", binlog_path.c_str(), tid, pid);
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void TabletImpl::CheckFile(RpcController* controller, const ::openmldb::api::CheckFileRequest* request,
                           ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::string db_root_path;
    ::openmldb::common::StorageMode mode = ::openmldb::common::kMemory;
    if (request->has_storage_mode()) {
        mode = request->storage_mode();
    }
    bool ok = ChooseDBRootPath(tid, pid, mode, db_root_path);
    if (!ok) {
        response->set_code(::openmldb::base::ReturnCode::kFailToGetDbRootPath);
        response->set_msg("fail to get db root path");
        PDLOG(WARNING, "fail to get table db root path");
        return;
    }
    std::string file_name = request->file();
    std::string full_path = GetDBPath(db_root_path, tid, pid) + "/";
    if (request->has_dir_name() && request->dir_name().size() > 0) {
        if (request->dir_name() != "index") {
            full_path.append("snapshot/");
        }
        full_path.append(request->dir_name() + "/");
    } else if (file_name != "table_meta.txt") {
        full_path.append("snapshot/");
    }
    full_path += file_name;
    uint64_t size = 0;
    if (!::openmldb::base::GetFileSize(full_path, size)) {
        response->set_code(-1);
        response->set_msg("get size failed");
        PDLOG(WARNING, "get size failed. file[%s]", full_path.c_str());
        return;
    }
    if (size != request->size()) {
        response->set_code(-1);
        response->set_msg("check size failed");
        PDLOG(WARNING, "check size failed. file[%s] cur_size[%lu] expect_size[%lu]", full_path.c_str(), size,
              request->size());
        return;
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void TabletImpl::GetManifest(RpcController* controller, const ::openmldb::api::GetManifestRequest* request,
                             ::openmldb::api::GetManifestResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::string db_root_path;
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    ::openmldb::common::StorageMode mode = ::openmldb::common::kMemory;
    if (request->has_storage_mode()) {
        mode = request->storage_mode();
    }
    bool ok = ChooseDBRootPath(tid, pid, mode, db_root_path);
    if (!ok) {
        response->set_code(::openmldb::base::ReturnCode::kFailToGetDbRootPath);
        response->set_msg("fail to get db root path");
        PDLOG(WARNING, "fail to get table db root path");
        return;
    }
    std::string db_path = GetDBPath(db_root_path, tid, pid);
    std::string manifest_file = db_path + "/snapshot/MANIFEST";
    ::openmldb::api::Manifest manifest;
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
        PDLOG(INFO, "[%s] does not exist", manifest_file.c_str());
        manifest.set_offset(0);
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
    ::openmldb::api::Manifest* manifest_r = response->mutable_manifest();
    manifest_r->CopyFrom(manifest);
}

int TabletImpl::WriteTableMeta(const std::string& path, const ::openmldb::api::TableMeta* table_meta) {
    if (!::openmldb::base::MkdirRecur(path)) {
        PDLOG(WARNING, "fail to create path %s", path.c_str());
        return -1;
    }
    std::string full_path = path + "/table_meta.txt";
    std::string table_meta_info;
    google::protobuf::TextFormat::PrintToString(*table_meta, &table_meta_info);
    FILE* fd_write = fopen(full_path.c_str(), "w");
    if (fd_write == nullptr) {
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

int TabletImpl::UpdateTableMeta(const std::string& path, ::openmldb::api::TableMeta* table_meta, bool for_add_column) {
    std::string full_path = path + "/table_meta.txt";
    int fd = open(full_path.c_str(), O_RDONLY);
    ::openmldb::api::TableMeta old_meta;
    if (fd < 0) {
        PDLOG(WARNING, "[%s] does not exist", "table_meta.txt");
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
    if (!for_add_column) {
        old_meta.clear_replicas();
        old_meta.MergeFrom(*table_meta);
        table_meta->CopyFrom(old_meta);
    }
    std::string new_name = full_path + "." + ::openmldb::base::GetNowTime();
    rename(full_path.c_str(), new_name.c_str());
    return 0;
}

int TabletImpl::UpdateTableMeta(const std::string& path, ::openmldb::api::TableMeta* table_meta) {
    return UpdateTableMeta(path, table_meta, false);
}

int TabletImpl::CreateTableInternal(const ::openmldb::api::TableMeta* table_meta, std::string& msg) {
    uint32_t tid = table_meta->tid();
    uint32_t pid = table_meta->pid();
    std::map<std::string, std::string> real_ep_map;
    for (int32_t i = 0; i < table_meta->replicas_size(); i++) {
        real_ep_map.insert(std::make_pair(table_meta->replicas(i), ""));
    }
    if (FLAGS_use_name) {
        if (!GetRealEp(tid, pid, &real_ep_map)) {
            msg.assign("name not found in real_ep_map");
            return -1;
        }
    }
    std::string db_root_path;
    bool ok = ChooseDBRootPath(tid, pid, table_meta->storage_mode(), db_root_path);
    if (!ok) {
        PDLOG(WARNING, "fail to get table db root path");
        msg.assign("fail to get table db root path");
        return -1;
    }
    std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
    std::shared_ptr<Table> table = GetTableUnLock(tid, pid);
    if (table) {
        PDLOG(WARNING, "table with tid[%u] and pid[%u] exists", tid, pid);
        msg.assign("table exists");
        return -1;
    }
    std::string table_db_path = GetDBPath(db_root_path, tid, pid);
    if (table_meta->storage_mode() == openmldb::common::kMemory) {
        table = std::make_shared<MemTable>(*table_meta);
    } else {
        table = std::make_shared<DiskTable>(*table_meta, table_db_path);
    }

    if (!table->Init()) {
        PDLOG(WARNING, "fail to init table. tid %u, pid %u", table_meta->tid(), table_meta->pid());
        msg.assign("fail to init table");
        return -1;
    }
    PDLOG(INFO, "create table. tid %u pid %u", tid, pid);

    std::shared_ptr<LogReplicator> replicator;
    if (table->IsLeader()) {
        replicator = std::make_shared<LogReplicator>(tid, pid, table_db_path, real_ep_map, ReplicatorRole::kLeaderNode);
    } else {
        replicator = std::make_shared<LogReplicator>(tid, pid, table_db_path, std::map<std::string, std::string>(),
                                                     ReplicatorRole::kFollowerNode);
    }
    if (!replicator) {
        PDLOG(WARNING, "fail to create replicator for table tid %u, pid %u", tid, pid);
        msg.assign("fail create replicator for table");
        return -1;
    }
    ok = replicator->Init();
    if (!ok) {
        PDLOG(WARNING, "fail to init replicator for table tid %u, pid %u", tid, pid);
        // clean memory
        msg.assign("fail init replicator for table");
        return -1;
    }
    if (!zk_cluster_.empty() && table_meta->mode() == ::openmldb::api::TableMode::kTableLeader) {
        replicator->SetLeaderTerm(table_meta->term());
    }

    ::openmldb::storage::Snapshot* snapshot_ptr = nullptr;
    if (table_meta->storage_mode() == openmldb::common::StorageMode::kMemory) {
        snapshot_ptr = new ::openmldb::storage::MemTableSnapshot(tid, pid, replicator->GetLogPart(), db_root_path);
    } else {
        snapshot_ptr = new ::openmldb::storage::DiskTableSnapshot(table_meta->tid(), table_meta->pid(), db_root_path);
    }
    if (!snapshot_ptr->Init()) {
        PDLOG(WARNING, "fail to init snapshot for tid %u, pid %u", table_meta->tid(), table_meta->pid());
        msg.assign("fail to init snapshot");
        return -1;
    }

    std::shared_ptr<Snapshot> snapshot(snapshot_ptr);
    tables_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), table));
    snapshots_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), snapshot));
    replicators_[table_meta->tid()].insert(std::make_pair(table_meta->pid(), replicator));
    if (!table_meta->db().empty() && table_meta->mode() == ::openmldb::api::TableMode::kTableLeader) {
        if (catalog_->AddTable(*table_meta, table)) {
            LOG(INFO) << "add table " << table_meta->name() << " to catalog with db " << table_meta->db();
        } else {
            LOG(WARNING) << "fail to add table " << table_meta->name() << " to catalog with db " << table_meta->db();
        }
        engine_->ClearCacheLocked("");

        // we always refresh the aggr catalog in case zk notification arrives later than the `deploy` sql
        if (boost::iequals(table_meta->db(), openmldb::nameserver::PRE_AGG_DB)) {
            RefreshAggrCatalog();
        }
    }
    return 0;
}

void TabletImpl::DropTable(RpcController* controller, const ::openmldb::api::DropTableRequest* request,
                           ::openmldb::api::DropTableResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kDropTable, task_ptr) < 0) {
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
            response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
            response->set_msg("table does not exist");
            PDLOG(WARNING, "table does not exist. tid[%u] pid[%u]", tid, pid);
            break;
        } else {
            if (table->GetTableStat() == ::openmldb::storage::kMakingSnapshot) {
                PDLOG(WARNING, "making snapshot task is running now. tid[%u] pid[%u]", tid, pid);
                response->set_code(::openmldb::base::ReturnCode::kTableStatusIsKmakingsnapshot);
                response->set_msg("table status is kMakingSnapshot");
                break;
            }
        }
        task_pool_.AddTask(boost::bind(&TabletImpl::DeleteTableInternal, this, tid, pid, task_ptr));
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

void TabletImpl::GetTaskStatus(RpcController* controller, const ::openmldb::api::TaskStatusRequest* request,
                               ::openmldb::api::TaskStatusResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : task_map_) {
        for (const auto& task_info : kv.second) {
            ::openmldb::api::TaskInfo* task = response->add_task();
            task->CopyFrom(*task_info);
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void TabletImpl::DeleteOPTask(RpcController* controller, const ::openmldb::api::DeleteTaskRequest* request,
                              ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    for (int idx = 0; idx < request->op_id_size(); idx++) {
        auto iter = task_map_.find(request->op_id(idx));
        if (iter == task_map_.end()) {
            continue;
        }
        if (!iter->second.empty()) {
            PDLOG(INFO, "delete op task. op_id[%lu] op_type[%s] task_num[%u]", request->op_id(idx),
                  ::openmldb::api::OPType_Name(iter->second.front()->op_type()).c_str(), iter->second.size());
            iter->second.clear();
        }
        task_map_.erase(iter);
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void TabletImpl::ConnectZK(RpcController* controller, const ::openmldb::api::ConnectZKRequest* request,
                           ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (zk_client_ && zk_client_->Reconnect() && zk_client_->Register()) {
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        PDLOG(INFO, "connect zk ok");
        return;
    }
    response->set_code(-1);
    response->set_msg("connect failed");
}

void TabletImpl::DisConnectZK(RpcController* controller, const ::openmldb::api::DisConnectZKRequest* request,
                              ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (zk_client_) {
        zk_client_->CloseZK();
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
    PDLOG(INFO, "disconnect zk ok");
    return;
}

void TabletImpl::SetTaskStatus(std::shared_ptr<::openmldb::api::TaskInfo>& task_ptr,
                               ::openmldb::api::TaskStatus status) {
    if (!task_ptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    task_ptr->set_status(status);
}

int TabletImpl::GetTaskStatus(const std::shared_ptr<::openmldb::api::TaskInfo>& task_ptr,
                              ::openmldb::api::TaskStatus* status) {
    if (!task_ptr) {
        return -1;
    }
    std::lock_guard<std::mutex> lock(mu_);
    *status = task_ptr->status();
    return 0;
}

int TabletImpl::AddOPTask(const ::openmldb::api::TaskInfo& task_info, ::openmldb::api::TaskType task_type,
                          std::shared_ptr<::openmldb::api::TaskInfo>& task_ptr) {
    std::lock_guard<std::mutex> lock(mu_);
    if (IsExistTaskUnLock(task_info)) {
        PDLOG(WARNING, "task is running. op_id[%lu] op_type[%s] task_type[%s]", task_info.op_id(),
              ::openmldb::api::OPType_Name(task_info.op_type()).c_str(),
              ::openmldb::api::TaskType_Name(task_info.task_type()).c_str());
        return -1;
    }
    task_ptr.reset(task_info.New());
    task_ptr->CopyFrom(task_info);
    task_ptr->set_status(::openmldb::api::TaskStatus::kDoing);
    auto iter = task_map_.find(task_info.op_id());
    if (iter == task_map_.end()) {
        iter = task_map_.emplace(task_info.op_id(), std::list<std::shared_ptr<::openmldb::api::TaskInfo>>()).first;
    }
    iter->second.push_back(task_ptr);
    if (task_info.task_type() != task_type) {
        PDLOG(WARNING, "task type is not match. type is[%s] expect type is [%s]",
              ::openmldb::api::TaskType_Name(task_info.task_type()).c_str(),
              ::openmldb::api::TaskType_Name(task_type).c_str());
        task_ptr->set_status(::openmldb::api::TaskStatus::kFailed);
        return -1;
    }
    PDLOG(INFO, "add task map success, op_id %lu op_type %s task_type %s %s", task_info.op_id(),
          ::openmldb::api::OPType_Name(task_info.op_type()).c_str(),
          ::openmldb::api::TaskType_Name(task_info.task_type()).c_str(), nameserver::Task::GetAdditionalMsg(task_info));
    return 0;
}

bool TabletImpl::IsExistTaskUnLock(const ::openmldb::api::TaskInfo& task) {
    auto iter = task_map_.find(task.op_id());
    if (iter == task_map_.end()) {
        return false;
    }
    for (auto& cur_task : iter->second) {
        if (cur_task->op_id() == task.op_id() && cur_task->task_type() == task.task_type()) {
            if (task.has_tid() && (!cur_task->has_tid() || task.tid() != cur_task->tid())) {
                continue;
            }
            if (task.has_pid() && (!cur_task->has_pid() || task.pid() != cur_task->pid())) {
                continue;
            }
            if (task.has_task_id() && (!cur_task->has_task_id() || task.task_id() != cur_task->task_id())) {
                continue;
            }
            return true;
        }
    }
    return false;
}

void TabletImpl::GcTable(uint32_t tid, uint32_t pid, bool execute_once) {
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (table) {
        int32_t gc_interval = table->GetStorageMode() == common::kMemory ? FLAGS_gc_interval : FLAGS_disk_gc_interval;
        table->SchedGc();
        if (!execute_once) {
            gc_pool_.DelayTask(gc_interval * 60 * 1000, boost::bind(&TabletImpl::GcTable, this, tid, pid, false));
        }
        return;
    }
}

std::shared_ptr<Snapshot> TabletImpl::GetSnapshot(uint32_t tid, uint32_t pid) {
    std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
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
    std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
    return GetReplicatorUnLock(tid, pid);
}

std::shared_ptr<Table> TabletImpl::GetTable(uint32_t tid, uint32_t pid) {
    std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
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

std::shared_ptr<Aggrs> TabletImpl::GetAggregators(uint32_t tid, uint32_t pid) {
    std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
    return GetAggregatorsUnLock(tid, pid);
}

std::shared_ptr<Aggrs> TabletImpl::GetAggregatorsUnLock(uint32_t tid, uint32_t pid) {
    uint64_t uid = (uint64_t)tid << 32 | pid;
    auto it = aggregators_.find(uid);
    if (it != aggregators_.end()) {
        return it->second;
    }
    return std::shared_ptr<Aggrs>();
}

bool TabletImpl::UpdateAggrs(uint32_t tid, uint32_t pid, const std::string& value,
                             const ::openmldb::storage::Dimensions& dimensions, uint64_t log_offset) {
    auto aggrs = GetAggregators(tid, pid);
    if (!aggrs) {
        return true;
    }
    for (auto iter = dimensions.begin(); iter != dimensions.end(); ++iter) {
        for (const auto& aggr : *aggrs) {
            if (aggr->GetIndexPos() != iter->idx()) {
                continue;
            }
            auto ok = aggr->Update(iter->key(), value, log_offset);
            if (!ok) {
                PDLOG(WARNING, "update aggr failed. tid[%u] pid[%u] index[%u] key[%s] value[%s]", tid, pid, iter->idx(),
                      iter->key().c_str(), value.c_str());
                return false;
            }
        }
    }
    return true;
}

void TabletImpl::ShowMemPool(RpcController* controller, const ::openmldb::api::HttpRequest* request,
                             ::openmldb::api::HttpResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
#ifdef TCMALLOC_ENABLE
    MallocExtension* tcmalloc = MallocExtension::instance();
    std::string stat;
    stat.resize(1024);
    char* buffer = reinterpret_cast<char*>(&(stat[0]));
    tcmalloc->GetStats(buffer, 1024);
    cntl->response_attachment().append("<html><head><title>Mem Stat</title></head><body><pre>");
    cntl->response_attachment().append(stat);
    cntl->response_attachment().append("</pre></body></html>");
#else
    cntl->response_attachment().append("TCMALLOC_ENABLE is not defined\n");
#endif
}

void TabletImpl::CheckZkClient() {
    if (zk_client_) {
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
        trivial_task_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval, boost::bind(&TabletImpl::CheckZkClient, this));
    }
}

bool TabletImpl::RefreshSingleTable(uint32_t tid) {
    std::string value;
    std::string node = zk_path_ + "/table/db_table_data/" + std::to_string(tid);
    if (zk_client_ && !zk_client_->GetNodeValue(node, value)) {
        LOG(WARNING) << "fail to get table data. node: " << node;
        return false;
    }
    ::openmldb::nameserver::TableInfo table_info;
    if (!table_info.ParseFromString(value)) {
        LOG(WARNING) << "fail to parse table proto. tid: " << tid << " value: " << value;
        return false;
    }
    if (bool index_updated = false; !catalog_->UpdateTableInfo(table_info, &index_updated)) {
        return false;
    } else if (index_updated) {
        engine_->ClearCacheLocked("");
    }
    return true;
}

void TabletImpl::UpdateGlobalVarTable() {
    auto table_handler = catalog_->GetTable(nameserver::INFORMATION_SCHEMA_DB, nameserver::GLOBAL_VARIABLES);
    if (!table_handler) {
        LOG(WARNING) << "fail to get table handler. db " << nameserver::INFORMATION_SCHEMA_DB << " table "
                     << nameserver::GLOBAL_VARIABLES;
        return;
    }
    auto it = table_handler->GetIterator();
    if (!it) {
        LOG(WARNING) << "fail to get full iterator. db " << nameserver::INFORMATION_SCHEMA_DB << " table "
                     << nameserver::GLOBAL_VARIABLES;
        return;
    }
    auto old_global_var = std::atomic_load_explicit(&global_variables_, std::memory_order_relaxed);
    auto new_global_var = std::make_shared<std::map<std::string, std::string>>(*old_global_var);
    std::string key;
    std::string value;
    static ::hybridse::codec::RowView row_view(*(table_handler->GetSchema()));
    it->SeekToFirst();
    while (it->Valid()) {
        auto row = it->GetValue();
        const char* ch = nullptr;
        uint32_t len = 0;
        row_view.GetValue(row.buf(), 0, &ch, &len);
        key.assign(ch, len);
        row_view.GetValue(row.buf(), 1, &ch, &len);
        value.assign(ch, len);
        (*new_global_var)[key] = value;
        it->Next();
    }
    std::atomic_store_explicit(&global_variables_, new_global_var, std::memory_order_relaxed);

    // no DEPLOY_STATS when init, so we can get the first DEPLOY_STATS value here, and all changes will be handled in
    // and we assume that global vars change is low frequency, so we reset here instead of in the repeated task
    if (!IsCollectDeployStatsEnabled()) {
        deploy_collector_->Reset();
    }
    return;
}

void TabletImpl::RefreshTableInfo() {
    if (!zk_client_) {
        return;
    }
    std::string db_table_notify_path = zk_path_ + "/table/notify";
    std::string value;
    if (!zk_client_->GetNodeValue(db_table_notify_path, value)) {
        LOG(WARNING) << "fail to get node value. node is " << db_table_notify_path;
        return;
    }
    uint64_t version = 0;
    try {
        version = std::stoull(value);
    } catch (const std::exception& e) {
        LOG(WARNING) << "value is not integer";
    }
    std::string db_table_data_path = zk_path_ + "/table/db_table_data";
    std::vector<std::string> table_datas;
    if (zk_client_->IsExistNode(db_table_data_path) == 0) {
        bool ok = zk_client_->GetChildren(db_table_data_path, table_datas);
        if (!ok) {
            LOG(WARNING) << "fail to get table list with path " << db_table_data_path;
            return;
        }
    } else {
        LOG(INFO) << "no tables in db";
    }
    std::vector<::openmldb::nameserver::TableInfo> table_info_vec;
    for (const auto& node : table_datas) {
        std::string value;
        if (!zk_client_->GetNodeValue(db_table_data_path + "/" + node, value)) {
            LOG(WARNING) << "fail to get table data. node: " << node;
            continue;
        }
        ::openmldb::nameserver::TableInfo table_info;
        if (!table_info.ParseFromString(value)) {
            LOG(WARNING) << "fail to parse table proto. node: " << node << " value: " << value;
            continue;
        }
        table_info_vec.push_back(std::move(table_info));
    }
    // procedure part
    std::vector<std::string> sp_datas;
    if (zk_client_->IsExistNode(sp_root_path_) == 0) {
        bool ok = zk_client_->GetChildren(sp_root_path_, sp_datas);
        if (!ok) {
            LOG(WARNING) << "fail to get procedure list with path " << sp_root_path_;
            return;
        }
    } else {
        DLOG(INFO) << "no procedures in db";
    }
    openmldb::catalog::Procedures db_sp_map;
    for (const auto& node : sp_datas) {
        if (node.empty()) continue;
        std::string value;
        bool ok = zk_client_->GetNodeValue(sp_root_path_ + "/" + node, value);
        if (!ok) {
            LOG(WARNING) << "fail to get procedure data. node: " << node;
            continue;
        }
        std::string uncompressed;
        ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
        ::openmldb::api::ProcedureInfo sp_info_pb;
        ok = sp_info_pb.ParseFromString(uncompressed);
        if (!ok) {
            LOG(WARNING) << "fail to parse procedure proto. node: " << node << " value: " << value;
            continue;
        }
        auto sp_info = std::make_shared<openmldb::catalog::ProcedureInfoImpl>(sp_info_pb);
        if (!sp_info) {
            LOG(WARNING) << "convert procedure info failed, sp_name: " << sp_info_pb.sp_name()
                         << " db: " << sp_info_pb.db_name();
            continue;
        }
        auto it = db_sp_map.find(sp_info->GetDbName());
        if (it == db_sp_map.end()) {
            std::map<std::string, std::shared_ptr<hybridse::sdk::ProcedureInfo>> sp_in_db = {
                {sp_info->GetSpName(), sp_info}};
            db_sp_map.insert(std::make_pair(sp_info->GetDbName(), sp_in_db));
        } else {
            it->second.insert(std::make_pair(sp_info->GetSpName(), sp_info));
        }
    }
    auto old_db_sp_map = catalog_->GetProcedures();
    bool updated = false;
    catalog_->Refresh(table_info_vec, version, db_sp_map, &updated);
    if (updated) {
        engine_->ClearCacheLocked("");
    }
    // skip exist procedure, don`t need recompile
    for (const auto& db_sp_map_kv : db_sp_map) {
        const auto& db = db_sp_map_kv.first;
        auto old_db_sp_map_it = old_db_sp_map.find(db);
        if (old_db_sp_map_it != old_db_sp_map.end()) {
            auto old_sp_map = old_db_sp_map_it->second;
            for (const auto& sp_map_kv : db_sp_map_kv.second) {
                const auto& sp_name = sp_map_kv.first;
                auto old_sp_map_it = old_sp_map.find(sp_name);
                if (old_sp_map_it != old_sp_map.end()) {
                    continue;
                } else {
                    CreateProcedure(sp_map_kv.second);
                }
            }
        } else {
            for (const auto& sp_map_kv : db_sp_map_kv.second) {
                CreateProcedure(sp_map_kv.second);
            }
        }
    }

    RefreshAggrCatalog();
}

bool TabletImpl::RefreshAggrCatalog() {
    std::string meta_db = nameserver::INTERNAL_DB;
    std::string meta_table = nameserver::PRE_AGG_META_NAME;
    std::shared_ptr<::hybridse::vm::TableHandler> table = catalog_->GetTable(meta_db, meta_table);
    if (!table) {
        PDLOG(WARNING, "%s.%s not found", meta_db, meta_table);
        return false;
    }
    static ::hybridse::codec::RowView row_view(*(table->GetSchema()));
    auto it = table->GetIterator();
    if (!it) {
        PDLOG(WARNING, "fail to get iterator. %s.%s", meta_db, meta_table);
        return false;
    }
    it->SeekToFirst();
    std::vector<::hybridse::vm::AggrTableInfo> table_infos;
    while (it->Valid()) {
        auto row = it->GetValue();
        const char* str = nullptr;
        uint32_t len = 0;
        ::hybridse::vm::AggrTableInfo table_info;
        row_view.GetValue(row.buf(), 0, &str, &len);
        table_info.aggr_table.assign(str, len);
        row_view.GetValue(row.buf(), 1, &str, &len);
        table_info.aggr_db.assign(str, len);
        row_view.GetValue(row.buf(), 2, &str, &len);
        table_info.base_db.assign(str, len);
        row_view.GetValue(row.buf(), 3, &str, &len);
        table_info.base_table.assign(str, len);
        row_view.GetValue(row.buf(), 4, &str, &len);
        table_info.aggr_func.assign(str, len);
        row_view.GetValue(row.buf(), 5, &str, &len);
        table_info.aggr_col.assign(str, len);
        row_view.GetValue(row.buf(), 6, &str, &len);
        table_info.partition_cols.assign(str, len);
        row_view.GetValue(row.buf(), 7, &str, &len);
        table_info.order_by_col.assign(str, len);
        row_view.GetValue(row.buf(), 8, &str, &len);
        table_info.bucket_size.assign(str, len);
        row_view.GetValue(row.buf(), 9, &str, &len);
        table_info.filter_col.assign(str, len);

        table_infos.emplace_back(std::move(table_info));
        it->Next();
    }
    catalog_->RefreshAggrTables(table_infos);
    LOG(INFO) << "Refresh agg catalog (size = " << table_infos.size() << ")";
    return true;
}

int TabletImpl::CheckDimessionPut(const ::openmldb::api::PutRequest* request, uint32_t idx_cnt) {
    for (int32_t i = 0; i < request->dimensions_size(); i++) {
        if (idx_cnt <= request->dimensions(i).idx()) {
            PDLOG(WARNING,
                  "invalid put request dimensions, request idx %u is greater "
                  "than table idx cnt %u",
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
        // TODO(nauta): need better way to handle aggregator volatile status lost.
        bool deleted = false;
        replicator->DeleteBinlog(&deleted);
        if (deleted) {
            auto aggrs = GetAggregators(tid, pid);
            if (aggrs) {
                for (auto& aggr : *aggrs) {
                    aggr->FlushAll();
                }
            }
        }
        task_pool_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&TabletImpl::SchedDelBinlog, this, tid, pid));
    }
}

bool TabletImpl::ChooseDBRootPath(uint32_t tid, uint32_t pid, const ::openmldb::common::StorageMode& mode,
                                  std::string& path) {
    std::vector<std::string>& paths = mode_root_paths_[mode];
    if (paths.size() < 1) {
        return false;
    }

    if (paths.size() == 1) {
        path.assign(paths[0]);
        return path.size();
    }

    std::string key = std::to_string(tid) + std::to_string(pid);
    uint32_t index = ::openmldb::base::hash(key.c_str(), key.size(), SEED) % paths.size();
    path.assign(paths[index]);
    return path.size();
}

bool TabletImpl::ChooseRecycleBinRootPath(uint32_t tid, uint32_t pid, const ::openmldb::common::StorageMode& mode,
                                          std::string& path) {
    std::vector<std::string>& paths = mode_recycle_root_paths_[mode];
    if (paths.size() < 1) return false;

    if (paths.size() == 1) {
        path.assign(paths[0]);
        return true;
    }
    std::string key = std::to_string(tid) + std::to_string(pid);
    uint32_t index = ::openmldb::base::hash(key.c_str(), key.size(), SEED) % paths.size();
    path.assign(paths[index]);
    return true;
}

void TabletImpl::DelRecycle(const std::string& path) {
    std::vector<std::string> file_vec;
    ::openmldb::base::GetChildFileName(path, file_vec);
    for (auto file_path : file_vec) {
        std::string file_name = ::openmldb::base::ParseFileNameFromPath(file_path);
        std::vector<std::string> parts;
        int64_t recycle_time;
        int64_t now_time = ::baidu::common::timer::get_micros() / 1000000;
        ::openmldb::base::SplitString(file_name, "_", parts);
        if (parts.size() == 3) {
            recycle_time = ::openmldb::base::ParseTimeToSecond(parts[2], "%Y%m%d%H%M%S");
        } else {
            recycle_time = ::openmldb::base::ParseTimeToSecond(parts[3], "%Y%m%d%H%M%S");
        }
        if (FLAGS_recycle_ttl != 0 && (now_time - recycle_time) > FLAGS_recycle_ttl * 60) {
            PDLOG(INFO, "delete recycle dir %s", file_path.c_str());
            ::openmldb::base::RemoveDirRecursive(file_path);
        }
    }
}

void TabletImpl::SchedDelRecycle() {
    for (auto kv : mode_recycle_root_paths_) {
        for (auto path : kv.second) {
            DelRecycle(path);
        }
    }
    task_pool_.DelayTask(FLAGS_recycle_ttl * 60 * 1000, boost::bind(&TabletImpl::SchedDelRecycle, this));
}

bool TabletImpl::CreateMultiDir(const std::vector<std::string>& dirs) {
    std::vector<std::string>::const_iterator it = dirs.begin();
    for (; it != dirs.end(); ++it) {
        std::string path = *it;
        bool ok = ::openmldb::base::MkdirRecur(path);
        if (!ok) {
            PDLOG(WARNING, "fail to create dir %s", path.c_str());
            return false;
        }
    }
    return true;
}

bool TabletImpl::ChooseTableRootPath(uint32_t tid, uint32_t pid, const ::openmldb::common::StorageMode& mode,
                                     std::string& path) {
    std::string root_path;
    bool ok = ChooseDBRootPath(tid, pid, mode, root_path);
    if (!ok) {
        PDLOG(WARNING, "table db path doesn't found. tid %u, pid %u", tid, pid);
        return false;
    }
    path = GetDBPath(root_path, tid, pid);
    if (!::openmldb::base::IsExists(path)) {
        PDLOG(WARNING, "table db path doesn`t exist. tid %u, pid %u", tid, pid);
        return false;
    }
    return true;
}

bool TabletImpl::GetTableRootSize(uint32_t tid, uint32_t pid, const ::openmldb::common::StorageMode& mode,
                                  uint64_t& size) {
    std::string table_path;
    if (!ChooseTableRootPath(tid, pid, mode, table_path)) {
        return false;
    }
    if (!::openmldb::base::GetDirSizeRecur(table_path, size)) {
        PDLOG(WARNING, "get table root size failed. tid %u, pid %u", tid, pid);
        return false;
    }
    return true;
}

void TabletImpl::GetDiskused() {
    std::vector<std::shared_ptr<Table>> tables;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto it = tables_.begin(); it != tables_.end(); ++it) {
            for (auto pit = it->second.begin(); pit != it->second.end(); ++pit) {
                tables.push_back(pit->second);
            }
        }
    }
    for (const auto& table : tables) {
        uint64_t size = 0;
        if (!GetTableRootSize(table->GetId(), table->GetPid(), table->GetStorageMode(), size)) {
            PDLOG(WARNING, "get table root size failed. tid[%u] pid[%u]", table->GetId(), table->GetPid());
        } else {
            table->SetDiskused(size);
        }
    }
    task_pool_.DelayTask(FLAGS_get_table_diskused_interval, boost::bind(&TabletImpl::GetDiskused, this));
}

void TabletImpl::GetMemoryStat() {
    auto size = base::GetRSS() >> 20;
    memory_used_.store(size, std::memory_order_relaxed);
    task_pool_.DelayTask(FLAGS_get_memory_stat_interval, boost::bind(&TabletImpl::GetMemoryStat, this));
}

void TabletImpl::SetMode(RpcController* controller, const ::openmldb::api::SetModeRequest* request,
                         ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    follower_.store(request->follower(), std::memory_order_relaxed);
    std::string mode = request->follower() == true ? "follower" : "normal";
    PDLOG(INFO, "set tablet mode %s", mode.c_str());
    response->set_code(::openmldb::base::ReturnCode::kOk);
}

void TabletImpl::DeleteIndex(RpcController* controller, const ::openmldb::api::DeleteIndexRequest* request,
                             ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        PDLOG(WARNING, "table does not exist. tid %u, pid %u", tid, pid);
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    if (table->GetStorageMode() != ::openmldb::common::kMemory) {
        response->set_code(::openmldb::base::ReturnCode::kOperatorNotSupport);
        response->set_msg("only support mem_table");
        PDLOG(WARNING, "only support mem_table. tid %u, pid %u", tid, pid);
        return;
    }
    std::string root_path;
    if (!ChooseDBRootPath(tid, pid, table->GetStorageMode(), root_path)) {
        response->set_code(::openmldb::base::ReturnCode::kFailToGetDbRootPath);
        response->set_msg("fail to get table db root path");
        PDLOG(WARNING, "table db path is not found. tid %u, pid %u", tid, pid);
        return;
    }
    MemTable* mem_table = dynamic_cast<MemTable*>(table.get());
    if (!mem_table->DeleteIndex(request->idx_name())) {
        response->set_code(::openmldb::base::ReturnCode::kDeleteIndexFailed);
        response->set_msg("delete index failed");
        PDLOG(WARNING, "delete index %s failed. tid %u pid %u", request->idx_name().c_str(), tid, pid);
        return;
    }
    std::string db_path = GetDBPath(root_path, tid, pid);
    WriteTableMeta(db_path, table->GetTableMeta().get());
    PDLOG(INFO, "delete index %s success. tid %u pid %u", request->idx_name().c_str(), tid, pid);
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void TabletImpl::SendIndexData(RpcController* controller, const ::openmldb::api::SendIndexDataRequest* request,
                               ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kSendIndexRequest, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    do {
        std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
        if (!table) {
            PDLOG(WARNING, "table does not exist. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
            response->set_msg("table does not exist");
            break;
        }
        if (table->GetStorageMode() != ::openmldb::common::kMemory) {
            response->set_code(::openmldb::base::ReturnCode::kOperatorNotSupport);
            response->set_msg("only support mem_table");
            PDLOG(WARNING, "only support mem_table. tid %u, pid %u", request->tid(), request->pid());
            return;
        }
        MemTable* mem_table = dynamic_cast<MemTable*>(table.get());
        if (mem_table == nullptr) {
            PDLOG(WARNING, "table is not memtable. tid %u, pid %u", request->tid(), request->pid());
            response->set_code(::openmldb::base::ReturnCode::kTableTypeMismatch);
            response->set_msg("table is not memtable");
            break;
        }
        std::map<uint32_t, std::string> pid_endpoint_map;
        for (int idx = 0; idx < request->pairs_size(); idx++) {
            pid_endpoint_map.insert(std::make_pair(request->pairs(idx).pid(), request->pairs(idx).endpoint()));
        }
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        if (pid_endpoint_map.empty()) {
            PDLOG(INFO, "pid endpoint map is empty. tid %u, pid %u", request->tid(), request->pid());
            SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kDone);
        } else {
            task_pool_.AddTask(
                boost::bind(&TabletImpl::SendIndexDataInternal, this, table, pid_endpoint_map, task_ptr));
        }
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

void TabletImpl::SendIndexDataInternal(std::shared_ptr<::openmldb::storage::Table> table,
                                       const std::map<uint32_t, std::string>& pid_endpoint_map,
                                       std::shared_ptr<::openmldb::api::TaskInfo> task_ptr) {
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    std::string db_root_path;
    if (!ChooseDBRootPath(tid, pid, table->GetStorageMode(), db_root_path)) {
        PDLOG(WARNING, "fail to find db root path for table tid %u pid %u storage_mode %s", tid, pid,
              common::StorageMode_Name(table->GetStorageMode()));
        SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
        return;
    }
    std::string index_path = GetDBPath(db_root_path, tid, pid) + "/index/";
    for (const auto& kv : pid_endpoint_map) {
        if (kv.first == pid) {
            continue;
        }
        std::string index_file_name = absl::StrCat(pid, "_", kv.first, "_index.data");
        std::string src_file = index_path + index_file_name;
        if (!::openmldb::base::IsExists(src_file)) {
            PDLOG(WARNING, "file %s does not exist. tid[%u] pid[%u]", src_file.c_str(), tid, pid);
            continue;
        }
        if (kv.second == endpoint_) {
            std::shared_ptr<Table> des_table = GetTable(tid, kv.first);
            if (!table) {
                PDLOG(WARNING, "table does not exist. tid[%u] pid[%u]", tid, kv.first);
                SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
                return;
            }
            std::string des_db_root_path;
            if (!ChooseDBRootPath(tid, kv.first, table->GetStorageMode(), des_db_root_path)) {
                PDLOG(WARNING, "fail to find db root path for table tid %u pid %u storage_mode %s", tid, kv.first,
                      common::StorageMode_Name(table->GetStorageMode()));
                SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
                return;
            }
            std::string des_index_path = GetDBPath(des_db_root_path, tid, kv.first) + "/index/";
            if (!::openmldb::base::IsExists(des_index_path) && !::openmldb::base::MkdirRecur(des_index_path)) {
                PDLOG(WARNING, "mkdir failed. tid[%u] pid[%u] path[%s]", tid, pid, des_index_path.c_str());
                SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
                return;
            }
            if (db_root_path == des_db_root_path) {
                if (!::openmldb::base::Rename(src_file, des_index_path + index_file_name)) {
                    PDLOG(WARNING, "rename dir failed. tid[%u] pid[%u] file[%s]", tid, pid, index_file_name.c_str());
                    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
                    return;
                }
                PDLOG(INFO, "rename file %s success. tid[%u] pid[%u]", index_file_name.c_str(), tid, pid);
            } else {
                if (!::openmldb::base::CopyFile(src_file, des_index_path + index_file_name)) {
                    PDLOG(WARNING, "copy failed. tid[%u] pid[%u] file[%s]", tid, pid, index_file_name.c_str());
                    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
                    return;
                }
                PDLOG(INFO, "copy file %s success. tid[%u] pid[%u]", index_file_name.c_str(), tid, pid);
            }
        } else {
            std::string real_endpoint = kv.second;
            if (FLAGS_use_name) {
                auto tmp_map = std::atomic_load_explicit(&real_ep_map_, std::memory_order_acquire);
                auto iter = tmp_map->find(kv.second);
                if (iter == tmp_map->end()) {
                    PDLOG(WARNING, "name %s not found in real_ep_map. tid[%u] pid[%u]", kv.second.c_str(), tid, pid);
                    break;
                }
                real_endpoint = iter->second;
            }
            FileSender sender(tid, kv.first, table->GetStorageMode(), real_endpoint);
            if (!sender.Init()) {
                PDLOG(WARNING, "Init FileSender failed. tid[%u] pid[%u] des_pid[%u] endpoint[%s]", tid, pid, kv.first,
                      kv.second.c_str());
                SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
                return;
            }
            if (sender.SendFile(index_file_name, std::string("index"), index_path + index_file_name) < 0) {
                PDLOG(WARNING, "send file %s failed. tid[%u] pid[%u] des_pid[%u]", index_file_name.c_str(), tid, pid,
                      kv.first);
                SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
                return;
            }
            PDLOG(INFO, "send file %s to endpoint %s success. tid[%u] pid[%u] des_pid[%u]", index_file_name.c_str(),
                  kv.second.c_str(), tid, pid, kv.first);
        }
    }
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kDone);
}

void TabletImpl::ExtractIndexDataInternal(std::shared_ptr<::openmldb::storage::Table> table,
                                          std::shared_ptr<::openmldb::storage::MemTableSnapshot> memtable_snapshot,
                                          const std::vector<::openmldb::common::ColumnKey>& column_keys,
                                          uint32_t partition_num, uint64_t offset, bool dump_data,
                                          std::shared_ptr<::openmldb::api::TaskInfo> task) {
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    std::string db_root_path;
    if (!ChooseDBRootPath(tid, pid, table->GetStorageMode(), db_root_path)) {
        PDLOG(WARNING, "fail to find db root path for table tid %u pid %u storage_mode %s", tid, pid,
              common::StorageMode_Name(table->GetStorageMode()));
        SetTaskStatus(task, ::openmldb::api::kFailed);
        return;
    }
    std::string index_path = GetDBPath(db_root_path, tid, pid) + "/index/";
    if (!::openmldb::base::MkdirRecur(index_path)) {
        LOG(WARNING) << "fail to create path " << index_path << ". tid " << tid << " pid " << pid;
        SetTaskStatus(task, ::openmldb::api::kFailed);
        return;
    }
    std::vector<std::shared_ptr<::openmldb::log::WriteHandle>> whs(partition_num);
    if (dump_data) {
        for (uint32_t i = 0; i < partition_num; i++) {
            std::string index_file_name = absl::StrCat(pid, "_", i, "_index.data");
            std::string index_data_path = index_path + index_file_name;
            FILE* fd = fopen(index_data_path.c_str(), "wb+");
            if (fd == nullptr) {
                LOG(WARNING) << "fail to create file " << index_data_path << ". tid " << tid << " pid " << pid;
                SetTaskStatus(task, ::openmldb::api::kFailed);
                return;
            }
            whs[i] = std::make_shared<::openmldb::log::WriteHandle>("off", index_file_name, fd);
        }
    }
    auto status = memtable_snapshot->ExtractIndexData(table, column_keys, whs, offset, dump_data);
    if (status.OK()) {
        PDLOG(INFO, "extract index on table tid[%u] pid[%u] succeed", tid, pid);
        SetTaskStatus(task, ::openmldb::api::kDone);
    } else {
        PDLOG(WARNING, "fail to extract index on table tid[%u] pid[%u] msg[%s]", tid, pid, status.GetMsg().c_str());
        SetTaskStatus(task, ::openmldb::api::kFailed);
    }
    for (auto& wh : whs) {
        if (wh) {
            wh->EndLog();
        }
    }
}

void TabletImpl::LoadIndexData(RpcController* controller, const ::openmldb::api::LoadIndexDataRequest* request,
                               ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kLoadIndexRequest, task_ptr) < 0) {
            response->set_code(-1);
            response->set_msg("add task failed");
            return;
        }
    }
    do {
        uint32_t tid = request->tid();
        uint32_t pid = request->pid();
        auto table = GetTable(tid, pid);
        if (!table) {
            PDLOG(WARNING, "table does not exist. tid %u, pid %u", tid, pid);
            response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
            response->set_msg("table does not exist");
            break;
        }
        if (table->GetStorageMode() != ::openmldb::common::kMemory) {
            response->set_code(::openmldb::base::ReturnCode::kOperatorNotSupport);
            response->set_msg("only support mem_table");
            PDLOG(WARNING, "only support mem_table. tid %u, pid %u", tid, pid);
            break;
        }
        if (table->GetTableStat() != ::openmldb::storage::kNormal) {
            PDLOG(WARNING, "table state is %d, cannot load index data. tid %u, pid %u", table->GetTableStat(), tid,
                  pid);
            response->set_code(::openmldb::base::ReturnCode::kTableStatusIsNotKnormal);
            response->set_msg("table status is not kNormal");
            break;
        }
        response->set_code(::openmldb::base::ReturnCode::kOk);
        response->set_msg("ok");
        if (request->partition_num() <= 1) {
            PDLOG(INFO, "partition num is %d need not load. tid %u, pid %u", request->partition_num(), tid, pid);
            SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kDone);
        } else {
            uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
            task_pool_.AddTask(boost::bind(&TabletImpl::LoadIndexDataInternal, this, tid, pid, 0,
                                           request->partition_num(), cur_time, task_ptr));
        }
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

void TabletImpl::LoadIndexDataInternal(uint32_t tid, uint32_t pid, uint32_t cur_pid, uint32_t partition_num,
                                       uint64_t last_time, std::shared_ptr<::openmldb::api::TaskInfo> task) {
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    if (cur_pid == pid) {
        task_pool_.AddTask(boost::bind(&TabletImpl::LoadIndexDataInternal, this, tid, pid, cur_pid + 1, partition_num,
                                       cur_time, task));
        return;
    }
    ::openmldb::api::TaskStatus status = ::openmldb::api::TaskStatus::kFailed;
    if (GetTaskStatus(task, &status) < 0 || status != ::openmldb::api::TaskStatus::kDoing) {
        PDLOG(INFO, "terminate load index. tid %u pid %u", tid, pid);
        return;
    }
    auto table = GetTable(tid, pid);
    if (!table) {
        PDLOG(WARNING, "table does not exist. tid %u pid %u", tid, pid);
        SetTaskStatus(task, ::openmldb::api::TaskStatus::kFailed);
        return;
    }
    auto replicator = GetReplicator(tid, pid);
    if (!replicator) {
        PDLOG(WARNING, "replicator does not exist. tid %u pid %u", tid, pid);
        SetTaskStatus(task, ::openmldb::api::TaskStatus::kFailed);
        return;
    }
    std::string db_root_path;
    bool ok = ChooseDBRootPath(tid, pid, table->GetStorageMode(), db_root_path);
    if (!ok) {
        PDLOG(WARNING, "fail to find db root path for table tid %u pid %u storage_mode %s", tid, pid,
              common::StorageMode_Name(table->GetStorageMode()));
        SetTaskStatus(task, ::openmldb::api::TaskStatus::kFailed);
        return;
    }
    std::string index_path = GetDBPath(db_root_path, tid, pid) + "/index/";
    std::string index_file_path = absl::StrCat(index_path, cur_pid, "_", pid, "_index.data");
    if (!::openmldb::base::IsExists(index_file_path)) {
        if (last_time + FLAGS_load_index_max_wait_time < cur_time) {
            PDLOG(WARNING, "wait time too long. tid %u pid %u file %s", tid, pid, index_file_path.c_str());
            SetTaskStatus(task, ::openmldb::api::TaskStatus::kFailed);
            return;
        }
        task_pool_.DelayTask(FLAGS_task_check_interval, boost::bind(&TabletImpl::LoadIndexDataInternal, this, tid, pid,
                                                                    cur_pid, partition_num, last_time, task));
        return;
    }
    FILE* fd = fopen(index_file_path.c_str(), "rb");
    if (fd == nullptr) {
        PDLOG(WARNING, "fail to open index file %s. tid %u, pid %u", index_file_path.c_str(), tid, pid);
        SetTaskStatus(task, ::openmldb::api::TaskStatus::kFailed);
        return;
    }
    auto seq_file = std::unique_ptr<::openmldb::log::SequentialFile>(::openmldb::log::NewSeqFile(index_file_path, fd));
    ::openmldb::log::Reader reader(seq_file.get(), nullptr, false, 0, false);
    std::string buffer;
    uint64_t succ_cnt = 0;
    uint64_t failed_cnt = 0;
    while (true) {
        buffer.clear();
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = reader.ReadRecord(&record, &buffer);
        if (status.IsWaitRecord() || status.IsEof()) {
            PDLOG(INFO, "read path %s for table tid %u pid %u completed. succ_cnt %lu, failed_cnt %lu",
                  index_file_path.c_str(), tid, pid, succ_cnt, failed_cnt);
            break;
        }
        if (!status.ok()) {
            PDLOG(WARNING, "fail to read record for tid %u, pid %u with error %s", tid, pid, status.ToString().c_str());
            failed_cnt++;
            continue;
        }
        ::openmldb::api::LogEntry entry;
        entry.ParseFromString(std::string(record.data(), record.size()));
        if (entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete) {
            table->Delete(entry);
        } else {
            table->Put(entry);
        }
        replicator->AppendEntry(entry);
        succ_cnt++;
    }
    if (cur_pid == partition_num - 1 || (cur_pid + 1 == pid && pid == partition_num - 1)) {
        if (FLAGS_recycle_bin_enabled) {
            std::string recycle_bin_root_path;
            ok = ChooseRecycleBinRootPath(tid, pid, table->GetStorageMode(), recycle_bin_root_path);
            if (!ok) {
                LOG(WARNING) << "fail to get recycle bin root path. tid " << tid << " pid " << pid;
                openmldb::base::RemoveDirRecursive(index_path);
            } else {
                std::string recycle_path = absl::StrCat(recycle_bin_root_path, "/", tid, "_", pid);
                if (!openmldb::base::IsExists(recycle_path)) {
                    openmldb::base::Mkdir(recycle_path);
                }
                std::string dst = absl::StrCat(recycle_path, "/index", openmldb::base::GetNowTime());
                openmldb::base::Rename(index_path, dst);
            }
        } else {
            openmldb::base::RemoveDirRecursive(index_path);
        }
        LOG(INFO) << "load index surccess. tid " << tid << " pid " << pid;
        SetTaskStatus(task, ::openmldb::api::TaskStatus::kDone);
        return;
    }
    cur_time = ::baidu::common::timer::get_micros() / 1000;
    task_pool_.AddTask(
        boost::bind(&TabletImpl::LoadIndexDataInternal, this, tid, pid, cur_pid + 1, partition_num, cur_time, task));
}

void TabletImpl::ExtractIndexData(RpcController* controller, const ::openmldb::api::ExtractIndexDataRequest* request,
                                  ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::openmldb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        if (AddOPTask(request->task_info(), ::openmldb::api::TaskType::kExtractIndexRequest, task_ptr) < 0) {
            base::SetResponseStatus(-1, "add task failed", response);
            return;
        }
    }
    do {
        uint32_t tid = request->tid();
        uint32_t pid = request->pid();
        std::shared_ptr<Table> table;
        std::shared_ptr<Snapshot> snapshot;
        {
            std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
            table = GetTableUnLock(tid, pid);
            if (!table) {
                PDLOG(WARNING, "table does not exist. tid %u pid %u", tid, pid);
                base::SetResponseStatus(base::ReturnCode::kTableIsNotExist, "table does not exist", response);
                break;
            }
            if (table->GetStorageMode() != ::openmldb::common::kMemory) {
                response->set_code(::openmldb::base::ReturnCode::kOperatorNotSupport);
                PDLOG(WARNING, "only support mem_table. tid %u pid %u", tid, pid);
                response->set_msg("only support mem_table");
                break;
            }
            if (table->GetTableStat() != ::openmldb::storage::kNormal) {
                PDLOG(WARNING, "table state is %d, cannot extract index data. tid %u, pid %u", table->GetTableStat(),
                      tid, pid);
                base::SetResponseStatus(base::ReturnCode::kTableStatusIsNotKnormal, "table status is not kNormal",
                                        response);
                break;
            }
            snapshot = GetSnapshotUnLock(tid, pid);
            if (!snapshot) {
                PDLOG(WARNING, "snapshot does not exist. tid %u pid %u", tid, pid);
                base::SetResponseStatus(base::ReturnCode::kSnapshotIsNotExist, "table snapshot does not exist",
                                        response);
                break;
            }
        }
        std::vector<::openmldb::common::ColumnKey> index_vec;
        for (const auto& cur_column_key : request->column_key()) {
            index_vec.push_back(cur_column_key);
        }
        auto memtable_snapshot = std::static_pointer_cast<::openmldb::storage::MemTableSnapshot>(snapshot);
        if (IsClusterMode()) {
            task_pool_.AddTask(boost::bind(&TabletImpl::ExtractIndexDataInternal, this, table, memtable_snapshot,
                                           index_vec, request->partition_num(), request->offset(), request->dump_data(),
                                           task_ptr));
        } else {
            ExtractIndexDataInternal(table, memtable_snapshot, index_vec, request->partition_num(), request->offset(),
                                     false, nullptr);
        }
        base::SetResponseOK(response);
        return;
    } while (0);
    SetTaskStatus(task_ptr, ::openmldb::api::TaskStatus::kFailed);
}

void TabletImpl::AddIndex(RpcController* controller, const ::openmldb::api::AddIndexRequest* request,
                          ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        PDLOG(WARNING, "table does not exist. tid %u, pid %u", tid, pid);
        base::SetResponseStatus(base::ReturnCode::kTableIsNotExist, "table does not exist", response);
        return;
    }
    if (table->GetStorageMode() != ::openmldb::common::kMemory) {
        response->set_code(::openmldb::base::ReturnCode::kOperatorNotSupport);
        response->set_msg("only support mem_table");
        PDLOG(WARNING, "only support mem_table. tid %u, pid %u", tid, pid);
        return;
    }
    auto* mem_table = dynamic_cast<MemTable*>(table.get());
    if (mem_table == nullptr) {
        PDLOG(WARNING, "table is not memtable. tid %u, pid %u", tid, pid);
        base::SetResponseStatus(base::ReturnCode::kTableTypeMismatch, "table is not memtable", response);
        return;
    }
    if (request->column_keys_size() > 0) {
        for (const auto& column_key : request->column_keys()) {
            // TODO(denglong): support add multi indexs in memory table
            if (!mem_table->AddIndex(column_key)) {
                PDLOG(WARNING, "add index %s failed. tid %u, pid %u", column_key.index_name().c_str(), tid, pid);
                base::SetResponseStatus(base::ReturnCode::kAddIndexFailed, "add index failed", response);
                return;
            }
        }
    } else {
        if (!mem_table->AddIndex(request->column_key())) {
            PDLOG(WARNING, "add index failed. tid %u, pid %u", tid, pid);
            base::SetResponseStatus(base::ReturnCode::kAddIndexFailed, "add index failed", response);
            return;
        }
    }
    std::string db_root_path;
    bool ok = ChooseDBRootPath(tid, pid, table->GetStorageMode(), db_root_path);
    if (!ok) {
        base::SetResponseStatus(base::ReturnCode::kFailToGetDbRootPath, "fail to get db root path", response);
        PDLOG(WARNING, "fail to get table db root path for tid %u, pid %u", tid, pid);
        return;
    }
    std::string db_path = GetDBPath(db_root_path, tid, pid);
    if (!::openmldb::base::IsExists(db_path)) {
        PDLOG(WARNING, "table db path doesn't exist. tid %u, pid %u", tid, pid);
        base::SetResponseStatus(base::ReturnCode::kTableDbPathIsNotExist, "table db path does not exist", response);
        return;
    }
    if (WriteTableMeta(db_path, table->GetTableMeta().get()) < 0) {
        PDLOG(WARNING, "write table_meta failed. tid[%u] pid[%u]", tid, pid);
        base::SetResponseStatus(base::ReturnCode::kWriteDataFailed, "write meta data failed", response);
        return;
    }
    if (request->column_keys_size() > 0) {
        for (const auto& column_key : request->column_keys()) {
            PDLOG(INFO, "add index %s ok. tid %u pid %u", column_key.index_name().c_str(), tid, pid);
        }
    } else {
        PDLOG(INFO, "add index %s ok. tid %u pid %u", request->column_key().index_name().c_str(), tid, pid);
    }
    if (!catalog_->UpdateTableMeta(*(table->GetTableMeta()))) {
        PDLOG(WARNING, "update table meta failed. tid %u pid %u", tid, pid);
    }
    base::SetResponseOK(response);
}

void TabletImpl::CancelOP(RpcController* controller, const openmldb::api::CancelOPRequest* request,
                          openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    uint64_t op_id = request->op_id();
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = task_map_.find(op_id);
        if (iter != task_map_.end()) {
            for (auto& task : iter->second) {
                if (task->status() == ::openmldb::api::TaskStatus::kInited ||
                    task->status() == ::openmldb::api::TaskStatus::kDoing) {
                    task->set_status(::openmldb::api::TaskStatus::kCanceled);
                    PDLOG(INFO, "cancel op [%lu] task_type[%s] ", op_id,
                          ::openmldb::api::TaskType_Name(task->task_type()).c_str());
                }
            }
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

void TabletImpl::UpdateRealEndpointMap(RpcController* controller,
                                       const openmldb::api::UpdateRealEndpointMapRequest* request,
                                       openmldb::api::GeneralResponse* response, Closure* done) {
    DLOG(INFO) << "UpdateRealEndpointMap";
    brpc::ClosureGuard done_guard(done);
    if (FLAGS_zk_cluster.empty()) {
        response->set_code(-1);
        response->set_msg("tablet is not run in cluster mode");
        PDLOG(WARNING, "tablet is not run in cluster mode");
        return;
    }
    decltype(real_ep_map_) tmp_real_ep_map = std::make_shared<std::map<std::string, std::string>>();
    for (int i = 0; i < request->real_endpoint_map_size(); i++) {
        auto& pair = request->real_endpoint_map(i);
        tmp_real_ep_map->insert(std::make_pair(pair.name(), pair.real_endpoint()));
    }
    std::atomic_store_explicit(&real_ep_map_, tmp_real_ep_map, std::memory_order_release);
    DLOG(INFO) << "real_ep_map size is " << tmp_real_ep_map->size();
    for (const auto& kv : *tmp_real_ep_map) {
        LOG(INFO) << "update real endpoint: " << kv.first << " : " << kv.second;
    }
    catalog_->UpdateClient(*tmp_real_ep_map);
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
}

bool TabletImpl::GetRealEp(uint64_t tid, uint64_t pid, std::map<std::string, std::string>* real_ep_map) {
    if (real_ep_map == nullptr) {
        return false;
    }
    auto tmp_map = std::atomic_load_explicit(&real_ep_map_, std::memory_order_acquire);
    for (auto rit = real_ep_map->begin(); rit != real_ep_map->end(); ++rit) {
        auto iter = tmp_map->find(rit->first);
        if (iter == tmp_map->end()) {
            PDLOG(WARNING,
                  "name %s not found in real_ep_map."
                  "tid[%u] pid[%u]",
                  rit->first.c_str(), tid, pid);
            return false;
        }
        rit->second = iter->second;
    }
    return true;
}

void TabletImpl::CreateProcedure(RpcController* controller, const openmldb::api::CreateProcedureRequest* request,
                                 openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    auto& sp_info = request->sp_info();
    const std::string& db_name = sp_info.db_name();
    const std::string& sp_name = sp_info.sp_name();
    const std::string& sql = sp_info.sql();
    if (sp_cache_->ProcedureExist(db_name, sp_name)) {
        response->set_code(::openmldb::base::ReturnCode::kProcedureAlreadyExists);
        response->set_msg("store procedure already exists");
        PDLOG(WARNING, "store procedure[%s] already exists in db[%s]", sp_name.c_str(), db_name.c_str());
        return;
    }
    ::hybridse::base::Status status;
    auto sp_info_impl = std::make_shared<openmldb::catalog::ProcedureInfoImpl>(sp_info);

    auto long_windows = sp_info_impl->GetOption(hybridse::vm::LONG_WINDOWS);
    std::shared_ptr<std::unordered_map<std::string, std::string>> options = nullptr;
    if (long_windows) {
        options = std::make_shared<std::unordered_map<std::string, std::string>>();
        options->emplace(hybridse::vm::LONG_WINDOWS, *long_windows);
    }
    // in deploy, add index-> create procedure, but index may be flipped over(perhaps zk RefreshTableInfo), may get
    // compile error 'Isn't partition provider'

    // build for single request
    ::hybridse::vm::RequestRunSession session;
    session.SetOptions(options);
    bool ok = engine_->Get(sql, db_name, session, status);
    if (!ok || session.GetCompileInfo() == nullptr) {
        response->set_msg(status.str());
        response->set_code(::openmldb::base::kSQLCompileError);
        LOG(WARNING) << "fail to compile sql " << sql << std::endl << status.str();
        return;
    }

    // build for batch request
    ::hybridse::vm::BatchRequestRunSession batch_session;
    batch_session.SetOptions(options);
    for (auto i = 0; i < sp_info.input_schema_size(); ++i) {
        bool is_constant = sp_info.input_schema().Get(i).is_constant();
        if (is_constant) {
            batch_session.AddCommonColumnIdx(i);
        }
    }
    ok = engine_->Get(sql, db_name, batch_session, status);
    if (!ok || batch_session.GetCompileInfo() == nullptr) {
        response->set_msg(status.str());
        response->set_code(::openmldb::base::kSQLCompileError);
        LOG(WARNING) << "fail to compile batch request for sql " << sql;
        return;
    }

    if (!sp_info_impl) {
        response->set_msg(status.str());
        response->set_code(::openmldb::base::kCreateProcedureFailedOnTablet);
        LOG(WARNING) << "convert procedure info failed, sp_name: " << sp_name << " db: " << db_name;
        return;
    }
    ok = catalog_->AddProcedure(db_name, sp_name, sp_info_impl);
    if (ok) {
        LOG(INFO) << "add procedure " << sp_name << " to catalog with db " << db_name;
    } else {
        LOG(WARNING) << "fail to add procedure " << sp_name << " to catalog with db " << db_name;
    }

    sp_cache_->InsertSQLProcedureCacheEntry(db_name, sp_name, sp_info_impl, session.GetCompileInfo(),
                                            batch_session.GetCompileInfo());

    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
    LOG(INFO) << "create procedure success! sp_name: " << sp_name << ", db: " << db_name << ", sql: " << sql;
}

void TabletImpl::DropProcedure(RpcController* controller, const ::openmldb::api::DropProcedureRequest* request,
                               ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string& db_name = request->db_name();
    const std::string& sp_name = request->sp_name();

    auto sp_info = sp_cache_->FindSpProcedureInfo(db_name, sp_name);
    auto is_deployment_procedure = sp_info.ok() && sp_info.value()->GetType() == hybridse::sdk::kReqDeployment;

    sp_cache_->DropSQLProcedureCacheEntry(db_name, sp_name);
    if (!catalog_->DropProcedure(db_name, sp_name)) {
        LOG(WARNING) << "drop procedure " << db_name << "." << sp_name << " in catalog failed";
    }

    if (is_deployment_procedure) {
        auto collector_key = absl::StrCat(db_name, ".", sp_name);
        auto s = deploy_collector_->DeleteDeploy(db_name, sp_name);
        if (!s.ok()) {
            LOG(ERROR) << "[ERROR] delete deploy collector: " << s;
        } else {
            LOG(INFO) << "deleted deploy collector for " << collector_key;
        }
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    response->set_msg("ok");
    PDLOG(INFO, "drop procedure success. db_name[%s] sp_name[%s]", db_name.c_str(), sp_name.c_str());
}

void TabletImpl::RunRequestQuery(RpcController* ctrl, const openmldb::api::QueryRequest& request,
                                 ::hybridse::vm::RequestRunSession& session, openmldb::api::QueryResponse& response,
                                 butil::IOBuf& buf) {
    if (request.is_debug()) {
        session.EnableDebug();
    }
    ::hybridse::codec::Row row;
    auto& request_buf = dynamic_cast<brpc::Controller*>(ctrl)->request_attachment();
    size_t input_slices = request.row_slices();
    if (!codec::DecodeRpcRow(request_buf, 0, request.row_size(), input_slices, &row)) {
        response.set_code(::openmldb::base::kSQLRunError);
        response.set_msg("fail to decode input row");
        return;
    }
    ::hybridse::codec::Row output;
    int32_t ret = 0;
    if (request.has_task_id()) {
        ret = session.Run(request.task_id(), row, &output);
    } else {
        ret = session.Run(row, &output);
    }
    if (ret != 0) {
        response.set_code(::openmldb::base::kSQLRunError);
        response.set_msg("fail to run sql");
        return;
    } else if (row.GetRowPtrCnt() != 1) {
        response.set_code(::openmldb::base::kSQLRunError);
        response.set_msg("do not support multiple output row slices");
        return;
    }
    size_t buf_total_size;
    if (!codec::EncodeRpcRow(output, &buf, &buf_total_size)) {
        response.set_code(::openmldb::base::kSQLRunError);
        response.set_msg("fail to encode sql output row");
        return;
    }
    if (!request.has_task_id()) {
        response.set_schema(session.GetEncodedSchema());
    }
    response.set_byte_size(buf_total_size);
    response.set_count(1);
    response.set_row_slices(1);
    response.set_code(::openmldb::base::kOk);
}

void TabletImpl::CreateProcedure(const std::shared_ptr<hybridse::sdk::ProcedureInfo>& sp_info) {
    const std::string& db_name = sp_info->GetDbName();
    const std::string& sp_name = sp_info->GetSpName();
    const std::string& sql = sp_info->GetSql();
    auto long_windows = sp_info->GetOption(hybridse::vm::LONG_WINDOWS);
    std::shared_ptr<std::unordered_map<std::string, std::string>> options = nullptr;
    if (long_windows) {
        options = std::make_shared<std::unordered_map<std::string, std::string>>();
        options->emplace(hybridse::vm::LONG_WINDOWS, *long_windows);
    }

    ::hybridse::base::Status status;
    // build for single request
    ::hybridse::vm::RequestRunSession session;
    session.SetOptions(options);
    bool ok = engine_->Get(sql, db_name, session, status);
    if (!ok || session.GetCompileInfo() == nullptr) {
        LOG(WARNING) << "fail to compile sql " << sql;
        return;
    }
    // build for batch request
    ::hybridse::vm::BatchRequestRunSession batch_session;
    batch_session.SetOptions(options);
    for (auto i = 0; i < sp_info->GetInputSchema().GetColumnCnt(); ++i) {
        bool is_constant = sp_info->GetInputSchema().IsConstant(i);
        if (is_constant) {
            batch_session.AddCommonColumnIdx(i);
        }
    }
    ok = engine_->Get(sql, db_name, batch_session, status);
    if (!ok || batch_session.GetCompileInfo() == nullptr) {
        LOG(WARNING) << "fail to compile batch request for sql " << sql;
        return;
    }
    sp_cache_->InsertSQLProcedureCacheEntry(db_name, sp_name, sp_info, session.GetCompileInfo(),
                                            batch_session.GetCompileInfo());

    LOG(INFO) << "refresh procedure success! sp_name: " << sp_name << ", db: " << db_name << ", sql: " << sql;
}

void TabletImpl::GetBulkLoadInfo(RpcController* controller, const ::openmldb::api::BulkLoadInfoRequest* request,
                                 ::openmldb::api::BulkLoadInfoResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);

    if (follower_.load(std::memory_order_relaxed)) {
        response->set_code(::openmldb::base::ReturnCode::kIsFollowerCluster);
        response->set_msg("is follower cluster");
        return;
    }

    std::shared_ptr<Table> table = GetTable(request->tid(), request->pid());
    if (!table) {
        PDLOG(WARNING, "table does not exist. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    if (!table->IsLeader()) {
        response->set_code(::openmldb::base::ReturnCode::kTableIsFollower);
        response->set_msg("table is follower");
        return;
    }
    if (table->GetTableStat() == ::openmldb::storage::kLoading) {
        PDLOG(WARNING, "table is loading. tid %u, pid %u", request->tid(), request->pid());
        response->set_code(::openmldb::base::ReturnCode::kTableIsLoading);
        response->set_msg("table is loading");
        return;
    }
    if (table->GetStorageMode() != ::openmldb::common::kMemory) {
        response->set_code(::openmldb::base::ReturnCode::kOperatorNotSupport);
        response->set_msg("only support mem_table");
        PDLOG(WARNING, "only support mem_table. tid %u, pid %u", request->tid(), request->pid());
        return;
    }

    // TODO(hw): BulkLoadInfoResponse
    //  TableIndex is inside Table, so let table fulfill the response for us.
    DLOG(INFO) << "GetBulkLoadInfo for " << table->GetId() << "-" << table->GetPid();
    auto* mem_table = dynamic_cast<MemTable*>(table.get());
    mem_table->GetBulkLoadInfo(response);

    response->set_code(::openmldb::base::kOk);
    response->set_msg("ok");
}

std::string TabletImpl::GetDBPath(const std::string& root_path, uint32_t tid, uint32_t pid) {
    return root_path + "/" + std::to_string(tid) + "_" + std::to_string(pid);
}

bool TabletImpl::IsCollectDeployStatsEnabled() const {
    auto p = std::atomic_load_explicit(&global_variables_, std::memory_order_relaxed);
    auto it = p->find(DEPLOY_STATS);
    return it != p->end() && (it->second == "true" || it->second == "on");
}

// try collect the cost time for a deployment procedure into collector
// if failed, log inside, no extra handle
void TabletImpl::TryCollectDeployStats(const std::string& db, const std::string& name, absl::Time start_time) {
    absl::Time now = absl::Now();
    absl::Duration time = now - start_time;
    auto st = deploy_collector_->Collect(db, name, time);
    DLOG(INFO) << "collect " << db << "." << name << " latency " << time;
}

void TabletImpl::BulkLoad(RpcController* controller, const ::openmldb::api::BulkLoadRequest* request,
                          ::openmldb::api::GeneralResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);

    response->set_code(::openmldb::base::ReturnCode::kOk);

    if (follower_.load(std::memory_order_relaxed)) {
        response->set_code(::openmldb::base::ReturnCode::kIsFollowerCluster);
        response->set_msg("is follower cluster");
        return;
    }
    uint64_t start_time = ::baidu::common::timer::get_micros();

    auto tid = request->tid();
    auto pid = request->pid();
    std::shared_ptr<Table> table = GetTable(tid, pid);
    if (!table) {
        PDLOG(WARNING, "table %u-%u does not exist.", request->tid(), request->pid());
        response->set_code(::openmldb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("table does not exist");
        return;
    }
    if (!table->IsLeader()) {
        response->set_code(::openmldb::base::ReturnCode::kTableIsFollower);
        response->set_msg("table is follower");
        return;
    }
    if (table->GetTableStat() == ::openmldb::storage::kLoading) {
        PDLOG(WARNING, "table %u-%u is loading.", request->tid(), request->pid());
        response->set_code(::openmldb::base::ReturnCode::kTableIsLoading);
        response->set_msg("table is loading");
        return;
    }

    // first DataRegion, then IndexRegion, when we get IndexRegion rpc, empty DataRegion is available
    auto* cntl = dynamic_cast<brpc::Controller*>(controller);
    const auto& data = cntl->request_attachment();
    // Data is a part of MemTable data, DataReceiver of MemTable is in charge of it.
    // Data part_id & info is in request, checked in AppendData()
    if (!data.empty()) {
        if (!bulk_load_mgr_.AppendData(tid, pid, request, data)) {
            response->set_code(::openmldb::base::ReturnCode::kReceiveDataError);
            response->set_msg("bulk load data region append failed");
            LOG(WARNING) << tid << "-" << pid << " " << response->msg();
            return;
        }
        LOG(INFO) << tid << "-" << pid << " has loaded data region part " << request->part_id()
                  << ". Time:" << ::baidu::common::timer::get_micros() - start_time << " us";

        // TODO(hw): sync first, maybe use async later, e.g. another rpc request to load binlog.

        // we send x data blocks, so there will be x binlog info.
        if (request->binlog_info_size() != request->block_info_size()) {
            response->set_code(::openmldb::base::ReturnCode::kReceiveDataError);
            response->set_msg("bulk load data region and binlog info size mismatch");
            LOG(WARNING) << tid << "-" << pid << " " << response->msg();
            return;
        }
        auto binlog_start = ::baidu::common::timer::get_micros();
        std::shared_ptr<LogReplicator> replicator;
        do {
            replicator = GetReplicator(request->tid(), request->pid());
            if (!replicator) {
                PDLOG(WARNING, "fail to find table tid %u pid %u leader's log replicator", request->tid(),
                      request->pid());
                break;
            }

            auto ok = bulk_load_mgr_.WriteBinlogToReplicator(tid, pid, replicator, request);
            if (!ok) {
                LOG(WARNING) << tid << "-" << pid << " write binlog failed";
            }
        } while (false);
        auto binlog_end = ::baidu::common::timer::get_micros();
        PDLOG(INFO, "%u-%u, binlog cost %lu us", request->tid(), request->pid(), binlog_end - binlog_start);

        if (replicator) {
            if (FLAGS_binlog_notify_on_put) {
                replicator->Notify();
            }
        }
    }

    if (request->index_region_size() > 0) {
        LOG(INFO) << tid << "-" << pid << " get index region, do bulk load";
        // table must disable gc when index region loading, but we can't know which is the first index region part,
        // set it every time.
        std::dynamic_pointer_cast<MemTable>(table)->SetExpire(false);
        // The request may have both data & index region(the first index rpc, contains some rest data), it's ok.
        // BulkLoad() only load index region to table.
        if (!bulk_load_mgr_.BulkLoad(std::dynamic_pointer_cast<MemTable>(table), request)) {
            response->set_code(::openmldb::base::ReturnCode::kWriteDataFailed);
            response->set_msg("bulk load to table failed");
            LOG(WARNING) << tid << "-" << pid << " " << response->msg();
            return;
        }

        uint64_t load_time = ::baidu::common::timer::get_micros();
        PDLOG(INFO, "%u-%u, bulk load only load cost %lu us", request->tid(), request->pid(), load_time - start_time);
    }

    // If the previous parts load succeed, and no other parts, only need to remove the data receiver
    // If not, we delete all relative memory when drop the table.
    if (request->eof()) {
        LOG(INFO) << tid << "-" << pid << " get bulk load eof(means success), clean up the data receiver";
        bulk_load_mgr_.RemoveReceiver(tid, pid);
        std::dynamic_pointer_cast<MemTable>(table)->SetExpire(true);
    }
}

void TabletImpl::CreateFunction(RpcController* controller, const openmldb::api::CreateFunctionRequest* request,
                                openmldb::api::CreateFunctionResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    auto status = CreateFunctionInternal(request->fun());
    response->set_code(status.code);
    response->set_msg(status.msg);
}

base::Status TabletImpl::CreateFunctionInternal(const ::openmldb::common::ExternalFun& fun) {
    hybridse::node::DataType return_type;
    openmldb::schema::SchemaAdapter::ConvertType(fun.return_type(), &return_type);
    if (fun.file().empty()) {
        LOG(WARNING) << "file is empty";
        return {base::kCreateFunctionFailed, "file is empty"};
    }
    std::vector<hybridse::node::DataType> arg_types;
    for (int idx = 0; idx < fun.arg_type_size(); idx++) {
        hybridse::node::DataType data_type;
        openmldb::schema::SchemaAdapter::ConvertType(fun.arg_type(idx), &data_type);
        arg_types.emplace_back(data_type);
    }
    auto status = engine_->RegisterExternalFunction(fun.name(), return_type, fun.return_nullable(), arg_types,
                                                    fun.arg_nullable(), fun.is_aggregate(), fun.file());
    if (status.isOK()) {
        LOG(INFO) << "create function success. name " << fun.name() << " path " << fun.file();
        return {};
    } else {
        return {base::kCreateFunctionFailed, status.msg};
    }
}

void TabletImpl::DropFunction(RpcController* controller, const openmldb::api::DropFunctionRequest* request,
                              openmldb::api::DropFunctionResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const auto& fun = request->fun();
    std::vector<hybridse::node::DataType> arg_types;
    for (int idx = 0; idx < fun.arg_type_size(); idx++) {
        hybridse::node::DataType data_type;
        openmldb::schema::SchemaAdapter::ConvertType(fun.arg_type(idx), &data_type);
        arg_types.emplace_back(data_type);
    }
    engine_->ClearCacheLocked("");
    auto status = engine_->RemoveExternalFunction(fun.name(), arg_types, fun.file());
    if (status.isOK()) {
        LOG(INFO) << "Drop function success. name " << fun.name() << " path " << fun.file();
        base::SetResponseOK(response);
    } else {
        // udf remove failed but it's ok to recreate even it exists, nameserver should treat it as success
        SET_RESP_AND_WARN(response, base::ReturnCode::kDeleteFailed,
                          absl::StrCat("drop function failed, name ", fun.name(), ", error: [", status.GetCode(), "] ",
                                       status.str()));
    }
}

void TabletImpl::CreateAggregator(RpcController* controller, const ::openmldb::api::CreateAggregatorRequest* request,
                                  ::openmldb::api::CreateAggregatorResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::string msg;

    // persistent aggregator meta
    std::string aggr_path;
    std::shared_ptr<Table> aggr_table = GetTable(request->aggr_table_tid(), request->aggr_table_pid());
    auto table_meta = aggr_table->GetTableMeta();
    ::openmldb::common::StorageMode mode = table_meta->storage_mode();
    bool ok = ChooseDBRootPath(request->aggr_table_tid(), request->aggr_table_pid(), mode, aggr_path);
    if (!ok) {
        response->set_code(::openmldb::base::ReturnCode::kFailToGetDbRootPath);
        response->set_msg("fail to get pre-aggr table db root path");
        PDLOG(WARNING, "pre-aggr table db path is not found. tid %u, pid %u", request->aggr_table_tid(),
              request->aggr_table_pid());
        return;
    }
    std::string aggr_table_path = GetDBPath(aggr_path, request->aggr_table_tid(), request->aggr_table_pid());
    if (!::openmldb::base::IsExists(aggr_table_path)) {
        response->set_code(::openmldb::base::ReturnCode::kTableDbPathIsNotExist);
        response->set_msg("pre-aggr table db path does not exist");
        return;
    }
    std::string aggr_info;
    google::protobuf::TextFormat::PrintToString(*request, &aggr_info);
    std::string aggr_info_path = aggr_table_path + "/aggr_info.txt";

    FILE* fd_write = fopen(aggr_info_path.c_str(), "w");
    if (fd_write == nullptr) {
        PDLOG(WARNING, "fail to open file %s. err[%d: %s]", aggr_info_path.c_str(), errno, strerror(errno));
        response->set_code(::openmldb::base::ReturnCode::kError);
        response->set_msg("open aggr info file failed");
        return;
    }
    if (fputs(aggr_info.c_str(), fd_write) == EOF) {
        PDLOG(WARNING, "write error. path[%s], err[%d: %s]", aggr_info_path.c_str(), errno, strerror(errno));
        fclose(fd_write);
        return;
    }
    fclose(fd_write);

    if (!CreateAggregatorInternal(request, msg)) {
        response->set_code(::openmldb::base::ReturnCode::kError);
        response->set_msg(msg.c_str());
        return;
    }
    response->set_code(::openmldb::base::ReturnCode::kOk);
    return;
}

bool TabletImpl::CreateAggregatorInternal(const ::openmldb::api::CreateAggregatorRequest* request, std::string& msg) {
    const auto& base_meta = request->base_table_meta();
    std::shared_ptr<Table> aggr_table = GetTable(request->aggr_table_tid(), request->aggr_table_pid());
    if (!aggr_table) {
        PDLOG(WARNING, "table does not exist. tid %u, pid %u", request->aggr_table_tid(), request->aggr_table_pid());
        msg.assign("table does not exist");
        return false;
    }
    auto aggr_replicator = GetReplicator(request->aggr_table_tid(), request->aggr_table_pid());
    auto base_table = GetTable(base_meta.tid(), base_meta.pid());
    if (!base_table) {
        PDLOG(WARNING, "base table does not exist. tid %u, pid %u", base_meta.tid(), base_meta.pid());
        return false;
    }
    auto aggregator = ::openmldb::storage::CreateAggregator(base_meta, base_table,
            *aggr_table->GetTableMeta(), aggr_table, aggr_replicator, request->index_pos(), request->aggr_col(),
            request->aggr_func(), request->order_by_col(), request->bucket_size(), request->filter_col());
    if (!aggregator) {
        msg.assign("create aggregator failed");
        return false;
    }

    auto base_replicator = GetReplicator(base_meta.tid(), base_meta.pid());
    if (!aggregator->Init(base_replicator)) {
        PDLOG(WARNING, "aggregator init failed");
    }
    uint64_t uid = (uint64_t)base_meta.tid() << 32 | base_meta.pid();
    {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        auto iter = aggregators_.find(uid);
        if (iter == aggregators_.end()) {
            iter = aggregators_.emplace(uid, std::make_shared<Aggrs>()).first;
        }
        iter->second->push_back(aggregator);
    }
    return true;
}

void TabletImpl::GetAndFlushDeployStats(::google::protobuf::RpcController* controller,
                                        const ::openmldb::api::GAFDeployStatsRequest* request,
                                        ::openmldb::api::DeployStatsResponse* response,
                                        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    // TODO(hw): delete rpc?
    response->set_code(ReturnCode::kOk);
}

}  // namespace tablet
}  // namespace openmldb
