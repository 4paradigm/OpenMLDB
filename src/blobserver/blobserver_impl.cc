//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-16

#include "blobserver/blobserver_impl.h"

#include <utility>

#include "gflags/gflags.h"
#include "google/protobuf/text_format.h"

#include "base/file_util.h"
#include "base/glog_wapper.h"
#include "base/hash.h"
#include "base/status.h"
#include "base/strings.h"
#include "boost/bind.hpp"

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_string(hdd_root_path);
DECLARE_uint32(oss_flush_size);
DECLARE_int32(oss_flush_period);
DECLARE_bool(use_name);
DECLARE_bool(recycle_bin_enabled);
DECLARE_int32(task_pool_size);
DECLARE_uint32(oss_flush_delay);

using ::rtidb::base::ReturnCode;
using ::rtidb::base::BLOB_PREFIX;

namespace rtidb {
namespace blobserver {

static const uint32_t SEED = 0xe17a1465;

BlobServerImpl::BlobServerImpl()
    : spin_mutex_(),
      zk_client_(nullptr),
      server_(nullptr),
      keep_alive_pool_(1),
      task_pool_(FLAGS_task_pool_size),
      follower_(false),
      object_stores_() {}

BlobServerImpl::~BlobServerImpl() {
    if (zk_client_ != nullptr) {
        delete zk_client_;
    }
    task_pool_.Stop(true);
}

bool BlobServerImpl::Init(const std::string& real_endpoint) {
    if (FLAGS_hdd_root_path.empty()) {
        PDLOG(WARNING, "hdd root path did not set");
        return false;
    }
    if (!FLAGS_zk_cluster.empty()) {
        zk_client_ =
            new ZkClient(FLAGS_zk_cluster, real_endpoint,
                    FLAGS_zk_session_timeout,
                    BLOB_PREFIX + FLAGS_endpoint, FLAGS_zk_root_path);
        bool ok = zk_client_->Init();
        if (!ok) {
            PDLOG(WARNING, "fail to init zookeeper with cluster %s",
                  FLAGS_zk_cluster.c_str());
            return false;
        }
    } else {
        PDLOG(INFO, "zk cluster disabled");
    }
    std::vector<std::string> root_paths;
    ::rtidb::base::SplitString(FLAGS_hdd_root_path, ",", root_paths);
    for (auto &it : root_paths) {
        bool ok = ::rtidb::base::MkdirRecur(it + "/recycle");
        if (!ok) {
            PDLOG(WARNING, "fail to creat dir %s", it.c_str());
            return false;
        }
    }
    PDLOG(INFO, "FLAGS_endpoint: %s, real_endpoint: %s.",
            FLAGS_endpoint.c_str(), real_endpoint.c_str());
    return true;
}

bool BlobServerImpl::RegisterZK() {
    if (!FLAGS_zk_cluster.empty()) {
        if (FLAGS_use_name) {
            if (!zk_client_->RegisterName()) {
                return false;
            }
        }
        if (!zk_client_->Register(true)) {
            PDLOG(WARNING, "fail to register blob with value %s%s",
                  BLOB_PREFIX.c_str(), FLAGS_endpoint.c_str());
            return false;
        }
        PDLOG(INFO, "blob with value %s%s register to zk cluster %s ok",
              BLOB_PREFIX.c_str(), FLAGS_endpoint.c_str(),
              FLAGS_zk_cluster.c_str());
        keep_alive_pool_.DelayTask(
            FLAGS_zk_keep_alive_check_interval,
            boost::bind(&BlobServerImpl::CheckZkClient, this));
    }
    return true;
}

void BlobServerImpl::CheckZkClient() {
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
    keep_alive_pool_.DelayTask(
        FLAGS_zk_keep_alive_check_interval,
        boost::bind(&BlobServerImpl::CheckZkClient, this));
}

void BlobServerImpl::CreateTable(RpcController *controller,
                                 const CreateTableRequest *request,
                                 CreateTableResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
    int ret = CreateTable(request->table_meta());
    switch (ret) {
        case 1:
            response->set_code(ReturnCode::kTableMetaIsIllegal);
            response->set_msg("table type illegal");
            break;
        case 2:
            response->set_code(ReturnCode::kTableAlreadyExists);
            response->set_msg("table already exists");
            break;
        case 3:
            response->set_code(ReturnCode::kFailToGetDbRootPath);
            response->set_msg("failto find db root path");
            break;
        case 4:
            response->set_code(ReturnCode::kCreateTableFailed);
            response->set_msg("init object store failed");
            break;
        case 5:
            response->set_code(ReturnCode::kWriteDataFailed);
            response->set_msg("write data failed");
            break;
        default:
            response->set_code(ReturnCode::kOk);
            response->set_msg("ok");
            break;
    }
}

int BlobServerImpl::WriteTableMeta(const std::string& path,
                                   const TableMeta& meta) {
    if (!::rtidb::base::MkdirRecur(path)) {
        PDLOG(WARNING, "fail to create path %s", path.c_str());
        return 1;
    }
    std::string full_path = path + "/table_meta.txt";
    std::string table_meta_info;
    google::protobuf::TextFormat::PrintToString(meta, &table_meta_info);
    FILE* fd_write = fopen(full_path.c_str(), "w");
    if (fd_write == NULL) {
        PDLOG(WARNING, "fail to open file %s. err[%d: %s]", full_path.c_str(),
              errno, strerror(errno));
        return -1;
    }
    if (fputs(table_meta_info.c_str(), fd_write) == EOF) {
        PDLOG(WARNING, "write error. path[%s], err[%d: %s]", full_path.c_str(),
              errno, strerror(errno));
        fclose(fd_write);
        return -1;
    }
    fclose(fd_write);
    return 0;
}

int BlobServerImpl::CreateTable(const TableMeta& meta) {
    uint32_t tid = meta.tid();
    uint32_t pid = meta.pid();
    if (meta.table_type() != ::rtidb::type::kObjectStore) {
        PDLOG(WARNING, "wrong table type check table meta. tid[%u] pid[%u]",
              tid, pid);
        return 1;
    }
    std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
    if (store) {
        PDLOG(WARNING, "table tid[%u] pid[%u] exists", tid, pid);
        return 2;
    }
    if (FLAGS_hdd_root_path.empty()) {
        PDLOG(WARNING, "hdd root path is empty. tid[%u] pid[%u]", tid, pid);
        return 3;
    }
    std::vector<std::string> root_paths;
    ::rtidb::base::SplitString(FLAGS_hdd_root_path, ",", root_paths);
    std::string db_path_suffix = "/" + std::to_string(tid) + "_" +
                                   std::to_string(pid);
    std::string paths;
    for (auto &it : root_paths) {
        it += db_path_suffix;
        int ret = WriteTableMeta(it, meta);
        if (ret != 0) {
            return 5;
        }
        if (!paths.empty()) {
            paths.append(",");
        }
        paths.append(it);
    }
    store = std::make_shared<ObjectStore>(meta, paths, FLAGS_oss_flush_size, FLAGS_oss_flush_period,
                                          FLAGS_hdd_root_path, FLAGS_recycle_bin_enabled);
    if (!store->Init()) {
        PDLOG(WARNING, "init table faield. tid[%u] pid[%u]", tid, pid);
        return 4;
    }
    {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        object_stores_[tid].insert(std::make_pair(pid, store));
    }
    task_pool_.DelayTask(FLAGS_oss_flush_delay * 1000, [this, tid, pid]() { FlushTableInternal(tid, pid); });
    return 0;
}

void BlobServerImpl::Get(RpcController *controller, const GetRequest *request,
                         GetResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
    if (!store) {
        PDLOG(WARNING, "table is not exist. tid[%u] pid[%u]", tid, pid);
        response->set_code(ReturnCode::kTableIsNotExist);
        response->set_msg("table is not exist");
        return;
    }
    rtidb::base::Slice slice = store->Get(request->key());
    if (slice.empty()) {
        PDLOG(WARNING, "key %lld not found. tid[%u] pid[%u]",
              request->key(), tid, pid);
        response->set_code(ReturnCode::kKeyNotFound);
        response->set_msg("key not found");
    } else {
        if (request->use_attachment()) {
            brpc::Controller *cntl =
                static_cast<brpc::Controller *>(controller);
            cntl->response_attachment().append(slice.data(), slice.size());
        } else {
            std::string *value = response->mutable_data();
            value->assign(slice.data(), slice.size());
        }
        response->set_code(ReturnCode::kOk);
        const char *ch = slice.data();
        delete[] ch;
    }
}

void BlobServerImpl::Put(RpcController *controller, const PutRequest *request,
                         PutResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
    uint32_t tid = request->tid();
    uint32_t pid = request->pid();
    std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
    if (!store) {
        PDLOG(WARNING, "table is not exist. tid[%u] pid[%u", tid, pid);
        response->set_code(ReturnCode::kTableIsNotExist);
        response->set_msg("table is not exist");
        return;
    }
    bool ok = false;
    int64_t key = 0;
    if (request->has_key()) {
        ok = store->Store(request->key(), request->data());
    } else {
        ok = store->Store(request->data(), &key);
    }
    if (!ok) {
        PDLOG(WARNING, "put %lld failed. tid[%u] pid[%u]",
              request->has_key() ? request->key() : key, tid, pid);
        response->set_code(ReturnCode::kPutFailed);
        response->set_msg("put failed");
        return;
    }
    if (!request->has_key()) {
        response->set_key(key);
    }
    response->set_code(ReturnCode::kOk);
    response->set_msg("ok");
}

void BlobServerImpl::Delete(RpcController *controller,
                            const DeleteRequest *request,
                            DeleteResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
    if (!request->has_key()) {
        response->set_code(ReturnCode::kKeyNotFound);
        response->set_msg("empty key");
    } else {
        uint32_t tid = request->tid();
        uint32_t pid = request->pid();
        std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
        if (!store) {
            PDLOG(WARNING, "table is not exist. tid[%u] pid[%u]", tid, pid);
            response->set_code(ReturnCode::kTableIsNotExist);
            response->set_msg("table is not exist");
            return;
        }
        bool ok = store->Delete(request->key());
        if (!ok) {
            PDLOG(WARNING, "delete %lld failed. tid[%u] pid[%u]",
                  request->key(), tid, pid);
            response->set_code(ReturnCode::kKeyNotFound);
            response->set_msg("key not found");
            return;
        }
        response->set_code(ReturnCode::kOk);
        response->set_msg("ok");
    }
}

void BlobServerImpl::LoadTable(RpcController *controller,
                               const LoadTableRequest *request,
                               LoadTableResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        // TODO(kongquan): process taskinfo
    }
    int ret = CreateTable(request->table_meta());
    switch (ret) {
        case 1:
            response->set_code(ReturnCode::kTableMetaIsIllegal);
            response->set_msg("table type illegal");
            break;
        case 2:
            response->set_code(ReturnCode::kTableAlreadyExists);
            response->set_msg("table already exists");
            break;
        case 3:
            response->set_code(ReturnCode::kFailToGetDbRootPath);
            response->set_msg("failto find db root path");
            break;
        case 4:
            response->set_code(ReturnCode::kCreateTableFailed);
            response->set_msg("init object store failed");
            break;
        case 5:
            response->set_code(ReturnCode::kWriteDataFailed);
            response->set_msg("write data failed");
            break;

        default:
            response->set_code(ReturnCode::kOk);
            response->set_msg("ok");
            break;
    }
}

std::shared_ptr<ObjectStore> BlobServerImpl::GetStore(uint32_t tid,
                                                      uint32_t pid) {
    std::lock_guard<SpinMutex> lock_guard(spin_mutex_);
    return GetStoreUnLock(tid, pid);
}

std::shared_ptr<ObjectStore> BlobServerImpl::GetStoreUnLock(uint32_t tid,
                                                            uint32_t pid) {
    auto it = object_stores_.find(tid);
    if (it != object_stores_.end()) {
        auto tit = it->second.find(pid);
        if (tit != it->second.end()) {
            return tit->second;
        }
    }
    return std::shared_ptr<ObjectStore>();
}

void BlobServerImpl::GetStoreStatus(RpcController *controller,
                                    const GetStoreStatusRequest *request,
                                    GetStoreStatusResponse *response,
                                    Closure *done) {
    brpc::ClosureGuard done_guard(done);
    if (request->has_tid() && request->has_pid()) {
        uint32_t tid = request->tid();
        uint32_t pid = request->pid();
        std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
        if (!store) {
            PDLOG(WARNING, "tid[%u] pid[%u] does not exist", tid, pid);
            response->set_code(ReturnCode::kOk);
            return;
        }
        StoreStatus *status = response->add_all_status();
        status->set_tid(tid);
        status->set_pid(pid);
        response->set_code(ReturnCode::kOk);
        return;
    }
    std::lock_guard<SpinMutex> lock_guard(spin_mutex_);
    for (const auto &it : object_stores_) {
        for (const auto &tit : it.second) {
            StoreStatus *status = response->add_all_status();
            status->set_tid(it.first);
            status->set_pid(tit.first);
        }
    }
    response->set_code(ReturnCode::kOk);
}

void BlobServerImpl::DropTable(RpcController *controller,
                               const DropTableRequest *request,
                               DropTableResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
    if (request->has_tid() && request->has_pid()) {
        uint32_t tid = request->tid();
        uint32_t pid = request->pid();
        std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
        if (!store) {
            response->set_code(ReturnCode::kOk);
            PDLOG(WARNING, "tid[%u] pid[%u] does not exist", tid, pid);
            return;
        }
        task_pool_.AddTask(
            boost::bind(&BlobServerImpl::DropTableInternal, this, tid, pid));
        response->set_code(ReturnCode::kOk);
        response->set_msg("ok");
        PDLOG(INFO, "drop table tid[%u] pid[%u]", tid, pid);
    } else {
        response->set_code(::rtidb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("please provide tid and pid");
        return;
    }
}

void BlobServerImpl::DropTableInternal(uint32_t tid, uint32_t pid) {
    std::shared_ptr<ObjectStore> store;
    {
        std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
        store = GetStoreUnLock(tid, pid);
        if (!store) {
            PDLOG(WARNING, "tid[%u] pid[%u] does not exist", tid, pid);
            return;
        }
        object_stores_[tid].erase(pid);
        if (object_stores_[tid].empty()) {
            object_stores_.erase(tid);
        }
    }
    store.reset();

    PDLOG(INFO, "drop table tid[%u] pid[%u] success", tid, pid);
}
void BlobServerImpl::FlushTableInternal(uint32_t tid, uint32_t pid) {
    std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
    if (!store) {
        PDLOG(WARNING, "tid[%u] pid[%u] does not exist", tid, pid);
        return;
    }
    store->DoFlash();
    task_pool_.DelayTask(FLAGS_oss_flush_delay * 1000, [this, tid, pid]() { FlushTableInternal(tid, pid); });
}

}  // namespace blobserver
}  // namespace rtidb
