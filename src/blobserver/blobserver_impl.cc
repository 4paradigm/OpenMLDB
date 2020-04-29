//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-16

#include "blobserver/blobserver_impl.h"

#include <gflags/gflags.h>

#include <utility>

#include "base/file_util.h"
#include "base/hash.h"
#include "base/status.h"
#include "base/strings.h"
#include "boost/bind.hpp"
#include "logging.h"  // NOLINT

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_string(hdd_root_path);

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::rtidb::base::ReturnCode;

namespace rtidb {
namespace blobserver {

static const uint32_t SEED = 0xe17a1465;

BlobServerImpl::BlobServerImpl()
    : spin_mutex_(),
      zk_client_(NULL),
      server_(NULL),
      keep_alive_pool_(1),
      task_pool_(2),
      object_stores_(),
      root_paths_() {}

BlobServerImpl::~BlobServerImpl() {
    if (zk_client_ != NULL) {
        delete zk_client_;
    }
}

bool BlobServerImpl::Init() {
    std::lock_guard<SpinMutex> lock(spin_mutex_);
    if (FLAGS_hdd_root_path.empty()) {
        PDLOG(WARNING, "hdd root path did not set");
        return false;
    }
    if (!FLAGS_zk_cluster.empty()) {
        std::string oss_zk_path = FLAGS_zk_root_path + "/ossnodes";
        zk_client_ =
            new ZkClient(FLAGS_zk_cluster, FLAGS_zk_session_timeout,
                         FLAGS_endpoint, FLAGS_zk_root_path, oss_zk_path);
        bool ok = zk_client_->Init();
        if (!ok) {
            PDLOG(WARNING, "fail to init zookeeper with cluster %s",
                  FLAGS_zk_cluster.c_str());
        }
    } else {
        PDLOG(INFO, "zk cluster disabled");
    }
    ::rtidb::base::SplitString(FLAGS_hdd_root_path, ",", root_paths_);
    for (auto &it : root_paths_) {
        bool ok = ::rtidb::base::MkdirRecur(it);
        if (!ok) {
            PDLOG(WARNING, "fail to creat dir %s", it.c_str());
            return false;
        }
    }
    return true;
}

bool BlobServerImpl::RegisterZK() {
    if (!FLAGS_zk_cluster.empty()) {
        if (!zk_client_->Register(true)) {
            PDLOG(WARNING, "fail to register tablet with endpoint %s",
                  FLAGS_endpoint.c_str());
            return false;
        }
        PDLOG(INFO, "oss with endpoint %s register to zk cluster %s ok",
              FLAGS_endpoint.c_str(), FLAGS_zk_cluster.c_str());
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
                                 GeneralResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
    const TableMeta &table_meta = request->table_meta();
    uint32_t tid = table_meta.tid();
    uint32_t pid = table_meta.pid();
    if (table_meta.table_type() != ::rtidb::type::kObjectStore) {
        response->set_code(ReturnCode::kTableMetaIsIllegal);
        response->set_msg("table type illegal");
        PDLOG(WARNING, "check table meta failed. tid[%u] pid[%u]", tid, pid);
        return;
    }
    std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
    if (store) {
        PDLOG(WARNING, "table with tid[%u] and pid[%u] exists", tid, pid);
        response->set_code(ReturnCode::kTableAlreadyExists);
        response->set_msg("table already exists");
    }
    std::string name = table_meta.name();
    if (root_paths_.size() < 1) {
        PDLOG(WARNING, "fail to find db root path tid[%u] pid[%u]", tid, pid);
        response->set_code(ReturnCode::kFailToGetDbRootPath);
        response->set_msg("failto find db root path");
        return;
    }
    std::string path;
    if (root_paths_.size() == 1) {
        path.assign(root_paths_[0]);
    } else {
        std::string key = std::to_string(tid) + std::to_string(pid);
        uint32_t index = ::rtidb::base::hash(key.c_str(), key.size(), SEED) %
                         root_paths_.size();
        path.assign(root_paths_[index]);
    }
    store = std::make_shared<ObjectStore>(table_meta, path);
    if (!store->Init()) {
        PDLOG(WARNING, "init object store failed");
        response->set_code(ReturnCode::kCreateTableFailed);
        response->set_msg("init object store failed");
    }
    PDLOG(INFO, "creat table tid[%u] pid[%u] success", tid, pid);
    std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
    object_stores_[tid].insert(std::make_pair(pid, store));
}

void BlobServerImpl::Get(RpcController *controller, const GetRequest *request,
                         GetResponse *response, Closure *done) {
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
    rtidb::base::Slice slice = store->Get(request->key());
    if (slice.size() > 0) {
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
    } else {
        response->set_code(ReturnCode::kKeyNotFound);
        response->set_msg("key not found");
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
    std::string key;
    if (request->key().empty()) {
        ok = store->Store(&key, request->data());
    } else {
        ok = store->Store(request->key(), request->data());
    }
    if (!ok) {
        response->set_code(ReturnCode::kPutFailed);
        response->set_msg("put failed");
        return;
    }
    if (request->key().empty()) {
        response->set_key(key);
    }
    response->set_code(ReturnCode::kOk);
    response->set_msg("ok");
}

void BlobServerImpl::Delete(RpcController *controller,
                            const DeleteRequest *request,
                            GeneralResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
    if (request->key().empty()) {
        response->set_code(ReturnCode::kKeyNotFound);
        response->set_msg("empty key");
    } else {
        uint32_t tid = request->tid();
        uint32_t pid = request->pid();
        std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
        if (!store) {
            PDLOG(WARNING, "table is not exist. tid[%u] pid[%u", tid, pid);
            response->set_code(ReturnCode::kTableIsNotExist);
            response->set_msg("table is not exist");
            return;
        }
        bool ok = store->Delete(request->key());
        if (!ok) {
            response->set_code(ReturnCode::kKeyNotFound);
            response->set_msg("key not found");
            return;
        }
        response->set_code(ReturnCode::kOk);
    }
}

void BlobServerImpl::LoadTable(RpcController *controller,
                               const LoadTableRequest *request,
                               GeneralResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
    std::shared_ptr<::rtidb::api::TaskInfo> task_ptr;
    if (request->has_task_info() && request->task_info().IsInitialized()) {
        // TODO(kongquan): process taskinfo
    }
    const TableMeta &table_meta = request->table_meta();
    uint32_t tid = table_meta.tid();
    uint32_t pid = table_meta.pid();
    std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
    if (store) {
        PDLOG(WARNING, "table with tid[%u] and pid[%u] exists", tid, pid);
        response->set_code(::rtidb::base::ReturnCode::kTableAlreadyExists);
        response->set_msg("table already exists");
        return;
    }
    PDLOG(INFO, "start creating table tid[%u] pid[%u]", tid, pid);
    if (root_paths_.size() < 1) {
        PDLOG(WARNING, "fail to find db root path tid[%u] pid[%u]", tid, pid);
        response->set_code(ReturnCode::kFailToGetDbRootPath);
        response->set_msg("failto find db root path");
        return;
    }
    std::string path;
    if (root_paths_.size() == 1) {
        path.assign(root_paths_[0]);
    } else {
        std::string key = std::to_string(tid) + std::to_string(pid);
        uint32_t index = ::rtidb::base::hash(key.c_str(), key.size(), SEED) %
                         root_paths_.size();
        path.assign(root_paths_[index]);
    }
    store = std::make_shared<ObjectStore>(table_meta, path);
    if (!store->Init()) {
        PDLOG(WARNING, "init object store failed");
        response->set_code(ReturnCode::kCreateTableFailed);
        response->set_msg("init object store failed");
    }
    std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
    object_stores_[tid].insert(std::make_pair(pid, store));
}

std::shared_ptr<ObjectStore> BlobServerImpl::GetStore(uint32_t tid,
                                                      uint32_t pid) {
    std::lock_guard<SpinMutex> lock_guard(spin_mutex_);
    return GetStoreUnLock(tid, pid);
}

std::shared_ptr<ObjectStore> BlobServerImpl::GetStoreUnLock(uint32_t tid,
                                                            uint32_t pid) {
    ObjectStores::iterator it = object_stores_.find(tid);
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
                               GeneralResponse *response, Closure *done) {
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
    } else {
        response->set_code(::rtidb::base::ReturnCode::kTableIsNotExist);
        response->set_msg("please provide tid and pid");
    }
}

void BlobServerImpl::DropTableInternal(uint32_t tid, uint32_t pid) {
    std::shared_ptr<ObjectStore> store = GetStore(tid, pid);
    if (!store) {
        PDLOG(WARNING, "tid[%u] pid[%u] does not exist", tid, pid);
        return;
    }

    std::lock_guard<SpinMutex> spin_lock(spin_mutex_);
    object_stores_[tid].erase(pid);
    if (object_stores_[tid].empty()) {
        object_stores_.erase(tid);
    }
}

}  // namespace blobserver
}  // namespace rtidb
