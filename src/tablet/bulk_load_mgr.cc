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

#include "tablet/bulk_load_mgr.h"

#include <memory>
#include <utility>

namespace openmldb::tablet {

bool BulkLoadMgr::AppendData(uint32_t tid, uint32_t pid, const ::openmldb::api::BulkLoadRequest* request,
                             const butil::IOBuf& data) {
    if (data.empty() || !request->has_part_id()) {
        LOG(ERROR) << "AppendData: data is empty or don't have part id";
        return false;
    }
    auto part_id = request->part_id();
    auto data_receiver = GetDataReceiver(tid, pid, part_id == 0);
    if (!data_receiver) {
        LOG(ERROR) << "AppendData: can't get data receiver for " << tid << "-" << pid << ", part id " << part_id;
        return false;
    }

    if (!data_receiver->AppendData(request, data)) {
        return false;
    }
    return true;
}

bool BulkLoadMgr::BulkLoad(const std::shared_ptr<storage::MemTable>& table,
                           const ::openmldb::api::BulkLoadRequest* request) {
    auto data_receiver = GetDataReceiver(table->GetId(), table->GetPid(), DO_NOT_CREATE);
    if (!data_receiver) {
        LOG(ERROR) << "BulkLoad: can't get data receiver for " << table->GetId() << "-" << table->GetPid();
        return false;
    }
    if (!data_receiver->BulkLoad(table, request)) {
        return false;
    }
    return true;
}

bool BulkLoadMgr::WriteBinlogToReplicator(uint32_t tid, uint32_t pid,
                                          const std::shared_ptr<replica::LogReplicator>& replicator,
                                          const ::openmldb::api::BulkLoadRequest* request) {
    auto data_receiver = GetDataReceiver(tid, pid, DO_NOT_CREATE);
    if (!data_receiver) {
        return false;
    }
    if (!data_receiver->WriteBinlogToReplicator(replicator, request)) {
        return false;
    }

    return true;
}

std::shared_ptr<DataReceiver> BulkLoadMgr::GetDataReceiver(uint32_t tid, uint32_t pid, bool create) {
    std::shared_ptr<DataReceiver> data_receiver = nullptr;
    do {
        std::unique_lock<std::mutex> ul(catalog_mu_);
        auto table_cat_iter = catalog_.find(tid);
        if (table_cat_iter == catalog_.end()) {
            if (!create) {
                break;
            }
            // tid-pid-DataReceiver
            data_receiver = std::make_shared<DataReceiver>(tid, pid);
            auto pid_cat = decltype(catalog_)::mapped_type();
            pid_cat[pid] = data_receiver;
            catalog_[tid] = pid_cat;
            break;
        }
        auto& pid_cat = table_cat_iter->second;
        auto iter = pid_cat.find(pid);
        if (iter == pid_cat.end()) {
            if (!create) {
                DLOG(INFO) << "pid" << pid << " not found";
                break;
            }
            data_receiver = std::make_shared<DataReceiver>(tid, pid);
            pid_cat[pid] = data_receiver;
            break;
        }

        // Catalog has the receiver for tid-pid, we treat it as error. // TODO(hw): or should treat it as covering?
        // If bulk load failed, plz recreate table and bulk load again.
        if (create) {
            LOG(WARNING) << "already has the receiver for " << tid << "-" << pid << ", but want to create a new one";
            break;
        }
        data_receiver = iter->second;
    } while (false);
    return data_receiver;
}

void BulkLoadMgr::RemoveReceiver(uint32_t tid, uint32_t pid, bool del_data_blocks) {
    std::unique_lock<std::mutex> ul(catalog_mu_);
    RemoveReceiverUnlock(tid, pid, del_data_blocks);
}

void BulkLoadMgr::RemoveReceiverUnlock(uint32_t tid, uint32_t pid, bool del_data_blocks) {
    auto table_cat_iter = catalog_.find(tid);
    if (table_cat_iter == catalog_.end()) {
        LOG(WARNING) << "not existed table, " << tid << "-" << pid;
        return;
    }
    auto& pid_cat = table_cat_iter->second;
    auto iter = pid_cat.find(pid);
    if (iter == pid_cat.end()) {
        LOG(WARNING) << "not existed partition, " << tid << "-" << pid;
        return;
    }

    iter->second->Close(del_data_blocks);
    pid_cat.erase(iter);
    LOG(INFO) << "data receiver for " << tid << "-" << pid << " removed " << (del_data_blocks ? "forcefully" : "");
}

void BulkLoadMgr::RemoveReceiverLater(const std::shared_ptr<storage::Table>& table) {
    std::unique_lock<std::mutex> ul(catalog_mu_);
    auto tid = table->GetId();
    auto pid = table->GetPid();
    to_cleanup_.emplace_back(tid, pid, table);
}

void BulkLoadMgr::ReceiverCleanup() {
    std::unique_lock<std::mutex> ul(catalog_mu_);
    for (auto iter = to_cleanup_.begin(); iter != to_cleanup_.end();) {
        if (!iter->ptr.expired()) {
            LOG(WARNING) << iter->tid << "-" << iter->pid
                         << " bulk load cleanup, but iter is holding by others too, can't do memory cleanup, try again";
            iter++;
        } else {
            // data receiver deletes all data blocks forcefully(cuz the ref in data blocks is mismatch with real
            // reference count)
            RemoveReceiverUnlock(iter->tid, iter->pid, true);
            iter = to_cleanup_.erase(iter);
        }
    }
}
}  // namespace openmldb::tablet
