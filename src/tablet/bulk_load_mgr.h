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

#ifndef SRC_TABLET_BULK_LOAD_MGR_H_
#define SRC_TABLET_BULK_LOAD_MGR_H_

#include <map>
#include <memory>

#include "replica/log_replicator.h"
#include "storage/mem_table.h"
#include "tablet/data_receiver.h"

namespace openmldb::tablet {
class BulkLoadMgr {
 public:
    bool AppendData(uint32_t tid, uint32_t pid, const ::openmldb::api::BulkLoadRequest* request,
                    const butil::IOBuf& data);
    bool WriteBinlogToReplicator(uint32_t tid, uint32_t pid, const std::shared_ptr<replica::LogReplicator>& replicator,
                                 const ::openmldb::api::BulkLoadRequest* request);

    bool BulkLoad(const std::shared_ptr<storage::MemTable>& table, const ::openmldb::api::BulkLoadRequest* request);

    void RemoveReceiver(uint32_t tid, uint32_t pid, bool del_data_blocks);

    std::shared_ptr<DataReceiver> GetDataReceiver(uint32_t tid, uint32_t pid, bool create);

    static const bool DO_NOT_CREATE = false;

    // All marked remove receivers are failed receivers, should delete data blocks forcefully in ReceiverCleanup()
    void RemoveReceiverLater(const std::shared_ptr<storage::Table>& table);

    void ReceiverCleanup();

 private:
    void RemoveReceiverUnlock(uint32_t tid, uint32_t pid, bool del_data_blocks);

 private:
    // RWLock is not easy when we're using two-level map catalog. Use unique lock for simplicity.
    std::mutex catalog_mu_;
    std::map<uint32_t, std::map<uint32_t, std::shared_ptr<DataReceiver>>> catalog_;
    struct CleanupInfo {
        uint32_t tid;
        uint32_t pid;
        std::weak_ptr<storage::Table> ptr;
        CleanupInfo(uint32_t tid, uint32_t pid, const std::shared_ptr<storage::Table> ptr)
            : tid(tid), pid(pid), ptr(ptr) {}
    };
    std::vector<CleanupInfo> to_cleanup_;
    // TODO(hw): support time measurement
};
}  // namespace openmldb::tablet
#endif  // SRC_TABLET_BULK_LOAD_MGR_H_
