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

#ifndef SRC_TABLET_DATA_RECEIVER_H_
#define SRC_TABLET_DATA_RECEIVER_H_

#include <memory>
#include <vector>

#include "replica/log_replicator.h"
#include "storage/mem_table.h"

namespace openmldb::tablet {
// TODO(hw): Create a abstract receiver to manage received data. FileReceiver can inherited from it too.
class DataReceiver {
 public:
    //    DataReceiver() = default;
    DataReceiver(uint32_t tid, uint32_t pid) : tid_(tid), pid_(pid) {}

    // only one of the methods below executes at the time.
    bool DataAppend(const ::openmldb::api::BulkLoadRequest* request, const butil::IOBuf& data);
    bool BulkLoad(std::shared_ptr<storage::MemTable> table, const ::openmldb::api::BulkLoadRequest* request);
    bool WriteBinlogToReplicator(std::shared_ptr<replica::LogReplicator> replicator,
                                 const ::google::protobuf::RepeatedPtrField<::openmldb::api::BulkLoadIndex>& indexes);

 private:
    bool PartValidation(int part_id);

 private:
    // TODO(hw): init val?
    const uint32_t tid_{};
    const uint32_t pid_{};

    std::mutex mu_;
    int next_part_id_{0};
    std::vector<storage::DataBlock*> data_blocks_;
};

}  // namespace openmldb::tablet
#endif  // SRC_TABLET_DATA_RECEIVER_H_
