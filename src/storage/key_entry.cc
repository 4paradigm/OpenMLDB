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

#include "base/glog_wrapper.h"
#include "storage/key_entry.h"
#include "storage/record.h"

namespace openmldb {
namespace storage {

void KeyEntry::Release(StatisticsInfo* statistics_info) {
    if (entries.IsEmpty()) {
        return;
    }
    std::unique_ptr<TimeEntries::Iterator> it(entries.NewIterator());
    it->SeekToFirst();
    if (!it->Valid()) {
        return;
    }
    uint64_t ts = it->GetKey();
    base::Node<uint64_t, DataBlock*>* node = entries.Split(ts);
    while (node) {
        if (node->GetValue()->dim_cnt_down > 1) {
            node->GetValue()->dim_cnt_down--;
        } else {
            DEBUGLOG("delele data block for key %lu", node->GetKey());
            statistics_info->record_byte_size += GetRecordSize(node->GetValue()->size);
            delete node->GetValue();
            statistics_info->record_cnt++;
        }
        statistics_info->idx_byte_size += GetRecordTsIdxSize(node->Height());
        auto tmp = node;
        node = node->GetNextNoBarrier(0);
        delete tmp;
    }
}

}  // namespace storage
}  // namespace openmldb
