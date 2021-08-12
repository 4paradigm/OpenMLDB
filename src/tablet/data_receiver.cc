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

#include "tablet/data_receiver.h"

#include "storage/segment.h"

namespace openmldb::tablet {
bool DataReceiver::DataAppend(const ::openmldb::api::BulkLoadRequest* request, const butil::IOBuf& data) {
    std::unique_lock<std::mutex> ul(mu_);

    if (!request->has_part_id() || !PartValidation(request->part_id())) {
        LOG(WARNING) << tid_ << "-" << pid_ << " data receiver received invalid part id, expect " << next_part_id_
                     << ", actual "
                     << (request->has_part_id() ? "no id" : std::to_string(request->part_id()));
        return false;
    }

    // We must copy data from IOBuf, cuz the rows have different TTLs, it's not a good idea to keep them together.
    butil::IOBufBytesIterator iter(data);
    auto last_block_size = data_blocks_.size();
    for (int i = 0; i < request->block_info_size(); ++i) {
        const auto& info = request->block_info(i);
        auto buf = new char[info.length()];  // TODO(hw): use pool
        iter.copy_and_forward(buf, info.length());
        data_blocks_.emplace_back(new storage::DataBlock(info.ref_cnt(), buf, info.length(), true));
    }
    if (iter.bytes_left() != 0) {
        LOG(ERROR) << tid_ << "-" << pid_ << " data and info mismatch, revert this part";
        // Not only ptr, DataBlock needs delete.
        for (auto block_iter = data_blocks_.begin() + last_block_size; block_iter != data_blocks_.end();) {
            // regardless of dim_cnt_down
            delete *block_iter;
            block_iter = data_blocks_.erase(block_iter);
        }
        return false;
    }

    DLOG(INFO) << "inserted into table(" << tid_ << "-" << pid_ << ") " << request->block_info_size()
               << " rows. Looking forward to part " << next_part_id_ << " or IndexRegion.";
    return true;
}

bool DataReceiver::PartValidation(int part_id) {
    if (part_id != next_part_id_) {
        LOG(WARNING) << tid_ << "-" << pid_ << " data receiver needs part " << next_part_id_ << ", but get part "
                     << part_id;
        return false;
    }
    next_part_id_++;
    return true;
}

bool DataReceiver::BulkLoad(std::shared_ptr<storage::MemTable> table, const ::openmldb::api::BulkLoadRequest* request) {
    std::unique_lock<std::mutex> ul(mu_);
    DLOG_ASSERT(tid_ == table->GetId() && pid_ == table->GetPid());

    if (!request->has_part_id() || !PartValidation(request->part_id())) {
        LOG(WARNING) << tid_ << "-" << pid_ << " data receiver received invalid part id, expect " << next_part_id_
                     << ", actual "
                     << (request->has_part_id() ? "no id" : std::to_string(request->part_id()));
        return false;
    }

    if (!table->BulkLoad(data_blocks_, request->index_region())) {
        LOG(ERROR) << "bulk load to mem table(" << tid_ << "-" << pid_ << ") failed.";
    }

    DLOG(INFO) << "bulk load to mem table(" << tid_ << "-" << pid_ << ") " << data_blocks_.size() << " rows.";
    return true;
}

bool DataReceiver::WriteBinlogToReplicator(
    std::shared_ptr<replica::LogReplicator> replicator,
    const ::google::protobuf::RepeatedPtrField<::openmldb::api::BulkLoadIndex>& indexes) {
    std::unique_lock<std::mutex> ul(mu_);

    for (int i = 0; i < indexes.size(); ++i) {
        const auto& inner_index = indexes.Get(i);
        for (int j = 0; j < inner_index.segment_size(); ++j) {
            const auto& segment_index = inner_index.segment(j);
            for (int key_idx = 0; key_idx < segment_index.key_entries_size(); ++key_idx) {
                const auto& key_entries = segment_index.key_entries(key_idx);
                const auto& pk = key_entries.key();
                for (int key_entry_idx = 0; key_entry_idx < key_entries.key_entry_size(); ++key_entry_idx) {
                    const auto& key_entry = key_entries.key_entry(key_entry_idx);
                    for (int time_idx = 0; time_idx < key_entry.time_entry_size(); ++time_idx) {
                        const auto& time_entry = key_entry.time_entry(time_idx);
                        auto* block =
                            time_entry.block_id() < data_blocks_.size() ? data_blocks_[time_entry.block_id()] : nullptr;
                        if (block == nullptr) {
                            // TODO(hw): valid?
                            continue;
                        }
                        ::openmldb::api::LogEntry entry;
                        entry.set_pk(pk);
                        entry.set_ts(time_entry.time());
                        entry.set_value(block->data, block->size);
                        entry.set_term(replicator->GetLeaderTerm());
                        replicator->AppendEntry(entry);
                    }
                }
            }
        }
    }
    // TODO(hw): after binlog, release data block ptr cache? or delete it when we destroy the whole data receiver?
    return true;
}
}  // namespace openmldb::tablet
