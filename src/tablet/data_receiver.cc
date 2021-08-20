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
bool DataReceiver::AppendData(const ::openmldb::api::BulkLoadRequest* request, const butil::IOBuf& data) {
    std::unique_lock<std::mutex> ul(mu_);
    if (!request->has_part_id() || !PartValidation(request->part_id())) {
        LOG(WARNING) << tid_ << "-" << pid_ << " data receiver received invalid part id, expect " << next_part_id_
                     << ", actual " << (request->has_part_id() ? "no id" : std::to_string(request->part_id()));
        return false;
    }

    // We must copy data from IOBuf, cuz the rows have different TTLs, it's not a good idea to keep them in a memory
    // block.
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
        for (auto i = last_block_size; i < data_blocks_.size(); ++i) {
            // regardless of dim_cnt_down
            delete data_blocks_[i];
        }
        // range erase
        data_blocks_.resize(last_block_size);
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

bool DataReceiver::BulkLoad(const std::shared_ptr<storage::MemTable>& table,
                            const ::openmldb::api::BulkLoadRequest* request) {
    std::unique_lock<std::mutex> ul(mu_);
    DLOG_ASSERT(tid_ == table->GetId() && pid_ == table->GetPid());

    if (!request->has_part_id() || !PartValidation(request->part_id())) {
        LOG(WARNING) << tid_ << "-" << pid_ << " data receiver received invalid part id, expect " << next_part_id_
                     << ", actual " << (request->has_part_id() ? "no id" : std::to_string(request->part_id()));
        return false;
    }

    if (!table->BulkLoad(data_blocks_, request->index_region())) {
        LOG(ERROR) << "bulk load to mem table(" << tid_ << "-" << pid_ << ") failed.";
        return false;
    }

    DLOG(INFO) << "bulk load to mem table(" << tid_ << "-" << pid_ << ") " << data_blocks_.size() << " rows.";
    return true;
}

bool DataReceiver::WriteBinlogToReplicator(const std::shared_ptr<replica::LogReplicator>& replicator,
                                           const ::openmldb::api::BulkLoadRequest* request) {
    std::unique_lock<std::mutex> ul(mu_);
    // Do not do PartValidation
    // TODO(hw): maybe binlog should hava the part id too?
    if (request->part_id() != next_part_id_ - 1) {
        LOG(WARNING)
            << "WriteBinlogToReplicator follows AppendData, the same request, the same part id, but cur part id"
            << next_part_id_ - 1 << ", request part id " << request->part_id();
        return false;
    }
    for (int i = 0; i < request->binlog_info_size(); ++i) {
        const auto& info = request->binlog_info(i);
        ::openmldb::api::LogEntry entry;
        auto* block = info.block_id() < data_blocks_.size() ? data_blocks_[info.block_id()] : nullptr;
        if (block == nullptr) {
            LOG(ERROR) << "binlog wants " << info.block_id() << ", but cached block size = " << data_blocks_.size();
            return false;
        }
        entry.set_value(block->data, block->size);
        entry.set_term(replicator->GetLeaderTerm());
        if (info.dimensions_size() > 0) {
            entry.mutable_dimensions()->CopyFrom(info.dimensions());
        }
        if (info.ts_dimensions_size() > 0) {
            entry.mutable_ts_dimensions()->CopyFrom(info.ts_dimensions());
        }
        entry.set_ts(info.time());
        replicator->AppendEntry(entry);
    }
    LOG(INFO) << "binlog write num " << request->binlog_info_size();
    return true;
}
}  // namespace openmldb::tablet
