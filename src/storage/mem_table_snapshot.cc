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

#include "storage/mem_table_snapshot.h"

#include <google/protobuf/io/zero_copy_stream_impl.h>
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>
#include <unistd.h>

#include <set>
#include <utility>

#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "base/hash.h"
#include "base/slice.h"
#include "base/strings.h"
#include "base/taskpool.hpp"
#include "boost/bind.hpp"
#include "codec/row_codec.h"
#include "common/thread_pool.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "log/log_reader.h"
#include "log/sequential_file.h"
#include "proto/tablet.pb.h"

DECLARE_uint64(gc_on_table_recover_count);
DECLARE_int32(binlog_name_length);
DECLARE_uint32(make_snapshot_max_deleted_keys);
DECLARE_uint32(load_table_batch);
DECLARE_uint32(load_table_thread_num);
DECLARE_uint32(load_table_queue_size);
DECLARE_string(snapshot_compression);

namespace openmldb {
namespace storage {

constexpr const char* SNAPSHOT_SUBFIX = ".sdb";
constexpr uint32_t KEY_NUM_DISPLAY = 1000000;
constexpr const char* MANIFEST = "MANIFEST";

bool TableIndexInfo::Init() {
    for (int32_t i = 0; i < table_meta_.column_desc_size(); i++) {
        column_idx_map_.emplace(table_meta_.column_desc(i).name(), i);
    }
    uint32_t base_size = table_meta_.column_desc_size();
    for (int32_t i = 0; i < table_meta_.added_column_desc_size(); ++i) {
        column_idx_map_.emplace(table_meta_.added_column_desc(i).name(), i + base_size);
    }
    std::set<uint32_t> index_col_set;
    std::set<uint32_t> add_index_col_set;
    uint32_t add_index_cnt = 0;
    for (int32_t i = 0; i < table_meta_.column_key_size(); i++) {
        const auto& ck = table_meta_.column_key(i);
        if (ck.flag()) {
            continue;
        }
        bool is_add_index = false;
        for (const auto& add_ck : add_indexs_) {
            if (ck.index_name() == add_ck.index_name()) {
                is_add_index = true;
                add_index_cnt++;
                break;
            }
        }
        std::vector<uint32_t> cols;
        for (const auto& name : ck.col_name()) {
            auto iter = column_idx_map_.find(name);
            if (iter != column_idx_map_.end()) {
                cols.push_back(iter->second);
                index_col_set.insert(iter->second);
                if (is_add_index) {
                    add_index_col_set.insert(iter->second);
                }
            } else {
                PDLOG(WARNING, "fail to find column_desc %s", name.c_str());
                return false;
            }
        }
        index_cols_map_.emplace(i, std::move(cols));
    }
    if (add_index_cnt != add_indexs_.size()) {
        return false;
    }
    for (auto idx : index_col_set) {
        all_index_cols_.push_back(idx);
    }
    for (auto idx : add_index_col_set) {
        add_index_cols_.push_back(idx);
    }
    return true;
}

bool TableIndexInfo::HasIndex(uint32_t idx) {
    return index_cols_map_.find(idx) != index_cols_map_.end();
}

const std::vector<uint32_t>& TableIndexInfo::GetIndexCols(uint32_t idx) {
    return index_cols_map_[idx];
}

MemTableSnapshot::MemTableSnapshot(uint32_t tid, uint32_t pid, LogParts* log_part, const std::string& db_root_path)
    : Snapshot(tid, pid), log_part_(log_part), db_root_path_(db_root_path) {}

bool MemTableSnapshot::Init() {
    snapshot_path_ = absl::StrCat(db_root_path_, "/", tid_, "_", pid_, "/snapshot/");
    log_path_ = absl::StrCat(db_root_path_, "/", tid_, "_", pid_, "/binlog/");
    if (!::openmldb::base::MkdirRecur(snapshot_path_)) {
        PDLOG(WARNING, "fail to create db meta path %s", snapshot_path_.c_str());
        return false;
    }
    if (!::openmldb::base::MkdirRecur(log_path_)) {
        PDLOG(WARNING, "fail to create db meta path %s", log_path_.c_str());
        return false;
    }
    return true;
}

bool MemTableSnapshot::Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) {
    ::openmldb::api::Manifest manifest;
    manifest.set_offset(0);
    int ret = GetLocalManifest(snapshot_path_ + MANIFEST, manifest);
    if (ret == -1) {
        return false;
    }
    if (ret == 0) {
        RecoverFromSnapshot(manifest.name(), manifest.count(), table);
        latest_offset = manifest.offset();
        offset_ = latest_offset;
    }
    return true;
}

void MemTableSnapshot::RecoverFromSnapshot(const std::string& snapshot_name, uint64_t expect_cnt,
                                           std::shared_ptr<Table> table) {
    std::string full_path = snapshot_path_ + "/" + snapshot_name;
    std::atomic<uint64_t> g_succ_cnt(0);
    std::atomic<uint64_t> g_failed_cnt(0);
    RecoverSingleSnapshot(full_path, table, &g_succ_cnt, &g_failed_cnt);
    PDLOG(INFO, "[Recover] progress done stat: success count %lu, failed count %lu",
          g_succ_cnt.load(std::memory_order_relaxed), g_failed_cnt.load(std::memory_order_relaxed));
    if (g_succ_cnt.load(std::memory_order_relaxed) != expect_cnt) {
        PDLOG(WARNING, "snapshot %s , expect cnt %lu but succ_cnt %lu", snapshot_name.c_str(), expect_cnt,
              g_succ_cnt.load(std::memory_order_relaxed));
    }
}

void MemTableSnapshot::RecoverSingleSnapshot(const std::string& path, std::shared_ptr<Table> table,
                                             std::atomic<uint64_t>* g_succ_cnt, std::atomic<uint64_t>* g_failed_cnt) {
    ::openmldb::base::TaskPool load_pool_(FLAGS_load_table_thread_num, FLAGS_load_table_batch);
    std::atomic<uint64_t> succ_cnt, failed_cnt;
    succ_cnt = failed_cnt = 0;

    do {
        if (table == NULL) {
            PDLOG(WARNING, "table input is NULL");
            break;
        }
        FILE* fd = fopen(path.c_str(), "rb");
        if (fd == NULL) {
            PDLOG(WARNING, "fail to open path %s for error %s", path.c_str(), strerror(errno));
            break;
        }
        bool compressed = IsCompressed(path);
        ::openmldb::log::SequentialFile* seq_file = ::openmldb::log::NewSeqFile(path, fd);
        ::openmldb::log::Reader reader(seq_file, NULL, false, 0, compressed);
        std::string buffer;
        // second
        uint64_t consumed = ::baidu::common::timer::now_time();
        std::vector<std::string*> recordPtr;
        recordPtr.reserve(FLAGS_load_table_batch);

        while (true) {
            buffer.clear();
            ::openmldb::base::Slice record;
            ::openmldb::log::Status status = reader.ReadRecord(&record, &buffer);
            if (status.IsWaitRecord() || status.IsEof()) {
                consumed = ::baidu::common::timer::now_time() - consumed;
                PDLOG(INFO,
                      "read path %s for table tid %u pid %u completed, "
                      "succ_cnt %lu, failed_cnt %lu, consumed %us",
                      path.c_str(), tid_, pid_, succ_cnt.load(std::memory_order_relaxed),
                      failed_cnt.load(std::memory_order_relaxed), consumed);
                break;
            }

            if (!status.ok()) {
                PDLOG(WARNING, "fail to read record for tid %u, pid %u with error %s", tid_, pid_,
                      status.ToString().c_str());
                failed_cnt.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            std::string* sp = new std::string(record.data(), record.size());
            recordPtr.push_back(sp);
            if (recordPtr.size() >= FLAGS_load_table_batch) {
                load_pool_.AddTask(
                    boost::bind(&MemTableSnapshot::Put, this, path, table, recordPtr, &succ_cnt, &failed_cnt));
                recordPtr.clear();
            }
        }
        if (recordPtr.size() > 0) {
            load_pool_.AddTask(
                boost::bind(&MemTableSnapshot::Put, this, path, table, recordPtr, &succ_cnt, &failed_cnt));
        }
        // will close the fd atomic
        delete seq_file;
        if (g_succ_cnt) {
            g_succ_cnt->fetch_add(succ_cnt, std::memory_order_relaxed);
        }
        if (g_failed_cnt) {
            g_failed_cnt->fetch_add(failed_cnt, std::memory_order_relaxed);
        }
    } while (false);
    load_pool_.Stop();
}

void MemTableSnapshot::Put(std::string& path, std::shared_ptr<Table>& table, std::vector<std::string*> recordPtr,
                           std::atomic<uint64_t>* succ_cnt, std::atomic<uint64_t>* failed_cnt) {
    ::openmldb::api::LogEntry entry;
    for (auto it = recordPtr.cbegin(); it != recordPtr.cend(); it++) {
        bool ok = entry.ParseFromString(**it);
        if (!ok) {
            failed_cnt->fetch_add(1, std::memory_order_relaxed);
            delete *it;
            continue;
        }
        auto scount = succ_cnt->fetch_add(1, std::memory_order_relaxed);
        if (scount % 100000 == 0) {
            PDLOG(INFO, "load snapshot %s with succ_cnt %lu, failed_cnt %lu", path.c_str(), scount,
                  failed_cnt->load(std::memory_order_relaxed));
        }
        table->Put(entry);
        delete *it;
    }
}

int MemTableSnapshot::TTLSnapshot(std::shared_ptr<Table> table, const ::openmldb::api::Manifest& manifest,
                                  WriteHandle* wh, uint64_t& count, uint64_t& expired_key_num,
                                  uint64_t& deleted_key_num) {
    std::string full_path = snapshot_path_ + manifest.name();
    FILE* fd = fopen(full_path.c_str(), "rb");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to open path %s for error %s", full_path.c_str(), strerror(errno));
        return -1;
    }
    bool compressed = IsCompressed(full_path);
    ::openmldb::log::SequentialFile* seq_file = ::openmldb::log::NewSeqFile(manifest.name(), fd);
    ::openmldb::log::Reader reader(seq_file, NULL, false, 0, compressed);

    std::string buffer;
    std::string tmp_buf;
    ::openmldb::api::LogEntry entry;
    bool has_error = false;
    std::set<uint32_t> deleted_index;
    for (const auto& it : table->GetAllIndex()) {
        if (it->GetStatus() != ::openmldb::storage::IndexStatus::kReady) {
            deleted_index.insert(it->GetId());
        }
    }
    while (true) {
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = reader.ReadRecord(&record, &buffer);
        if (status.IsEof()) {
            break;
        }
        if (!status.ok()) {
            PDLOG(WARNING, "fail to read record for tid %u, pid %u with error %s", tid_, pid_,
                  status.ToString().c_str());
            has_error = true;
            break;
        }
        if (!entry.ParseFromString(record.ToString())) {
            PDLOG(WARNING, "fail parse record for tid %u, pid %u with value %s", tid_, pid_,
                  ::openmldb::base::DebugString(record.ToString()).c_str());
            has_error = true;
            break;
        }
        int ret = RemoveDeletedKey(entry, deleted_index, &tmp_buf);
        if (ret == 1) {
            deleted_key_num++;
            continue;
        } else if (ret == 2) {
            record.reset(tmp_buf.data(), tmp_buf.size());
        }
        if (table->IsExpire(entry)) {
            expired_key_num++;
            continue;
        }
        status = wh->Write(record);
        if (!status.ok()) {
            PDLOG(WARNING, "fail to write snapshot. status[%s]", status.ToString().c_str());
            has_error = true;
            break;
        }
        if ((count + expired_key_num + deleted_key_num) % KEY_NUM_DISPLAY == 0) {
            PDLOG(INFO, "tackled key num[%lu] total[%lu]", count + expired_key_num, manifest.count());
        }
        count++;
    }
    delete seq_file;
    if (expired_key_num + count + deleted_key_num != manifest.count()) {
        PDLOG(WARNING,
              "key num not match! total key num[%lu] load key num[%lu] ttl key "
              "num[%lu]",
              manifest.count(), count, expired_key_num);
        has_error = true;
    }
    if (has_error) {
        return -1;
    }
    PDLOG(INFO, "load snapshot success. load key num[%lu] ttl key num[%lu]", count, expired_key_num);
    return 0;
}

uint64_t MemTableSnapshot::CollectDeletedKey(uint64_t end_offset) {
    deleted_keys_.clear();
    ::openmldb::log::LogReader log_reader(log_part_, log_path_, false);
    log_reader.SetOffset(offset_);
    uint64_t cur_offset = offset_;
    std::string buffer;
    while (true) {
        if (deleted_keys_.size() >= FLAGS_make_snapshot_max_deleted_keys) {
            PDLOG(WARNING, "deleted_keys map size reach the make_snapshot_max_deleted_keys %u, tid %u pid %u",
                  FLAGS_make_snapshot_max_deleted_keys, tid_, pid_);
            break;
        }
        if (end_offset > 0 && cur_offset >= end_offset) {
            return cur_offset;
        }
        buffer.clear();
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::openmldb::api::LogEntry entry;
            if (!entry.ParseFromString(record.ToString())) {
                PDLOG(WARNING, "fail to parse LogEntry. record[%s] size[%ld]",
                      ::openmldb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                break;
            }
            if (entry.log_index() <= cur_offset) {
                continue;
            }
            if (cur_offset + 1 != entry.log_index()) {
                PDLOG(WARNING, "log missing expect offset %lu but %ld. tid %u pid %u",
                        cur_offset + 1, entry.log_index(), tid_, pid_);
                continue;
            }
            cur_offset = entry.log_index();
            if (entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete) {
                if (entry.dimensions_size() == 0) {
                    PDLOG(WARNING, "no dimesion. tid %u pid %u offset %lu", tid_, pid_, cur_offset);
                    continue;
                }
                std::string combined_key = absl::StrCat(entry.dimensions(0).key(), "|", entry.dimensions(0).idx());
                DEBUGLOG("insert key %s offset %lu. tid %u pid %u", combined_key.c_str(), cur_offset, tid_, pid_);
                deleted_keys_.insert_or_assign(std::move(combined_key), cur_offset);
            }
        } else if (status.IsEof()) {
            continue;
        } else if (status.IsWaitRecord()) {
            int end_log_index = log_reader.GetEndLogIndex();
            int cur_log_index = log_reader.GetLogIndex();
            // judge end_log_index greater than cur_log_index
            if (end_log_index >= 0 && end_log_index > cur_log_index) {
                log_reader.RollRLogFile();
                PDLOG(WARNING, "read new binlog file. tid[%u] pid[%u] cur_log_index[%d] "
                      "end_log_index[%d] cur_offset[%lu]",
                      tid_, pid_, cur_log_index, end_log_index, cur_offset);
                continue;
            }
            DEBUGLOG("has read all record!");
            break;
        } else {
            PDLOG(WARNING, "fail to get record. status is %s", status.ToString().c_str());
            break;
        }
    }
    return cur_offset;
}

int MemTableSnapshot::MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset, uint64_t end_offset,
                                   uint64_t term) {
    if (making_snapshot_.load(std::memory_order_acquire)) {
        PDLOG(INFO, "snapshot is doing now!");
        return 0;
    }
    if (end_offset > 0 && end_offset <= offset_) {
        PDLOG(WARNING, "end_offset %lu less than or equal offset_ %lu, do nothing", end_offset, offset_);
        return -1;
    }
    making_snapshot_.store(true, std::memory_order_release);
    std::string now_time = ::openmldb::base::GetNowTime();
    MemSnapshotMeta snapshot_meta(GenSnapshotName(), snapshot_path_, FLAGS_snapshot_compression);
    auto wh = ::openmldb::log::CreateWriteHandle(FLAGS_snapshot_compression,
            snapshot_meta.snapshot_name, snapshot_meta.tmp_file_path);
    if (!wh) {
        PDLOG(WARNING, "fail to create file %s", snapshot_meta.tmp_file_path.c_str());
        making_snapshot_.store(false, std::memory_order_release);
        return -1;
    }
    uint64_t collected_offset = CollectDeletedKey(end_offset);
    uint64_t start_time = ::baidu::common::timer::now_time();
    ::openmldb::api::Manifest manifest;
    bool has_error = false;
    snapshot_meta.term = term;
    int result = GetLocalManifest(snapshot_path_ + MANIFEST, manifest);
    if (result == 0) {
        // filter old snapshot
        if (TTLSnapshot(table, manifest, wh.get(), snapshot_meta.count,
                    snapshot_meta.expired_key_num, snapshot_meta.deleted_key_num) < 0) {
            has_error = true;
        }
        snapshot_meta.term = manifest.term();
        DEBUGLOG("old manifest term is %lu", snapshot_meta.term);
    } else if (result < 0) {
        // parse manifest error
        has_error = true;
    }

    // get deleted index
    std::set<uint32_t> deleted_index;
    for (const auto& it : table->GetAllIndex()) {
        if (it->GetStatus() == ::openmldb::storage::IndexStatus::kDeleted) {
            deleted_index.insert(it->GetId());
        }
    }
    ::openmldb::log::LogReader log_reader(log_part_, log_path_, false);
    log_reader.SetOffset(offset_);
    uint64_t cur_offset = offset_;
    std::string buffer;
    std::string tmp_buf;
    while (!has_error && cur_offset < collected_offset) {
        buffer.clear();
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::openmldb::api::LogEntry entry;
            if (!entry.ParseFromString(record.ToString())) {
                PDLOG(WARNING, "fail to parse LogEntry. record[%s] size[%ld]",
                      ::openmldb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                has_error = true;
                break;
            }
            if (entry.log_index() <= cur_offset) {
                continue;
            }
            if (cur_offset + 1 != entry.log_index()) {
                PDLOG(WARNING, "log missing expect offset %lu but %ld. tid %u pid %u",
                        cur_offset + 1, entry.log_index(), tid_, pid_);
                continue;
            }
            cur_offset = entry.log_index();
            if (entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete) {
                continue;
            }
            if (entry.has_term()) {
                snapshot_meta.term = entry.term();
            }
            int ret = RemoveDeletedKey(entry, deleted_index, &tmp_buf);
            if (ret == 1) {
                snapshot_meta.deleted_key_num++;
                continue;
            } else if (ret == 2) {
                record.reset(tmp_buf.data(), tmp_buf.size());
            }
            if (table->IsExpire(entry)) {
                snapshot_meta.expired_key_num++;
                continue;
            }
            ::openmldb::log::Status status = wh->Write(record);
            if (!status.ok()) {
                PDLOG(WARNING, "fail to write snapshot. path[%s] status[%s]",
                        snapshot_meta.tmp_file_path.c_str(), status.ToString().c_str());
                has_error = true;
                break;
            }
            snapshot_meta.count++;
            if ((snapshot_meta.count + snapshot_meta.expired_key_num + snapshot_meta.deleted_key_num)
                    % KEY_NUM_DISPLAY == 0) {
                PDLOG(INFO, "has write key num[%lu] expired key num[%lu]",
                        snapshot_meta.count, snapshot_meta.expired_key_num);
            }
        } else if (status.IsEof()) {
            continue;
        } else if (status.IsWaitRecord()) {
            int end_log_index = log_reader.GetEndLogIndex();
            int cur_log_index = log_reader.GetLogIndex();
            // judge end_log_index greater than cur_log_index
            if (end_log_index >= 0 && end_log_index > cur_log_index) {
                log_reader.RollRLogFile();
                PDLOG(WARNING, "read new binlog file. tid[%u] pid[%u] cur_log_index[%d] "
                        "end_log_index[%d] cur_offset[%lu]",
                        tid_, pid_, cur_log_index, end_log_index, cur_offset);
                continue;
            }
            DEBUGLOG("has read all record!");
            break;
        } else {
            PDLOG(WARNING, "fail to get record. status is %s", status.ToString().c_str());
            has_error = true;
            break;
        }
    }
    deleted_keys_.clear();
    wh->EndLog();
    wh.reset();
    int ret = 0;
    if (has_error) {
        unlink(snapshot_meta.tmp_file_path.c_str());
        ret = -1;
    } else {
        snapshot_meta.offset = cur_offset;
        uint64_t old_offset = offset_;
        auto status = WriteSnapshot(snapshot_meta, manifest);
        if (!status.OK()) {
            ret = -1;
            PDLOG(WARNING, "write snapshot failed. tid %u pid %u msg is %s ", tid_, pid_, status.GetMsg().c_str());
        }
        uint64_t consumed = ::baidu::common::timer::now_time() - start_time;
        PDLOG(INFO, "make snapshot[%s] success. update offset from %lu to %lu."
              "use %lu second. write key %lu expired key %lu deleted key %lu",
              snapshot_meta.snapshot_name.c_str(), old_offset, snapshot_meta.offset, consumed,
              snapshot_meta.count, snapshot_meta.expired_key_num, snapshot_meta.deleted_key_num);
        out_offset = snapshot_meta.offset;
    }
    making_snapshot_.store(false, std::memory_order_release);
    return ret;
}

int MemTableSnapshot::RemoveDeletedKey(const ::openmldb::api::LogEntry& entry, const std::set<uint32_t>& deleted_index,
                                       std::string* buffer) {
    uint64_t cur_offset = entry.log_index();
    if (entry.dimensions_size() == 0) {
        std::string combined_key = absl::StrCat(entry.pk(), "|0");
        auto iter = deleted_keys_.find(combined_key);
        if (iter != deleted_keys_.end() && cur_offset <= iter->second) {
            DEBUGLOG("delete key %s  offset %lu", entry.pk().c_str(), entry.log_index());
            return 1;
        }
    } else {
        std::set<int> deleted_pos_set;
        for (int pos = 0; pos < entry.dimensions_size(); pos++) {
            std::string combined_key = absl::StrCat(entry.dimensions(pos).key(), "|", entry.dimensions(pos).idx());
            auto iter = deleted_keys_.find(combined_key);
            if ((iter != deleted_keys_.end() && cur_offset <= iter->second) ||
                deleted_index.count(entry.dimensions(pos).idx())) {
                deleted_pos_set.insert(pos);
            }
        }
        if (!deleted_pos_set.empty()) {
            if (static_cast<int>(deleted_pos_set.size()) == entry.dimensions_size()) {
                return 1;
            } else {
                ::openmldb::api::LogEntry tmp_entry(entry);
                tmp_entry.clear_dimensions();
                for (int pos = 0; pos < entry.dimensions_size(); pos++) {
                    if (deleted_pos_set.find(pos) == deleted_pos_set.end()) {
                        ::openmldb::api::Dimension* dimension = tmp_entry.add_dimensions();
                        dimension->CopyFrom(entry.dimensions(pos));
                    }
                }
                buffer->clear();
                tmp_entry.SerializeToString(buffer);
                return 2;
            }
        }
    }
    return 0;
}

base::Status MemTableSnapshot::GetAllDecoder(std::shared_ptr<Table> table,
        std::map<uint8_t, codec::RowView>* decoder_map) {
    if (decoder_map == nullptr) {
        return base::Status(base::ReturnCode::kError, "null ptr");
    }
    auto schema_map = table->GetAllVersionSchema();
    if (schema_map.empty()) {
        return base::Status(base::ReturnCode::kError, "schema map is empty");
    }
    decoder_map->clear();
    for (const auto& kv : schema_map) {
        if (kv.second) {
            decoder_map->emplace(kv.first, codec::RowView(*kv.second));
        }
    }
    return {};
}

/**
 * return code:
 * -1 : error
 * 0 : not delete
 * 1 : delete all key
 * 2 : delete some key
*/
int MemTableSnapshot::CheckDeleteAndUpdate(std::shared_ptr<Table> table, openmldb::api::LogEntry* entry) {
    if (entry == nullptr) {
        return -1;
    }
    if (deleted_keys_.empty()) {
        return 0;
    }
    // deleted key
    std::set<int> deleted_pos_set;
    for (int pos = 0; pos < entry->dimensions_size(); pos++) {
        std::string combined_key = absl::StrCat(entry->dimensions(pos).key(), "|", entry->dimensions(pos).idx());
        if (deleted_keys_.find(combined_key) != deleted_keys_.end() ||
            !table->GetIndex(entry->dimensions(pos).idx())->IsReady()) {
            deleted_pos_set.insert(pos);
        }
    }
    if (!deleted_pos_set.empty()) {
        if (static_cast<int>(deleted_pos_set.size()) == entry->dimensions_size()) {
            return 1;
        } else {
            ::openmldb::api::LogEntry tmp_entry(*entry);
            entry->clear_dimensions();
            for (int pos = 0; pos < tmp_entry.dimensions_size(); pos++) {
                if (deleted_pos_set.find(pos) == deleted_pos_set.end()) {
                    ::openmldb::api::Dimension* dimension = entry->add_dimensions();
                    dimension->CopyFrom(tmp_entry.dimensions(pos));
                }
            }
            return 2;
        }
    }
    return 0;
}

base::Status MemTableSnapshot::GetIndexKey(std::shared_ptr<Table> table,
        const std::shared_ptr<IndexDef>& index, const base::Slice& data,
        std::map<uint8_t, codec::RowView>* decoder_map, std::string* index_key) {
    if (table == nullptr || decoder_map == nullptr || index_key == nullptr) {
        return base::Status(base::ReturnCode::kError, "null ptr");
    }
    const int8_t* raw = reinterpret_cast<const int8_t*>(data.data());
    uint8_t version = openmldb::codec::RowView::GetSchemaVersion(raw);
    auto schema = table->GetVersionSchema(version);
    auto it = decoder_map->find(version);
    if (it == decoder_map->end() || schema == nullptr) {
        return base::Status(base::ReturnCode::kError, "schema version does not exist");
    }
    index_key->clear();
    for (const auto& col : index->GetColumns()) {
        if ((int32_t)col.GetId() >= schema->size()) {
            return base::Status(base::ReturnCode::kError, "cannot found col");
        }
        std::string val;
        int ret = it->second.GetStrValue(raw, col.GetId(), &val);
        if (ret < 0) {
            return base::Status(base::ReturnCode::kError, "decode error");
        } else if (ret == 1) {
            val = ::openmldb::codec::NONETOKEN;
        }
        if (index_key->empty()) {
            index_key->swap(val);
        } else {
            *index_key += "|" + val;
        }
    }
    return {};
}

base::Status MemTableSnapshot::ExtractIndexFromSnapshot(std::shared_ptr<Table> table,
        const ::openmldb::api::Manifest& manifest, WriteHandle* wh,
        const std::vector<::openmldb::common::ColumnKey>& add_indexs, uint32_t partition_num,
        uint64_t* count, uint64_t* expired_key_num, uint64_t* deleted_key_num) {
    if (wh == nullptr || count == nullptr || expired_key_num == nullptr || deleted_key_num == nullptr) {
        return base::Status(base::ReturnCode::kError, "null ptr");
    }
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    std::map<uint8_t, codec::RowView> decoder_map;
    auto ret = GetAllDecoder(table, &decoder_map);
    if (!ret.OK()) {
        return ret;
    }
    std::vector<std::shared_ptr<IndexDef>> index_vec;
    for (const auto& index : add_indexs) {
        auto index_def = table->GetIndex(index.index_name());
        if (!index_def) {
            return base::Status(base::ReturnCode::kError, "fail to get index " + index.index_name());
        }
        index_vec.push_back(index_def);
    }
    std::string full_path = snapshot_path_ + manifest.name();
    FILE* fd = fopen(full_path.c_str(), "rb");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to open path %s for error %s", full_path.c_str(), strerror(errno));
        return base::Status(base::ReturnCode::kError, "fail to open file");
    }
    ::openmldb::log::SequentialFile* seq_file = ::openmldb::log::NewSeqFile(manifest.name(), fd);
    bool compressed = IsCompressed(full_path);
    ::openmldb::log::Reader reader(seq_file, NULL, false, 0, compressed);
    std::string buffer;
    ::openmldb::api::LogEntry entry;
    bool has_error = false;
    uint64_t extract_count = 0;
    uint64_t write_count = 0;
    DLOG(INFO) << "extract index data from snapshot";
    while (true) {
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = reader.ReadRecord(&record, &buffer);
        if (status.IsEof()) {
            break;
        }
        if (!status.ok()) {
            PDLOG(WARNING, "fail to read record for tid %u, pid %u with error %s",
                    tid_, pid_, status.ToString().c_str());
            has_error = true;
            break;
        }
        if (!entry.ParseFromString(record.ToString())) {
            PDLOG(WARNING, "fail parse record for tid %u, pid %u with value %s",
                    tid_, pid_, ::openmldb::base::DebugString(record.ToString()).c_str());
            has_error = true;
            break;
        }
        std::string tmp_buf;
        if (!deleted_keys_.empty()) {
            int check_ret = CheckDeleteAndUpdate(table, &entry);
            if (check_ret == 1) {
                (*deleted_key_num)++;
                continue;
            } else if (check_ret == 2) {
                entry.SerializeToString(&tmp_buf);
                record.reset(tmp_buf.data(), tmp_buf.size());
            }
        }
        // delete timeout key
        if (table->IsExpire(entry)) {
            (*expired_key_num)++;
            continue;
        }
        if (!(entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete)) {
            std::string buff;
            openmldb::base::Slice data;
            if (table->GetCompressType() == openmldb::type::kSnappy) {
                snappy::Uncompress(entry.value().data(), entry.value().size(), &buff);
                data.reset(buff.data(), buff.size());
            } else {
                data.reset(entry.value().data(), entry.value().size());
            }
            std::map<uint32_t, std::string> add_key_idx_map;
            for (const auto& index : index_vec) {
                std::string index_key;
                auto ret = GetIndexKey(table, index, data, &decoder_map, &index_key);
                if (ret.OK() && !index_key.empty()) {
                    uint32_t index_pid = ::openmldb::base::hash64(index_key) % partition_num;
                    if (index_pid == pid) {
                        add_key_idx_map.emplace(index->GetId(), index_key);
                    }
                }
            }
            if (!add_key_idx_map.empty()) {
                for (const auto& kv : add_key_idx_map) {
                    ::openmldb::api::Dimension* dim = entry.add_dimensions();
                    dim->set_idx(kv.first);
                    dim->set_key(kv.second);
                }
                entry.SerializeToString(&tmp_buf);
                record.reset(tmp_buf.data(), tmp_buf.size());
                entry.clear_dimensions();
                for (const auto& kv : add_key_idx_map) {
                    ::openmldb::api::Dimension* dim = entry.add_dimensions();
                    dim->set_idx(kv.first);
                    dim->set_key(kv.second);
                }
                table->Put(entry);
                extract_count++;
            }
        }
        status = wh->Write(record);
        if (!status.ok()) {
            PDLOG(WARNING, "fail to extract index from snapshot. status[%s] tid[%u] pid[%u]",
                  status.ToString().c_str(), tid, pid);
            has_error = true;
            break;
        }
        if ((*count + *expired_key_num + *deleted_key_num) % KEY_NUM_DISPLAY == 0) {
            PDLOG(INFO, "tackled key num[%lu] total[%lu] tid[%u] pid[%u]",
                    *count + *expired_key_num, manifest.count(), tid, pid);
        }
        (*count)++;
    }
    delete seq_file;
    if (*expired_key_num + write_count + *deleted_key_num != manifest.count()) {
        PDLOG(WARNING, "key num not match! total key[%lu] load key[%lu] ttl key[%lu] delete key [%lu], tid %u pid %u",
                manifest.count(), *count, *expired_key_num, *deleted_key_num, tid, pid);
        has_error = true;
    }
    if (has_error) {
        return base::Status(base::ReturnCode::kError, "extract error");
    }
    PDLOG(INFO, "extract index from snapshot success! extract count [%lu], tid %u pid %u", extract_count, tid, pid);
    return {};
}

int MemTableSnapshot::ExtractIndexFromSnapshot(std::shared_ptr<Table> table, const ::openmldb::api::Manifest& manifest,
                                               WriteHandle* wh, const ::openmldb::common::ColumnKey& column_key,
                                               uint32_t idx, uint32_t partition_num, uint32_t max_idx,
                                               const std::vector<uint32_t>& index_cols, uint64_t& count,
                                               uint64_t& expired_key_num, uint64_t& deleted_key_num) {
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    std::string full_path = snapshot_path_ + manifest.name();
    FILE* fd = fopen(full_path.c_str(), "rb");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to open path %s for error %s", full_path.c_str(), strerror(errno));
        return -1;
    }
    ::openmldb::log::SequentialFile* seq_file = ::openmldb::log::NewSeqFile(manifest.name(), fd);
    bool compressed = IsCompressed(full_path);
    ::openmldb::log::Reader reader(seq_file, NULL, false, 0, compressed);
    std::string buffer;
    ::openmldb::api::LogEntry entry;
    bool has_error = false;
    uint64_t extract_count = 0;
    uint64_t schame_size_less_count = 0;
    uint64_t other_error_count = 0;
    DLOG(INFO) << "extract index data from snapshot";
    while (true) {
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = reader.ReadRecord(&record, &buffer);
        if (status.IsEof()) {
            break;
        }
        if (!status.ok()) {
            PDLOG(WARNING, "fail to read record for tid %u, pid %u with error %s", tid_, pid_,
                  status.ToString().c_str());
            has_error = true;
            break;
        }
        if (!entry.ParseFromString(record.ToString())) {
            PDLOG(WARNING, "fail parse record for tid %u, pid %u with value %s", tid_, pid_,
                  ::openmldb::base::DebugString(record.ToString()).c_str());
            has_error = true;
            break;
        }
        // deleted key
        std::string tmp_buf;
        if (entry.dimensions_size() == 0) {
            std::string combined_key = entry.pk() + "|0";
            if (deleted_keys_.find(combined_key) != deleted_keys_.end()) {
                deleted_key_num++;
                continue;
            }
        } else {
            std::set<int> deleted_pos_set;
            for (int pos = 0; pos < entry.dimensions_size(); pos++) {
                std::string combined_key =
                    entry.dimensions(pos).key() + "|" + std::to_string(entry.dimensions(pos).idx());
                if (deleted_keys_.find(combined_key) != deleted_keys_.end() ||
                    !table->GetIndex(entry.dimensions(pos).idx())->IsReady()) {
                    deleted_pos_set.insert(pos);
                }
            }
            if (!deleted_pos_set.empty()) {
                if ((int)deleted_pos_set.size() ==  // NOLINT
                    entry.dimensions_size()) {
                    deleted_key_num++;
                    continue;
                } else {
                    ::openmldb::api::LogEntry tmp_entry(entry);
                    entry.clear_dimensions();
                    for (int pos = 0; pos < tmp_entry.dimensions_size(); pos++) {
                        if (deleted_pos_set.find(pos) == deleted_pos_set.end()) {
                            ::openmldb::api::Dimension* dimension = entry.add_dimensions();
                            dimension->CopyFrom(tmp_entry.dimensions(pos));
                        }
                    }
                    entry.SerializeToString(&tmp_buf);
                    record.reset(tmp_buf.data(), tmp_buf.size());
                }
            }
        }
        // delete timeout key
        if (table->IsExpire(entry)) {
            expired_key_num++;
            continue;
        }
        if (!(entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete)) {
            // new column_key
            std::vector<std::string> row;
            int ret = DecodeData(table, entry, max_idx, row);
            if (ret == 2) {
                count++;
                wh->Write(record);
                continue;
            } else if (ret != 0) {
                DLOG(INFO) << "skip current data";
                other_error_count++;
                continue;
            }
            std::string cur_key;
            for (uint32_t i : index_cols) {
                if (cur_key.empty()) {
                    cur_key = row[i];
                } else {
                    cur_key += "|" + row[i];
                }
            }
            if (cur_key.empty()) {
                other_error_count++;
                DLOG(INFO) << "skip empty key";
                continue;
            }
            uint32_t index_pid = ::openmldb::base::hash64(cur_key) % partition_num;
            // update entry and write entry into memory
            if (index_pid == pid) {
                if (entry.dimensions_size() == 1 && entry.dimensions(0).idx() == idx) {
                    other_error_count++;
                    DLOG(INFO) << "skip not default key " << cur_key;
                    continue;
                }
                ::openmldb::api::Dimension* dim = entry.add_dimensions();
                dim->set_key(cur_key);
                dim->set_idx(idx);
                entry.SerializeToString(&tmp_buf);
                record.reset(tmp_buf.data(), tmp_buf.size());
                entry.clear_dimensions();
                dim = entry.add_dimensions();
                dim->set_key(cur_key);
                dim->set_idx(idx);
                table->Put(entry);
                extract_count++;
            }
        }
        status = wh->Write(record);
        if (!status.ok()) {
            PDLOG(WARNING,
                  "fail to extract index from snapshot. status[%s] tid[%u] "
                  "pid[%u]",
                  status.ToString().c_str(), tid, pid);
            has_error = true;
            break;
        }
        if ((count + expired_key_num + deleted_key_num) % KEY_NUM_DISPLAY == 0) {
            PDLOG(INFO, "tackled key num[%lu] total[%lu] tid[%u] pid[%u]", count + expired_key_num, manifest.count(),
                  tid, pid);
        }
        count++;
    }
    delete seq_file;
    if (expired_key_num + count + deleted_key_num + schame_size_less_count + other_error_count != manifest.count()) {
        LOG(WARNING) << "key num not match ! total key num[" << manifest.count() << "] load key num[" << count
                     << "] ttl key num[" << expired_key_num << "] schema size less num[" << schame_size_less_count
                     << "] other error count[" << other_error_count << "]"
                     << " tid[" << tid << "] pid[" << pid << "]";
        has_error = true;
    }
    if (has_error) {
        return -1;
    }
    LOG(INFO) << "extract index from snapshot success. extract key num[" << extract_count << "] load key num[" << count
              << "] ttl key num[" << expired_key_num << "] schema size less num[" << schame_size_less_count
              << "] other error count[" << other_error_count << "]"
              << " tid[" << tid << "] pid[" << pid << "]";
    return 0;
}

std::string MemTableSnapshot::GenSnapshotName() {
    std::string now_time = ::openmldb::base::GetNowTime();
    std::string snapshot_name = now_time.substr(0, now_time.length() - 2) + ".sdb";
    if (FLAGS_snapshot_compression != "off") {
        snapshot_name.append(".");
        snapshot_name.append(FLAGS_snapshot_compression);
    }
    return snapshot_name;
}

base::Status MemTableSnapshot::ExtractIndexFromBinlog(std::shared_ptr<Table> table,
        WriteHandle* wh, const std::vector<::openmldb::common::ColumnKey>& add_indexs,
        uint64_t collected_offset, uint32_t partition_num, uint64_t* offset,
        uint64_t* last_term, uint64_t* count, uint64_t* expired_key_num, uint64_t* deleted_key_num) {
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    std::map<uint8_t, codec::RowView> decoder_map;
    auto ret = GetAllDecoder(table, &decoder_map);
    if (!ret.OK()) {
        return ret;
    }
    std::vector<std::shared_ptr<IndexDef>> index_vec;
    for (const auto& index : add_indexs) {
        auto index_def = table->GetIndex(index.index_name());
        if (!index_def) {
            return base::Status(base::ReturnCode::kError, "fail to get index " + index.index_name());
        }
        index_vec.push_back(index_def);
    }
    ::openmldb::log::LogReader log_reader(log_part_, log_path_, false);
    log_reader.SetOffset(offset_);
    *offset = offset_;
    std::string buffer;
    uint64_t extract_count = 0;
    DLOG(INFO) << "extract index data from binlog";
    while (*offset < collected_offset) {
        buffer.clear();
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::openmldb::api::LogEntry entry;
            if (!entry.ParseFromString(record.ToString())) {
                LOG(WARNING) << "fail to parse LogEntry. record " << openmldb::base::DebugString(record.ToString())
                             << " size " << record.ToString().size() << " tid " << tid << " pid " << pid;
                return base::Status(base::ReturnCode::kError, "parse error");
            }
            if (entry.log_index() <= *offset) {
                continue;
            }
            if (*offset + 1 != entry.log_index()) {
                LOG(WARNING) << "log missing expect offset " << *offset + 1 << " but " << entry.log_index()
                             << ". tid " << tid << " pid " << pid;
                continue;
            }
            *offset = entry.log_index();
            if (entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete) {
                continue;
            }
            if (entry.has_term()) {
                *last_term = entry.term();
            }
            std::string tmp_buf;
            if (!deleted_keys_.empty()) {
                int check_ret = CheckDeleteAndUpdate(table, &entry);
                if (check_ret == 1) {
                    (*deleted_key_num)++;
                    continue;
                } else if (check_ret == 2) {
                    entry.SerializeToString(&tmp_buf);
                    record.reset(tmp_buf.data(), tmp_buf.size());
                }
            }
            if (table->IsExpire(entry)) {
                (*expired_key_num)++;
                continue;
            }
            if (!(entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete)) {
                std::string buff;
                openmldb::base::Slice data;
                if (table->GetCompressType() == openmldb::type::kSnappy) {
                    snappy::Uncompress(entry.value().data(), entry.value().size(), &buff);
                    data.reset(buff.data(), buff.size());
                } else {
                    data.reset(entry.value().data(), entry.value().size());
                }
                std::map<uint32_t, std::string> add_key_idx_map;
                for (const auto& index : index_vec) {
                    std::string index_key;
                    auto ret = GetIndexKey(table, index, data, &decoder_map, &index_key);
                    if (ret.OK() && !index_key.empty()) {
                        uint32_t index_pid = ::openmldb::base::hash64(index_key) % partition_num;
                        if (index_pid == pid) {
                            add_key_idx_map.emplace(index->GetId(), index_key);
                        }
                    }
                }
                if (!add_key_idx_map.empty()) {
                    for (const auto& kv : add_key_idx_map) {
                        ::openmldb::api::Dimension* dim = entry.add_dimensions();
                        dim->set_idx(kv.first);
                        dim->set_key(kv.second);
                    }
                    entry.SerializeToString(&tmp_buf);
                    entry.clear_dimensions();
                    for (const auto& kv : add_key_idx_map) {
                        ::openmldb::api::Dimension* dim = entry.add_dimensions();
                        dim->set_idx(kv.first);
                        dim->set_key(kv.second);
                    }
                    table->Put(entry);
                    extract_count++;
                    record.reset(tmp_buf.data(), tmp_buf.size());
                }
            }
            ::openmldb::log::Status status = wh->Write(record);
            if (!status.ok()) {
                PDLOG(WARNING, "fail to write snapshot. tid[%u] pid[%u] status[%s]",
                        tid, pid, status.ToString().c_str());
                return base::Status(base::ReturnCode::kError, "fail to write snapshot");
            }
            (*count)++;
            if ((*count + *expired_key_num + *deleted_key_num) % KEY_NUM_DISPLAY == 0) {
                PDLOG(INFO, "has write key num[%lu] expired key num[%lu]", *count, *expired_key_num);
            }
        } else if (status.IsEof()) {
            continue;
        } else if (status.IsWaitRecord()) {
            int end_log_index = log_reader.GetEndLogIndex();
            int cur_log_index = log_reader.GetLogIndex();
            // judge end_log_index greater than cur_log_index
            if (end_log_index >= 0 && end_log_index > cur_log_index) {
                log_reader.RollRLogFile();
                PDLOG(WARNING, "read new binlog file. tid[%u] pid[%u] cur_log_index[%d] "
                      "end_log_index[%d] cur_offset[%lu]",
                      tid, pid, cur_log_index, end_log_index, *offset);
                continue;
            }
            DEBUGLOG("has read all record!");
            return {};
        } else {
            PDLOG(WARNING, "fail to get record. status is %s", status.ToString().c_str());
            return base::Status(base::ReturnCode::kError, "fail to get record");
        }
    }
    return {};
}

int MemTableSnapshot::ExtractIndexData(std::shared_ptr<Table> table,
        const std::vector<::openmldb::common::ColumnKey>& indexs,
        uint32_t partition_num, uint64_t* out_offset) {
    if (out_offset == NULL) {
        return -1;
    }
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    if (making_snapshot_.exchange(true, std::memory_order_consume)) {
        PDLOG(INFO, "snapshot is doing now. tid %u, pid %u", tid, pid);
        return -1;
    }
    std::string snapshot_name = GenSnapshotName();
    std::string snapshot_name_tmp = snapshot_name + ".tmp";
    std::string full_path = snapshot_path_ + snapshot_name;
    std::string tmp_file_path = snapshot_path_ + snapshot_name_tmp;
    FILE* fd = fopen(tmp_file_path.c_str(), "ab+");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to create file %s. tid %u, pid %u", tmp_file_path.c_str(), tid, pid);
        making_snapshot_.store(false, std::memory_order_release);
        return -1;
    }
    uint64_t collected_offset = CollectDeletedKey(0);
    uint64_t start_time = ::baidu::common::timer::now_time();
    WriteHandle* wh = new WriteHandle(FLAGS_snapshot_compression, snapshot_name_tmp, fd);
    ::openmldb::api::Manifest manifest;
    bool has_error = false;
    uint64_t write_count = 0;
    uint64_t expired_key_num = 0;
    uint64_t deleted_key_num = 0;
    uint64_t last_term = 0;

    int result = GetLocalManifest(snapshot_path_ + MANIFEST, manifest);
    if (result == 0) {
        DLOG(INFO) << "begin extract index data from snapshot";
        if (!ExtractIndexFromSnapshot(table, manifest, wh, indexs, partition_num,
                    &write_count, &expired_key_num, &deleted_key_num).OK()) {
            has_error = true;
        }
        last_term = manifest.term();
        DLOG(INFO) << "old manifest term is " << last_term;
    } else if (result < 0) {
        // parse manifest error
        has_error = true;
    }
    uint64_t cur_offset = offset_;
    if (!has_error) {
        auto ret = ExtractIndexFromBinlog(table, wh, indexs, collected_offset, partition_num,
                &cur_offset, &last_term, &write_count, &expired_key_num, &deleted_key_num);
        if (!ret.OK()) {
            LOG(WARNING) << ret.msg;
            has_error = true;
        }
    }

    if (wh != NULL) {
        wh->EndLog();
        delete wh;
        wh = NULL;
    }
    int ret = 0;
    if (has_error) {
        unlink(tmp_file_path.c_str());
        ret = -1;
    } else {
        if (rename(tmp_file_path.c_str(), full_path.c_str()) == 0) {
            if (GenManifest(snapshot_name, write_count, cur_offset, last_term) == 0) {
                // delete old snapshot
                if (manifest.has_name() && manifest.name() != snapshot_name) {
                    DEBUGLOG("old snapshot[%s] has deleted", manifest.name().c_str());
                    unlink((snapshot_path_ + manifest.name()).c_str());
                }
                uint64_t consumed = ::baidu::common::timer::now_time() - start_time;
                PDLOG(INFO,
                      "make snapshot[%s] success. update offset from %lu to %lu."
                      "use %lu second. write key %lu expired key %lu deleted key %lu",
                      snapshot_name.c_str(), offset_, cur_offset, consumed, write_count, expired_key_num,
                      deleted_key_num);
                offset_ = cur_offset;
                *out_offset = cur_offset;
            } else {
                PDLOG(WARNING, "GenManifest failed. delete snapshot file[%s]", full_path.c_str());
                unlink(full_path.c_str());
                ret = -1;
            }
        } else {
            PDLOG(WARNING, "rename[%s] failed", snapshot_name.c_str());
            unlink(tmp_file_path.c_str());
            ret = -1;
        }
    }
    deleted_keys_.clear();
    making_snapshot_.store(false, std::memory_order_release);
    return ret;
}

int MemTableSnapshot::ExtractIndexData(std::shared_ptr<Table> table, const ::openmldb::common::ColumnKey& column_key,
                                       uint32_t idx, uint32_t partition_num, uint64_t& out_offset) {
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    if (making_snapshot_.exchange(true, std::memory_order_consume)) {
        PDLOG(INFO, "snapshot is doing now. tid %u, pid %u", tid, pid);
        return -1;
    }
    std::string now_time = ::openmldb::base::GetNowTime();
    std::string snapshot_name = now_time.substr(0, now_time.length() - 2) + ".sdb";
    if (FLAGS_snapshot_compression != "off") {
        snapshot_name.append(".");
        snapshot_name.append(FLAGS_snapshot_compression);
    }
    std::string snapshot_name_tmp = snapshot_name + ".tmp";
    std::string full_path = snapshot_path_ + snapshot_name;
    std::string tmp_file_path = snapshot_path_ + snapshot_name_tmp;
    FILE* fd = fopen(tmp_file_path.c_str(), "ab+");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to create file %s. tid %u, pid %u", tmp_file_path.c_str(), tid, pid);
        making_snapshot_.store(false, std::memory_order_release);
        return -1;
    }
    uint64_t collected_offset = CollectDeletedKey(0);
    uint64_t start_time = ::baidu::common::timer::now_time();
    WriteHandle* wh = new WriteHandle(FLAGS_snapshot_compression, snapshot_name_tmp, fd);
    ::openmldb::api::Manifest manifest;
    bool has_error = false;
    uint64_t write_count = 0;
    uint64_t expired_key_num = 0;
    uint64_t deleted_key_num = 0;
    uint64_t last_term = 0;

    std::map<std::string, uint32_t> column_desc_map;
    auto table_meta = table->GetTableMeta();
    for (int32_t i = 0; i < table_meta->column_desc_size(); ++i) {
        column_desc_map.insert(std::make_pair(table_meta->column_desc(i).name(), i));
    }
    uint32_t base_size = table_meta->column_desc_size();
    for (int32_t i = 0; i < table_meta->added_column_desc_size(); ++i) {
        column_desc_map.insert(std::make_pair(table_meta->added_column_desc(i).name(), i + base_size));
    }
    std::vector<uint32_t> index_cols;
    uint32_t max_idx = 0;
    // get columns in new column_key
    for (const auto& name : column_key.col_name()) {
        if (column_desc_map.find(name) != column_desc_map.end()) {
            uint32_t idx = column_desc_map[name];
            index_cols.push_back(idx);
            if (idx > max_idx) {
                max_idx = idx;
            }
        } else {
            PDLOG(WARNING, "fail to find column_desc %s. tid %u, pid %u", name.c_str(), tid, pid);
            making_snapshot_.store(false, std::memory_order_release);
            return -1;
        }
    }

    int result = GetLocalManifest(snapshot_path_ + MANIFEST, manifest);
    if (result == 0) {
        DLOG(INFO) << "begin extract index data from snapshot";
        if (ExtractIndexFromSnapshot(table, manifest, wh, column_key, idx, partition_num, max_idx, index_cols,
                                     write_count, expired_key_num, deleted_key_num) < 0) {
            has_error = true;
        }
        last_term = manifest.term();
        DLOG(INFO) << "old manifest term is " << last_term;
    } else if (result < 0) {
        // parse manifest error
        has_error = true;
    }

    ::openmldb::log::LogReader log_reader(log_part_, log_path_, false);
    log_reader.SetOffset(offset_);
    uint64_t cur_offset = offset_;
    std::string buffer;
    uint64_t extract_count = 0;
    DLOG(INFO) << "extract index data from binlog";
    while (!has_error && cur_offset < collected_offset) {
        buffer.clear();
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::openmldb::api::LogEntry entry;
            if (!entry.ParseFromString(record.ToString())) {
                LOG(WARNING) << "fail to parse LogEntry. record " << openmldb::base::DebugString(record.ToString())
                             << " size " << record.ToString().size() << " tid " << tid << " pid " << pid;
                has_error = true;
                break;
            }
            if (entry.log_index() <= cur_offset) {
                continue;
            }
            if (cur_offset + 1 != entry.log_index()) {
                LOG(WARNING) << "log missing expect offset " << cur_offset + 1 << " but " << entry.log_index()
                             << ". tid " << tid << " pid " << pid;
                continue;
            }
            cur_offset = entry.log_index();
            if (entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete) {
                continue;
            }
            if (entry.has_term()) {
                last_term = entry.term();
            }
            std::string tmp_buf;
            if (entry.dimensions_size() == 0) {
                std::string combined_key = entry.pk() + "|0";
                auto iter = deleted_keys_.find(combined_key);
                if (iter != deleted_keys_.end() && cur_offset <= iter->second) {
                    DEBUGLOG("delete key %s  offset %lu", entry.pk().c_str(), entry.log_index());
                    deleted_key_num++;
                    continue;
                }
            } else {
                std::set<int> deleted_pos_set;
                for (int pos = 0; pos < entry.dimensions_size(); pos++) {
                    std::string combined_key =
                        entry.dimensions(pos).key() + "|" + std::to_string(entry.dimensions(pos).idx());
                    auto iter = deleted_keys_.find(combined_key);
                    if ((iter != deleted_keys_.end() && cur_offset <= iter->second) ||
                        !table->GetIndex(entry.dimensions(pos).idx())->IsReady()) {
                        deleted_pos_set.insert(pos);
                    }
                }
                if (!deleted_pos_set.empty()) {
                    if ((int)deleted_pos_set.size() == entry.dimensions_size()) {  // NOLINT
                        deleted_key_num++;
                        continue;
                    } else {
                        ::openmldb::api::LogEntry tmp_entry(entry);
                        entry.clear_dimensions();
                        for (int pos = 0; pos < tmp_entry.dimensions_size(); pos++) {
                            if (deleted_pos_set.find(pos) == deleted_pos_set.end()) {
                                ::openmldb::api::Dimension* dimension = entry.add_dimensions();
                                dimension->CopyFrom(tmp_entry.dimensions(pos));
                            }
                        }
                        entry.SerializeToString(&tmp_buf);
                        record.reset(tmp_buf.data(), tmp_buf.size());
                    }
                }
            }
            if (table->IsExpire(entry)) {
                expired_key_num++;
                continue;
            }
            if (!(entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete)) {
                // new column_key
                std::vector<std::string> row;
                int ret = DecodeData(table, entry, max_idx, row);
                if (ret == 2) {
                    wh->Write(record);
                    write_count++;
                    continue;
                } else if (ret != 0) {
                    DLOG(INFO) << "skip current data";
                    continue;
                }
                std::string cur_key;
                for (uint32_t i : index_cols) {
                    if (cur_key.empty()) {
                        cur_key = row[i];
                    } else {
                        cur_key += "|" + row[i];
                    }
                }
                if (cur_key.empty()) {
                    DLOG(INFO) << "skip empty key";
                    continue;
                }
                uint32_t index_pid = ::openmldb::base::hash64(cur_key) % partition_num;
                // update entry and write entry into memory
                if (index_pid == pid) {
                    if (entry.dimensions_size() == 1 && entry.dimensions(0).idx() == idx) {
                        DLOG(INFO) << "skip not default key " << cur_key;
                        continue;
                    }
                    ::openmldb::api::Dimension* dim = entry.add_dimensions();
                    dim->set_key(cur_key);
                    dim->set_idx(idx);
                    entry.SerializeToString(&tmp_buf);
                    record.reset(tmp_buf.data(), tmp_buf.size());
                    entry.clear_dimensions();
                    dim = entry.add_dimensions();
                    dim->set_key(cur_key);
                    dim->set_idx(idx);
                    table->Put(entry);
                    extract_count++;
                }
            }
            ::openmldb::log::Status status = wh->Write(record);
            if (!status.ok()) {
                PDLOG(WARNING, "fail to write snapshot. path[%s] status[%s]", tmp_file_path.c_str(),
                      status.ToString().c_str());
                has_error = true;
                break;
            }
            write_count++;
            if ((write_count + expired_key_num + deleted_key_num) % KEY_NUM_DISPLAY == 0) {
                PDLOG(INFO, "has write key num[%lu] expired key num[%lu]", write_count, expired_key_num);
            }
        } else if (status.IsEof()) {
            continue;
        } else if (status.IsWaitRecord()) {
            int end_log_index = log_reader.GetEndLogIndex();
            int cur_log_index = log_reader.GetLogIndex();
            // judge end_log_index greater than cur_log_index
            if (end_log_index >= 0 && end_log_index > cur_log_index) {
                log_reader.RollRLogFile();
                PDLOG(WARNING,
                      "read new binlog file. tid[%u] pid[%u] cur_log_index[%d] "
                      "end_log_index[%d] cur_offset[%lu]",
                      tid_, pid_, cur_log_index, end_log_index, cur_offset);
                continue;
            }
            DEBUGLOG("has read all record!");
            break;
        } else {
            PDLOG(WARNING, "fail to get record. status is %s", status.ToString().c_str());
            has_error = true;
            break;
        }
    }
    if (wh != NULL) {
        wh->EndLog();
        delete wh;
        wh = NULL;
    }
    int ret = 0;
    if (has_error) {
        unlink(tmp_file_path.c_str());
        ret = -1;
    } else {
        if (rename(tmp_file_path.c_str(), full_path.c_str()) == 0) {
            if (GenManifest(snapshot_name, write_count, cur_offset, last_term) == 0) {
                // delete old snapshot
                if (manifest.has_name() && manifest.name() != snapshot_name) {
                    DEBUGLOG("old snapshot[%s] has deleted", manifest.name().c_str());
                    unlink((snapshot_path_ + manifest.name()).c_str());
                }
                uint64_t consumed = ::baidu::common::timer::now_time() - start_time;
                PDLOG(INFO,
                      "make snapshot[%s] success. update offset from %lu to %lu."
                      "use %lu second. write key %lu expired key %lu deleted key "
                      "%lu",
                      snapshot_name.c_str(), offset_, cur_offset, consumed, write_count, expired_key_num,
                      deleted_key_num);
                offset_ = cur_offset;
                out_offset = cur_offset;
            } else {
                PDLOG(WARNING, "GenManifest failed. delete snapshot file[%s]", full_path.c_str());
                unlink(full_path.c_str());
                ret = -1;
            }
        } else {
            PDLOG(WARNING, "rename[%s] failed", snapshot_name.c_str());
            unlink(tmp_file_path.c_str());
            ret = -1;
        }
    }
    deleted_keys_.clear();
    making_snapshot_.store(false, std::memory_order_release);
    return ret;
}

bool MemTableSnapshot::PackNewIndexEntry(std::shared_ptr<Table> table,
                                         const std::vector<std::vector<uint32_t>>& index_cols, uint32_t max_idx,
                                         uint32_t idx, uint32_t partition_num, ::openmldb::api::LogEntry* entry,
                                         uint32_t* index_pid) {
    if (entry->dimensions_size() == 0) {
        std::string combined_key = entry->pk() + "|0";
        if (deleted_keys_.find(combined_key) != deleted_keys_.end()) {
            return false;
        }
    } else {
        bool has_main_index = false;
        for (int pos = 0; pos < entry->dimensions_size(); pos++) {
            if (entry->dimensions(pos).idx() == 0) {
                std::string combined_key = entry->dimensions(pos).key() + "|0";
                if (deleted_keys_.find(combined_key) == deleted_keys_.end()) {
                    has_main_index = true;
                }
                break;
            }
        }
        if (!has_main_index) {
            return false;
        }
    }
    std::vector<std::string> row;
    int ret = DecodeData(table, *entry, max_idx, row);
    if (ret != 0 && ret != 2) {
        DLOG(INFO) << "pack fail code is " << ret;
        return false;
    }
    std::string key;
    std::set<uint32_t> pid_set;
    for (uint32_t i = 0; i < index_cols.size(); ++i) {
        std::string cur_key;
        bool skip_calc = false;
        for (uint32_t j : index_cols[i]) {
            if (j >= row.size()) {
                skip_calc = true;
                break;
            }
            if (cur_key.empty()) {
                cur_key = row[j];
            } else {
                cur_key += "|" + row[j];
            }
        }
        if (skip_calc) {
            continue;
        }
        if (cur_key.empty()) {
            DLOG(INFO) << "key is empty";
            continue;
        }

        uint32_t pid = ::openmldb::base::hash64(cur_key) % partition_num;
        if (i < index_cols.size() - 1) {
            pid_set.insert(pid);
        } else {
            *index_pid = pid;
            key = cur_key;
        }
    }
    DLOG(INFO) << "pack end ";
    if (key.empty()) {
        DLOG(INFO) << "key is empty";
        return false;
    }
    if (pid_set.find(*index_pid) == pid_set.end()) {
        entry->clear_dimensions();
        ::openmldb::api::Dimension* dim = entry->add_dimensions();
        dim->set_key(key);
        dim->set_idx(idx);
        return true;
    }
    return false;
}

bool MemTableSnapshot::DumpSnapshotIndexData(std::shared_ptr<Table> table,
        const std::vector<std::vector<uint32_t>>& index_cols, uint32_t max_idx,
        uint32_t idx, const std::vector<std::shared_ptr<::openmldb::log::WriteHandle>>& whs,
        uint64_t* snapshot_offset) {
    uint32_t partition_num = whs.size();
    ::openmldb::api::Manifest manifest;
    manifest.set_offset(0);
    int ret = GetLocalManifest(snapshot_path_ + MANIFEST, manifest);
    if (ret == -1) {
        return false;
    }
    *snapshot_offset = manifest.offset();
    std::string path = snapshot_path_ + "/" + manifest.name();
    uint64_t succ_cnt = 0;
    uint64_t failed_cnt = 0;
    FILE* fd = fopen(path.c_str(), "rb");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to open path %s for error %s", path.c_str(), strerror(errno));
        return false;
    }
    std::unique_ptr<::openmldb::log::SequentialFile> seq_file(::openmldb::log::NewSeqFile(path, fd));
    bool compressed = IsCompressed(path);
    ::openmldb::log::Reader reader(seq_file.get(), NULL, false, 0, compressed);
    ::openmldb::api::LogEntry entry;
    std::string buffer;
    std::string entry_buff;
    DLOG(INFO) << "begin dump snapshot index data";
    while (true) {
        buffer.clear();
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = reader.ReadRecord(&record, &buffer);
        if (status.IsWaitRecord() || status.IsEof()) {
            PDLOG(INFO, "read path %s for table tid %u pid %u completed, succ_cnt %lu, failed_cnt %lu",
                  path.c_str(), tid_, pid_, succ_cnt, failed_cnt);
            break;
        }
        if (!status.ok()) {
            PDLOG(WARNING, "fail to read record for tid %u, pid %u with error %s", tid_, pid_,
                  status.ToString().c_str());
            failed_cnt++;
            continue;
        }
        entry_buff.assign(record.data(), record.size());
        if (!entry.ParseFromString(entry_buff)) {
            PDLOG(WARNING, "fail to parse record for tid %u, pid %u", tid_, pid_);
            failed_cnt++;
            continue;
        }
        uint32_t index_pid = 0;
        if (!PackNewIndexEntry(table, index_cols, max_idx, idx, partition_num, &entry, &index_pid)) {
            DLOG(INFO) << "pack new entry fail in snapshot";
            continue;
        }
        std::string entry_str;
        entry.SerializeToString(&entry_str);
        ::openmldb::base::Slice new_record(entry_str);
        status = whs[index_pid]->Write(new_record);
        if (!status.ok()) {
            PDLOG(WARNING, "fail to dump index entrylog in snapshot to pid[%u]. tid %u pid %u", index_pid, tid_, pid_);
            return false;
        }
        succ_cnt++;
    }
    return true;
}

bool MemTableSnapshot::DumpIndexData(std::shared_ptr<Table> table,
        const std::vector<::openmldb::common::ColumnKey>& column_keys,
        uint32_t idx, const std::vector<std::shared_ptr<::openmldb::log::WriteHandle>>& whs) {
    auto column_key = column_keys[0];
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    if (making_snapshot_.exchange(true, std::memory_order_consume)) {
        PDLOG(INFO, "snapshot is doing now. tid %u, pid %u", tid, pid);
        return false;
    }
    std::map<std::string, uint32_t> column_desc_map;
    auto table_meta = table->GetTableMeta();
    for (int32_t i = 0; i < table_meta->column_desc_size(); ++i) {
        column_desc_map.emplace(table_meta->column_desc(i).name(), i);
    }
    uint32_t base_size = table_meta->column_desc_size();
    for (int32_t i = 0; i < table_meta->added_column_desc_size(); ++i) {
        column_desc_map.emplace(table_meta->added_column_desc(i).name(), i + base_size);
    }
    std::vector<std::vector<uint32_t>> index_cols;
    uint32_t max_idx = 0;
    for (const auto& ck : table_meta->column_key()) {
        std::vector<uint32_t> cols;
        if (ck.flag()) {
            continue;
        }
        for (const auto& name : ck.col_name()) {
            if (column_desc_map.find(name) != column_desc_map.end()) {
                uint32_t col_idx = column_desc_map[name];
                cols.push_back(col_idx);
                if (col_idx > max_idx) {
                    max_idx = col_idx;
                }
            } else {
                PDLOG(WARNING, "fail to find column_desc %s", name.c_str());
                making_snapshot_.store(false, std::memory_order_release);
                return false;
            }
        }
        index_cols.emplace_back(std::move(cols));
    }
    std::vector<uint32_t> cols;
    for (const auto& name : column_key.col_name()) {
        if (column_desc_map.find(name) != column_desc_map.end()) {
            uint32_t col_idx = column_desc_map[name];
            cols.push_back(col_idx);
            if (col_idx > max_idx) {
                max_idx = col_idx;
            }
        } else {
            PDLOG(WARNING, "fail to find column_desc %s", name.c_str());
            making_snapshot_.store(false, std::memory_order_release);
            return false;
        }
    }
    index_cols.push_back(cols);
    uint64_t collected_offset = CollectDeletedKey(0);
    uint64_t snapshot_offset = 0;
    bool ret = true;
    if (!DumpSnapshotIndexData(table, index_cols, max_idx, idx, whs, &snapshot_offset) ||
        !DumpBinlogIndexData(table, index_cols, max_idx, idx, whs, snapshot_offset, collected_offset)) {
        ret = false;
    }
    making_snapshot_.store(false, std::memory_order_release);
    return ret;
}

bool MemTableSnapshot::DumpBinlogIndexData(std::shared_ptr<Table> table,
        const std::vector<std::vector<uint32_t>>& index_cols, uint32_t max_idx,
        uint32_t idx, const std::vector<std::shared_ptr<::openmldb::log::WriteHandle>>& whs,
        uint64_t snapshot_offset, uint64_t collected_offset) {
    ::openmldb::log::LogReader log_reader(log_part_, log_path_, false);
    log_reader.SetOffset(snapshot_offset);
    uint64_t cur_offset = snapshot_offset;
    uint32_t partition_num = whs.size();
    ::openmldb::api::LogEntry entry;
    uint64_t succ_cnt = 0;
    uint64_t failed_cnt = 0;
    uint64_t consumed = ::baidu::common::timer::now_time();
    int last_log_index = log_reader.GetLogIndex();
    std::string buffer;
    std::string entry_buff;
    DLOG(INFO) << "begin dump binlog index data";
    while (cur_offset < collected_offset) {
        buffer.clear();
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.IsWaitRecord()) {
            int end_log_index = log_reader.GetEndLogIndex();
            int cur_log_index = log_reader.GetLogIndex();
            if (end_log_index >= 0 && end_log_index > cur_log_index) {
                log_reader.RollRLogFile();
                PDLOG(WARNING,
                      "read new binlog file. tid[%u] pid[%u] cur_log_index[%d] "
                      "end_log_index[%d] cur_offset[%lu]",
                      tid_, pid_, cur_log_index, end_log_index, cur_offset);
                continue;
            }
            consumed = ::baidu::common::timer::now_time() - consumed;
            PDLOG(INFO, "table tid %u pid %u completed, succ_cnt %lu, failed_cnt %lu, consumed %us",
                  tid_, pid_, succ_cnt, failed_cnt, consumed);
            break;
        }
        if (status.IsEof()) {
            if (log_reader.GetLogIndex() != last_log_index) {
                last_log_index = log_reader.GetLogIndex();
                continue;
            }
            break;
        }
        if (!status.ok()) {
            failed_cnt++;
            continue;
        }
        entry_buff.assign(record.data(), record.size());
        if (!entry.ParseFromString(entry_buff)) {
            PDLOG(WARNING, "fail parse record for tid %u, pid %u with value %s", tid_, pid_,
                  ::openmldb::base::DebugString(entry_buff).c_str());
            failed_cnt++;
            continue;
        }
        if (cur_offset >= entry.log_index()) {
            DEBUGLOG("offset %lu has been made snapshot", entry.log_index());
            continue;
        }

        if (cur_offset + 1 != entry.log_index()) {
            PDLOG(WARNING, "missing log entry cur_offset %lu , new entry offset %lu for tid %u, pid %u",
                  cur_offset, entry.log_index(), tid_, pid_);
        }
        uint32_t index_pid = 0;
        if (!PackNewIndexEntry(table, index_cols, max_idx, idx, partition_num, &entry, &index_pid)) {
            LOG(INFO) << "pack new entry fail in binlog";
            continue;
        }
        std::string entry_str;
        entry.SerializeToString(&entry_str);
        ::openmldb::base::Slice new_record(entry_str);
        status = whs[index_pid]->Write(new_record);
        if (!status.ok()) {
            PDLOG(WARNING, "fail to dump index entrylog in binlog to pid[%u].", index_pid);
            return false;
        }
        cur_offset = entry.log_index();
        succ_cnt++;
    }
    return true;
}

int MemTableSnapshot::DecodeData(std::shared_ptr<Table> table, const openmldb::api::LogEntry& entry, uint32_t max_idx,
                                 std::vector<std::string>& row) {
    std::string buff;
    openmldb::base::Slice data;
    if (table->GetCompressType() == openmldb::type::kSnappy) {
        snappy::Uncompress(entry.value().data(), entry.value().size(), &buff);
        data.reset(buff.data(), buff.size());
    } else {
        data.reset(entry.value().data(), entry.value().size());
    }
    const int8_t* raw = reinterpret_cast<const int8_t*>(data.data());
    uint8_t version = openmldb::codec::RowView::GetSchemaVersion(raw);
    int32_t data_size = data.size();
    std::shared_ptr<Schema> schema = table->GetVersionSchema(version);
    if (schema == nullptr) {
        LOG(WARNING) << "fail get version " << unsigned(version) << " schema";
        return 1;
    }

    bool ok = openmldb::codec::RowCodec::DecodeRow(*schema, raw, data_size, true, 0, max_idx + 1, row);
    if (!ok) {
        DLOG(WARNING) << "decode data error";
        return 3;
    }
    if (schema->size() < (int64_t)(max_idx + 1)) {
        DLOG(WARNING) << "data size is " << schema->size() << " less than " << max_idx + 1;
        return 2;
    }
    return 0;
}

::openmldb::base::Status DecodeData(std::shared_ptr<Table> table, const openmldb::api::LogEntry& entry,
        const std::vector<uint32_t>& cols, std::vector<std::string>* row) {
    row->clear();
    std::string buff;
    openmldb::base::Slice data;
    if (table->GetCompressType() == openmldb::type::kSnappy) {
        snappy::Uncompress(entry.value().data(), entry.value().size(), &buff);
        data.reset(buff.data(), buff.size());
    } else {
        data.reset(entry.value().data(), entry.value().size());
    }
    const int8_t* raw = reinterpret_cast<const int8_t*>(data.data());
    uint8_t version = openmldb::codec::RowView::GetSchemaVersion(raw);
    auto decoder = table->GetVersionDecoder(version);
    if (decoder == nullptr) {
        return ::openmldb::base::Status(-1, "get decoder failed. version is " + std::to_string(version));
    }
    if (!openmldb::codec::RowCodec::DecodeRow(*decoder, raw, cols, row)) {
        return ::openmldb::base::Status(-1, "decode failed");
    }
    return {};
}

bool MemTableSnapshot::IsCompressed(const std::string& path) {
    if (path.find(openmldb::log::ZLIB_COMPRESS_SUFFIX) != std::string::npos ||
        path.find(openmldb::log::SNAPPY_COMPRESS_SUFFIX) != std::string::npos) {
        return true;
    }
    return false;
}

::openmldb::base::Status MemTableSnapshot::WriteSnapshot(const MemSnapshotMeta& snapshot_meta,
        const ::openmldb::api::Manifest& old_manifest) {
    if (rename(snapshot_meta.tmp_file_path.c_str(), snapshot_meta.full_path.c_str()) == 0) {
        if (GenManifest(snapshot_meta) == 0) {
            // delete old snapshot
            if (old_manifest.has_name() && old_manifest.name() != snapshot_meta.snapshot_name) {
                DEBUGLOG("old snapshot[%s] has deleted", old_manifest.name().c_str());
                unlink((snapshot_path_ + old_manifest.name()).c_str());
            }
            offset_ = snapshot_meta.offset;
        } else {
            unlink(snapshot_meta.full_path.c_str());
            return {-1, absl::StrCat("GenManifest failed. delete snapshot file ", snapshot_meta.full_path)};
        }
    } else {
        unlink(snapshot_meta.tmp_file_path.c_str());
        return {-1, absl::StrCat("rename ", snapshot_meta.snapshot_name, " failed")};
    }
    return {};
}

::openmldb::base::Status MemTableSnapshot::DumpAndExtractIndexData(const std::shared_ptr<Table>& table,
        const std::vector<::openmldb::common::ColumnKey>& add_indexs,
        const std::vector<std::shared_ptr<::openmldb::log::WriteHandle>>& whs,
        uint64_t offset) {
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    if (making_snapshot_.exchange(true, std::memory_order_consume)) {
        PDLOG(INFO, "snapshot is doing now. tid %u, pid %u", tid, pid);
        return {-1, "snapshot is doing now"};
    }
    TableIndexInfo table_index_info(*(table->GetTableMeta()), add_indexs);
    if (!table_index_info.Init()) {
        return {-1, "parse TableIndexInfo failed"};
    }
    MemSnapshotMeta snapshot_meta(GenSnapshotName(), snapshot_path_, FLAGS_snapshot_compression);
    auto wh = ::openmldb::log::CreateWriteHandle(FLAGS_snapshot_compression,
            snapshot_meta.snapshot_name, snapshot_meta.tmp_file_path);
    if (!wh) {
        making_snapshot_.store(false, std::memory_order_release);
        return {-1, "create WriteHandle failed"};
    }
    uint64_t collected_offset = CollectDeletedKey(offset);

    ::openmldb::api::Manifest manifest;
    int result = GetLocalManifest(snapshot_path_ + MANIFEST, manifest);
    ::openmldb::base::Status status;
    if (result == 0) {
        DLOG(INFO) << "begin extract index data from snapshot";
        /*if (!ExtractIndexFromSnapshot(table, manifest, wh, indexs, partition_num,
                    &write_count, &expired_key_num, &deleted_key_num).OK()) {
            has_error = true;
        }*/
        snapshot_meta.term = manifest.term();
        DLOG(INFO) << "old manifest term is " << snapshot_meta.term;
    } else if (result < 0) {
        status = {-1, "parse manifest error"};
    }
    deleted_keys_.clear();
    wh->EndLog();
    wh.reset();
    if (!status.OK()) {
        unlink(snapshot_meta.tmp_file_path.c_str());
    } else {
        WriteSnapshot(snapshot_meta, manifest);
    }
    making_snapshot_.store(false, std::memory_order_release);
    return status;
}

}  // namespace storage
}  // namespace openmldb
