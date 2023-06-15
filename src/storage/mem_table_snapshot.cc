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

#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>
#include <unistd.h>

#include <set>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_set.h"
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
#include "google/protobuf/io/zero_copy_stream_impl.h"
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

bool IsCompressed(const std::string& path) {
    if (path.find(openmldb::log::ZLIB_COMPRESS_SUFFIX) != std::string::npos ||
        path.find(openmldb::log::SNAPPY_COMPRESS_SUFFIX) != std::string::npos) {
        return true;
    }
    return false;
}

std::shared_ptr<DataReader> DataReader::CreateDataReader(const std::string& snapshot_path, LogParts* log_part,
        const std::string& log_path, DataReaderType type) {
    return CreateDataReader(snapshot_path, log_part, log_path, type, 0);
}

std::shared_ptr<DataReader> DataReader::CreateDataReader(LogParts* log_part, const std::string& log_path,
        uint64_t start_offset, uint64_t end_offset) {
    auto reader = std::make_shared<DataReader>("", log_part, log_path, DataReaderType::kBinlog,
            start_offset, end_offset);
    if (!reader->Init()) {
        reader.reset();
    }
    return reader;
}

std::shared_ptr<DataReader> DataReader::CreateDataReader(const std::string& snapshot_path, LogParts* log_part,
        const std::string& log_path, DataReaderType type, uint64_t end_offset) {
    auto reader = std::make_shared<DataReader>(snapshot_path, log_part, log_path, type, 0, end_offset);
    if (!reader->Init()) {
        reader.reset();
    }
    return reader;
}

bool DataReader::Init() {
    uint64_t snapshot_offset = 0;
    if (read_type_ == DataReaderType::kSnapshot || read_type_ == DataReaderType::kSnapshotAndBinlog) {
        ::openmldb::api::Manifest manifest;
        int ret = Snapshot::GetLocalManifest(snapshot_path_ + MANIFEST, manifest);
        if (ret == -1) {
            return false;
        } else if (ret == 0) {
            snapshot_offset = manifest.offset();
            std::string path = absl::StrCat(snapshot_path_, "/", manifest.name());
            FILE* fd = fopen(path.c_str(), "rb");
            if (fd == nullptr) {
                PDLOG(WARNING, "fail to open path %s for error %s", path.c_str(), strerror(errno));
                return false;
            }
            seq_file_.reset(::openmldb::log::NewSeqFile(path, fd));
            bool compressed = IsCompressed(path);
            snapshot_reader_ = std::make_shared<::openmldb::log::Reader>(
                    seq_file_.get(), nullptr, false, 0, compressed);
            read_snapshot_ = true;
        }
    }
    if (read_type_ == DataReaderType::kBinlog || read_type_ == DataReaderType::kSnapshotAndBinlog) {
        if (log_part_ == nullptr) {
            return false;
        }
        binlog_reader_ = std::make_shared<::openmldb::log::LogReader>(log_part_, log_path_, false);
        cur_offset_ = std::max(start_offset_, snapshot_offset);
        binlog_reader_->SetOffset(cur_offset_);
        read_binlog_ = true;
    }
    return true;
}

bool DataReader::ReadFromSnapshot() {
    if (!read_snapshot_) {
        return false;
    }
    do {
        buffer_.clear();
        record_.clear();
        auto status = snapshot_reader_->ReadRecord(&record_, &buffer_);
        if (status.IsWaitRecord() || status.IsEof()) {
            PDLOG(INFO, "read snapshot completed, succ_cnt %lu, failed_cnt %lu, path %s",
                    succ_cnt_, failed_cnt_, snapshot_path_.c_str());
            succ_cnt_ = 0;
            failed_cnt_ = 0;
            read_snapshot_ = false;
            return false;
        }
        if (!status.ok()) {
            PDLOG(WARNING, "fail to read snapshot record. path %s, error %s",
                    snapshot_path_.c_str(), status.ToString().c_str());
            failed_cnt_++;
            continue;
        }
        entry_buff_.assign(record_.data(), record_.size());
        if (!entry_.ParseFromString(entry_buff_)) {
            PDLOG(WARNING, "fail to parse record. path %s", snapshot_path_);
            failed_cnt_++;
            continue;
        }
        succ_cnt_++;
        break;
    } while (true);
    return true;
}

bool DataReader::ReadFromBinlog() {
    if (!read_binlog_) {
        return false;
    }
    int last_log_index = binlog_reader_->GetLogIndex();
    do {
        if (end_offset_ > 0 && cur_offset_ >= end_offset_) {
            break;
        }
        buffer_.clear();
        record_.clear();
        auto status = binlog_reader_->ReadNextRecord(&record_, &buffer_);
        if (status.IsWaitRecord()) {
            int end_log_index = binlog_reader_->GetEndLogIndex();
            int cur_log_index = binlog_reader_->GetLogIndex();
            if (end_log_index >= 0 && end_log_index > cur_log_index) {
                binlog_reader_->RollRLogFile();
                PDLOG(INFO, "read new binlog file. path[%s] cur_log_index[%d] "
                        "end_log_index[%d] cur_offset[%lu]",
                        log_path_.c_str(), cur_log_index, end_log_index, cur_offset_);
                continue;
            }
            PDLOG(INFO, "read binlog completed, succ_cnt %lu, failed_cnt %lu, path %s",
                    succ_cnt_, failed_cnt_, log_path_.c_str());
            break;
        } else if (status.IsEof()) {
            if (binlog_reader_->GetLogIndex() != last_log_index) {
                continue;
            }
        } else if (!status.ok()) {
            failed_cnt_++;
            continue;
        }
        entry_buff_.assign(record_.data(), record_.size());
        if (!entry_.ParseFromString(entry_buff_)) {
            PDLOG(WARNING, "fail to parse record. path %s", log_path_.c_str());
            failed_cnt_++;
            continue;
        }
        if (cur_offset_ >= entry_.log_index()) {
            DEBUGLOG("cur offset is %lu, skip %lu", cur_offset_, entry_.log_index());
            continue;
        }
        if (cur_offset_ + 1 != entry_.log_index()) {
            PDLOG(WARNING, "missing log entry. cur_offset %lu, new entry offset %lu, path %s",
                    cur_offset_, entry_.log_index(), log_path_.c_str());
            continue;
        }
        succ_cnt_++;
        cur_offset_ = entry_.log_index();
        return true;
    } while (true);
    read_binlog_ = false;
    return false;
}

bool DataReader::HasNext() {
    if (read_snapshot_ && ReadFromSnapshot()) {
        return true;
    }
    if (read_binlog_ && ReadFromBinlog()) {
        return true;
    }
    return false;
}

bool TableIndexInfo::Init() {
    for (int32_t i = 0; i < table_meta_.column_desc_size(); i++) {
        column_idx_map_.emplace(table_meta_.column_desc(i).name(), i);
    }
    uint32_t base_size = table_meta_.column_desc_size();
    for (int32_t i = 0; i < table_meta_.added_column_desc_size(); ++i) {
        column_idx_map_.emplace(table_meta_.added_column_desc(i).name(), i + base_size);
    }
    std::set<uint32_t> index_col_set;
    for (int32_t i = 0; i < table_meta_.column_key_size(); i++) {
        const auto& ck = table_meta_.column_key(i);
        if (ck.flag()) {
            continue;
        }
        for (const auto& add_ck : add_indexs_) {
            if (ck.index_name() == add_ck.index_name()) {
                add_index_idx_vec_.push_back(i);
                break;
            }
        }
        std::vector<uint32_t> cols;
        for (const auto& name : ck.col_name()) {
            auto iter = column_idx_map_.find(name);
            if (iter != column_idx_map_.end()) {
                cols.push_back(iter->second);
                index_col_set.insert(iter->second);
            } else {
                PDLOG(WARNING, "fail to find column_desc %s", name.c_str());
                return false;
            }
        }
        index_cols_map_.emplace(i, std::move(cols));
    }
    if (add_index_idx_vec_.size() != add_indexs_.size()) {
        return false;
    }
    std::map<uint32_t, uint32_t> col_idx_map;
    for (auto idx : index_col_set) {
        col_idx_map.emplace(idx, all_index_cols_.size());
        all_index_cols_.push_back(idx);
    }
    for (const auto& kv : index_cols_map_) {
        std::vector<uint32_t> vec;
        for (auto idx : kv.second) {
            vec.push_back(col_idx_map[idx]);
        }
        real_index_cols_map_.emplace(kv.first, std::move(vec));
    }
    return true;
}

bool TableIndexInfo::HasIndex(uint32_t idx) const {
    return index_cols_map_.find(idx) != index_cols_map_.end();
}

const std::vector<uint32_t>& TableIndexInfo::GetIndexCols(uint32_t idx) {
    return index_cols_map_[idx];
}

const std::vector<uint32_t>& TableIndexInfo::GetRealIndexCols(uint32_t idx) {
    return real_index_cols_map_[idx];
}

DeleteSpan::DeleteSpan(const api::LogEntry& entry) {
    offset = entry.log_index();
    if (entry.dimensions_size() > 0) {
        idx = entry.dimensions(0).idx();
    }
    if (entry.has_ts()) {
        start_ts = entry.ts();
    }
    if (entry.has_end_ts()) {
        end_ts = entry.end_ts();
    }
}

bool DeleteSpan::IsDeleted(uint64_t offset_i, uint32_t idx_i, uint64_t ts) const {
    if (offset_i <= offset) {
        if (idx.has_value() && idx.value() != idx_i) {
            return false;
        }
        if (ts <= start_ts) {
            if (end_ts.has_value()) {
                if (ts > end_ts.value()) {
                    return true;
                }
            } else {
                return true;
            }
        }
    }
    return false;
}

void DeleteCollector::Clear() {
    deleted_keys_.clear();
    deleted_spans_.clear();
    no_key_spans_.clear();
}

bool DeleteCollector::IsEmpty() const {
    return deleted_keys_.empty() && deleted_spans_.empty() && no_key_spans_.empty();
}

bool DeleteCollector::IsDeleted(uint64_t offset, uint32_t idx, const std::string& key, uint64_t ts) const {
    if (IsEmpty()) {
        return false;
    }
    if (!no_key_spans_.empty()) {
        auto iter = no_key_spans_.lower_bound(offset);
        while (iter !=  no_key_spans_.end()) {
            if (iter->second.IsDeleted(offset, idx, ts)) {
                return true;
            }
            iter++;
        }
    }
    if (auto iter = deleted_keys_.find(key); iter != deleted_keys_.end() && offset <= iter->second) {
        return true;
    }
    if (auto iter = deleted_spans_.find(key);
            iter != deleted_spans_.end() && iter->second.IsDeleted(offset, idx, ts)) {
        return true;
    }
    return false;
}

void DeleteCollector::AddSpan(uint64_t offset, DeleteSpan span) {
    no_key_spans_.emplace(offset, std::move(span));
}

void DeleteCollector::AddSpan(std::string key, DeleteSpan span) {
    deleted_spans_.insert_or_assign(std::move(key), std::move(span));
}

void DeleteCollector::AddKey(uint64_t offset, std::string key) {
    deleted_keys_.insert_or_assign(std::move(key), offset);
}

size_t DeleteCollector::Size() const {
    return deleted_keys_.size() + deleted_spans_.size() + no_key_spans_.size();
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
    std::string full_path = absl::StrCat(snapshot_path_, "/", snapshot_name);
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
        std::unique_ptr<::openmldb::log::SequentialFile> seq_file(::openmldb::log::NewSeqFile(path, fd));
        ::openmldb::log::Reader reader(seq_file.get(), NULL, false, 0, compressed);
        std::string buffer;
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
    for (const auto ptr : recordPtr) {
        bool ok = entry.ParseFromString(*ptr);
        delete ptr;
        if (!ok) {
            failed_cnt->fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        auto scount = succ_cnt->fetch_add(1, std::memory_order_relaxed);
        if (scount % 100000 == 0) {
            PDLOG(INFO, "load snapshot %s with succ_cnt %lu, failed_cnt %lu", path.c_str(), scount,
                  failed_cnt->load(std::memory_order_relaxed));
        }
        if (entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete) {
            table->Delete(entry);
        } else {
            table->Put(entry);
        }
    }
}

int MemTableSnapshot::TTLSnapshot(std::shared_ptr<Table> table, const ::openmldb::api::Manifest& manifest,
        const std::shared_ptr<WriteHandle>& wh, MemSnapshotMeta* snapshot_meta) {
    auto data_reader = DataReader::CreateDataReader(snapshot_path_, nullptr, "", DataReaderType::kSnapshot);
    if (!data_reader) {
        PDLOG(WARNING, "fail to create data reader. tid %u pid %u", tid_, pid_);
        return -1;
    }
    bool has_error = false;
    std::string tmp_buf;
    while (data_reader->HasNext()) {
        auto& entry = data_reader->GetValue();
        ::openmldb::base::Slice record(data_reader->GetStrValue());
        if (!delete_collector_.IsEmpty()) {
            int ret = CheckDeleteAndUpdate(table, &entry);
            if (ret == 1) {
                snapshot_meta->deleted_key_num++;
                continue;
            } else if (ret == 2) {
                entry.SerializeToString(&tmp_buf);
                record.reset(tmp_buf.data(), tmp_buf.size());
            }
        }
        if (table->IsExpire(entry)) {
            snapshot_meta->expired_key_num++;
            continue;
        }
        auto status = wh->Write(record);
        if (!status.ok()) {
            PDLOG(WARNING, "fail to write snapshot. status[%s]", status.ToString().c_str());
            has_error = true;
            break;
        }
        if ((snapshot_meta->count + snapshot_meta->expired_key_num + snapshot_meta->deleted_key_num)
                % KEY_NUM_DISPLAY == 0) {
            PDLOG(INFO, "tackled key num[%lu] total[%lu]",
                    snapshot_meta->count + snapshot_meta->expired_key_num, manifest.count());
        }
        snapshot_meta->count++;
    }
    if (snapshot_meta->expired_key_num + snapshot_meta->count + snapshot_meta->deleted_key_num
            != manifest.count()) {
        PDLOG(WARNING, "key num not match! total key num[%lu] load key num[%lu] ttl key num[%lu]",
              manifest.count(), snapshot_meta->count, snapshot_meta->expired_key_num);
        has_error = true;
    }
    if (has_error) {
        return -1;
    }
    PDLOG(INFO, "load snapshot success. load key num[%lu] ttl key num[%lu]",
            snapshot_meta->count, snapshot_meta->expired_key_num);
    return 0;
}

uint64_t MemTableSnapshot::CollectDeletedKey(uint64_t end_offset) {
    delete_collector_.Clear();
    uint64_t cur_offset = offset_;
    auto data_reader = DataReader::CreateDataReader(log_part_, log_path_, offset_, end_offset);
    if (!data_reader) {
        return cur_offset;
    }
    while (data_reader->HasNext()) {
        if (delete_collector_.Size() >= FLAGS_make_snapshot_max_deleted_keys) {
            PDLOG(WARNING, "deleted_keys map size reach the make_snapshot_max_deleted_keys %u, tid %u pid %u",
                  FLAGS_make_snapshot_max_deleted_keys, tid_, pid_);
            break;
        }
        const auto& entry = data_reader->GetValue();
        cur_offset = entry.log_index();
        if (entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete) {
            if (entry.dimensions_size() == 0) {
                delete_collector_.AddSpan(cur_offset, DeleteSpan(entry));
                DEBUGLOG("insert span offset %lu. tid %u pid %u", cur_offset, tid_, pid_);
            } else {
                std::string combined_key = absl::StrCat(entry.dimensions(0).key(), "|", entry.dimensions(0).idx());
                if (entry.has_ts() || entry.has_end_ts()) {
                    delete_collector_.AddSpan(std::move(combined_key), DeleteSpan(entry));
                } else {
                    delete_collector_.AddKey(cur_offset, std::move(combined_key));
                }
                DEBUGLOG("insert key %s offset %lu. tid %u pid %u", combined_key.c_str(), cur_offset, tid_, pid_);
            }
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
    absl::Cleanup clean = [this] {
        this->making_snapshot_.store(false, std::memory_order_release);
        this->delete_collector_.Clear();
    };
    MemSnapshotMeta snapshot_meta(GenSnapshotName(), snapshot_path_, FLAGS_snapshot_compression);
    auto wh = ::openmldb::log::CreateWriteHandle(FLAGS_snapshot_compression,
            snapshot_meta.snapshot_name, snapshot_meta.tmp_file_path);
    if (!wh) {
        PDLOG(WARNING, "fail to create file %s", snapshot_meta.tmp_file_path.c_str());
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
        if (TTLSnapshot(table, manifest, wh, &snapshot_meta) < 0) {
            has_error = true;
        }
        snapshot_meta.term = manifest.term();
        DEBUGLOG("old manifest term is %lu", snapshot_meta.term);
    } else if (result < 0) {
        // parse manifest error
        has_error = true;
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
            if (!delete_collector_.IsEmpty()) {
                int ret = CheckDeleteAndUpdate(table, &entry);
                if (ret == 1) {
                    snapshot_meta.deleted_key_num++;
                    continue;
                } else if (ret == 2) {
                    entry.SerializeToString(&tmp_buf);
                    record.reset(tmp_buf.data(), tmp_buf.size());
                }
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
    wh->EndLog();
    wh.reset();
    if (has_error) {
        unlink(snapshot_meta.tmp_file_path.c_str());
        return -1;
    } else {
        snapshot_meta.offset = cur_offset;
        uint64_t old_offset = offset_;
        auto status = WriteSnapshot(snapshot_meta);
        if (!status.OK()) {
            PDLOG(WARNING, "write snapshot failed. tid %u pid %u msg is %s ", tid_, pid_, status.GetMsg().c_str());
            return -1;
        }
        uint64_t consumed = ::baidu::common::timer::now_time() - start_time;
        PDLOG(INFO, "make snapshot[%s] success. update offset from %lu to %lu."
              "use %lu second. write key %lu expired key %lu deleted key %lu",
              snapshot_meta.snapshot_name.c_str(), old_offset, snapshot_meta.offset, consumed,
              snapshot_meta.count, snapshot_meta.expired_key_num, snapshot_meta.deleted_key_num);
        out_offset = snapshot_meta.offset;
    }
    return 0;
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
    if (delete_collector_.IsEmpty()) {
        return 0;
    }
    if (entry->dimensions_size() == 0) {
        std::string combined_key = absl::StrCat(entry->pk(), "|0");
        if (delete_collector_.IsDeleted(entry->log_index(), 0, combined_key, entry->ts())) {
            return 1;
        }
    } else {
        absl::flat_hash_set<int> deleted_pos_set;
        for (int pos = 0; pos < entry->dimensions_size(); pos++) {
            uint32_t idx = entry->dimensions(pos).idx();
            auto index = table->GetIndex(idx);
            if (index == nullptr || !index->IsReady()) {
                deleted_pos_set.insert(pos);
                continue;
            }
            std::string combined_key = absl::StrCat(entry->dimensions(pos).key(), "|", idx);
            auto ts_col = index->GetTsColumn();
            if (ts_col->IsAutoGenTs()) {
                if (delete_collector_.IsDeleted(entry->log_index(), idx, combined_key, entry->ts())) {
                    deleted_pos_set.insert(pos);
                }
            } else {
                std::string buff;
                openmldb::base::Slice data;
                if (table->GetCompressType() == openmldb::type::kSnappy) {
                    snappy::Uncompress(entry->value().data(), entry->value().size(), &buff);
                    data.reset(buff.data(), buff.size());
                } else {
                    data.reset(entry->value().data(), entry->value().size());
                }
                const int8_t* raw = reinterpret_cast<const int8_t*>(data.data());
                uint8_t version = openmldb::codec::RowView::GetSchemaVersion(raw);
                auto decoder = table->GetVersionDecoder(version);
                int64_t ts = 0;
                if (decoder->GetInteger(raw, ts_col->GetId(), ts_col->GetType(), &ts) < 0) {
                    PDLOG(WARNING, "fail to get ts. tid %u pid %u", tid_, pid_);
                    return -1;
                }
                if (delete_collector_.IsDeleted(entry->log_index(), idx, combined_key, ts)) {
                    deleted_pos_set.insert(pos);
                }
            }
        }
        if (!deleted_pos_set.empty()) {
            if (static_cast<int>(deleted_pos_set.size()) == entry->dimensions_size()) {
                return 1;
            } else {
                ::google::protobuf::RepeatedPtrField<api::Dimension> dimensions(entry->dimensions());
                entry->clear_dimensions();
                for (int pos = 0; pos < dimensions.size(); pos++) {
                    if (deleted_pos_set.find(pos) == deleted_pos_set.end()) {
                        entry->add_dimensions()->CopyFrom(dimensions.Get(pos));
                    }
                }
                return 2;
            }
        }
    }
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

::openmldb::base::Status MemTableSnapshot::DecodeData(const std::shared_ptr<Table>& table,
        const openmldb::api::LogEntry& entry,
        const std::vector<uint32_t>& cols, std::vector<std::string>* row) {
    if (row == nullptr) {
        return {-1, "row is nullptr"};
    }
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
    if (!openmldb::codec::RowCodec::DecodeRow(*decoder, raw, cols, true, row)) {
        return ::openmldb::base::Status(-1, "decode failed");
    }
    return {};
}

::openmldb::base::Status MemTableSnapshot::WriteSnapshot(const MemSnapshotMeta& snapshot_meta) {
    ::openmldb::api::Manifest old_manifest;
    if (GetLocalManifest(snapshot_path_ + MANIFEST, old_manifest) < 0) {
        return {-1, absl::StrCat("get old manifest failed. snapshot path is ", snapshot_path_)};
    }
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

::openmldb::base::Status MemTableSnapshot::ExtractIndexData(const std::shared_ptr<Table>& table,
        const std::vector<::openmldb::common::ColumnKey>& add_indexs,
        const std::vector<std::shared_ptr<::openmldb::log::WriteHandle>>& whs,
        uint64_t offset, bool dump_data) {
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    if (making_snapshot_.exchange(true, std::memory_order_consume)) {
        PDLOG(INFO, "snapshot is doing now. tid %u, pid %u", tid, pid);
        return {-1, "snapshot is doing now"};
    }
    absl::Cleanup clean = [this]() {
        this->making_snapshot_.store(false, std::memory_order_release);
        delete_collector_.Clear();
    };
    TableIndexInfo table_index_info(*(table->GetTableMeta()), add_indexs);
    if (!table_index_info.Init()) {
        return {-1, "parse TableIndexInfo failed"};
    }
    MemSnapshotMeta snapshot_meta(GenSnapshotName(), snapshot_path_, FLAGS_snapshot_compression);
    auto wh = ::openmldb::log::CreateWriteHandle(FLAGS_snapshot_compression,
            snapshot_meta.snapshot_name, snapshot_meta.tmp_file_path);
    if (!wh) {
        return {-1, "create WriteHandle failed"};
    }
    CollectDeletedKey(offset);

    auto data_reader = DataReader::CreateDataReader(snapshot_path_, log_part_, log_path_,
            DataReaderType::kSnapshotAndBinlog, offset);
    if (!data_reader) {
        return {-1, "create DataReader failed"};
    }
    std::string buff;
    ::openmldb::base::Status status;
    uint64_t read_cnt = 0;
    uint64_t dump_cnt = 0;
    uint64_t extract_cnt = 0;
    uint64_t cur_offset = 0;
    while (data_reader->HasNext()) {
        auto& entry = data_reader->GetValue();
        read_cnt++;
        cur_offset = entry.log_index();
        if (entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete) {
            continue;
        }
        if (entry.has_term()) {
            snapshot_meta.term = entry.term();
        }
        if (table->IsExpire(entry)) {
            snapshot_meta.expired_key_num++;
            continue;
        }
        bool entry_updated = false;
        if (!delete_collector_.IsEmpty()) {
            auto ret = CheckDeleteAndUpdate(table, &entry);
            if (ret == 1) {
                snapshot_meta.deleted_key_num++;
                continue;
            } else if (ret == 2) {
                entry_updated = true;
            }
        }
        bool has_main_index = false;
        for (int pos = 0; pos < entry.dimensions_size(); pos++) {
            if (entry.dimensions(pos).idx() == 0) {
                has_main_index = true;
                break;
            }
        }
        if (!has_main_index) {
            if (entry_updated) {
                buff.clear();
                entry.SerializeToString(&buff);
                if (!wh->Write(::openmldb::base::Slice(buff)).ok()) {
                    status = {-1, "fail to write snapshot"};
                    break;
                }
            } else {
                if (!wh->Write(::openmldb::base::Slice(data_reader->GetStrValue())).ok()) {
                    status = {-1, "fail to write snapshot"};
                    break;
                }
            }
            continue;
        }
        std::vector<std::string> index_row;
        auto staus = DecodeData(table, entry, table_index_info.GetAllIndexCols(), &index_row);
        std::map<uint32_t, std::vector<::openmldb::api::Dimension>> dimension_map;
        for (auto idx : table_index_info.GetAddIndexIdx()) {
            std::string index_key;
            for (auto pos : table_index_info.GetRealIndexCols(idx)) {
                if (index_key.empty()) {
                    index_key = index_row.at(pos);
                } else {
                    absl::StrAppend(&index_key, "|", index_row.at(pos));
                }
            }
            ::openmldb::api::Dimension dim;
            dim.set_idx(idx);
            dim.set_key(index_key);
            uint32_t index_pid = ::openmldb::base::hash64(index_key) % whs.size();
            auto iter = dimension_map.emplace(index_pid, std::vector<::openmldb::api::Dimension>()).first;
            iter->second.push_back(dim);
        }
        std::string tmp_buf;
        auto iter = dimension_map.find(pid);
        if (iter != dimension_map.end()) {
            for (const auto& dim : iter->second) {
                entry.add_dimensions()->CopyFrom(dim);
            }
            entry.SerializeToString(&tmp_buf);
            if (!wh->Write(::openmldb::base::Slice(tmp_buf)).ok()) {
                status = {-1, "fail to write snapshot"};
                break;
            }
            entry.clear_dimensions();
            for (const auto& dim : iter->second) {
                ::openmldb::api::Dimension* new_dim = entry.add_dimensions();
                new_dim->CopyFrom(dim);
            }
            DLOG(INFO) << "extract: dim size " << entry.dimensions_size() << " key " << entry.dimensions(0).key();
            extract_cnt++;
            table->Put(entry);
        } else {
            if (entry_updated) {
                buff.clear();
                entry.SerializeToString(&buff);
                if (!wh->Write(::openmldb::base::Slice(buff)).ok()) {
                    status = {-1, "fail to write snapshot"};
                    break;
                }
            } else {
                if (!wh->Write(::openmldb::base::Slice(data_reader->GetStrValue())).ok()) {
                    status = {-1, "fail to write snapshot"};
                    break;
                }
            }
        }
        if (dump_data) {
            for (const auto& kv : dimension_map) {
                if (kv.first == pid) {
                    continue;
                }
                entry.clear_dimensions();
                for (const auto& dim : kv.second) {
                    ::openmldb::api::Dimension* new_dim = entry.add_dimensions();
                    new_dim->CopyFrom(dim);
                }
                tmp_buf.clear();
                entry.SerializeToString(&tmp_buf);
                if (!whs[kv.first]->Write(::openmldb::base::Slice(tmp_buf)).ok()) {
                    status =  {-1, "fail to dump index entry"};
                    break;
                }
                DLOG(INFO) << "dump " << pid << " dim size " << entry.dimensions_size()
                    << " key " << entry.dimensions(0).key();
                dump_cnt++;
            }
        }
        if (!status.OK()) {
            break;
        }
    }
    LOG(INFO) << "read cnt " << read_cnt << " dump cnt " << dump_cnt << " extract cnt " << extract_cnt
        << " tid " << tid << " pid " << pid;
    wh->EndLog();
    wh.reset();
    if (!status.OK()) {
        unlink(snapshot_meta.tmp_file_path.c_str());
    } else {
        snapshot_meta.offset = std::max(cur_offset, offset_);
        WriteSnapshot(snapshot_meta);
    }
    return status;
}

}  // namespace storage
}  // namespace openmldb
