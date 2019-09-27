//
// snapshot.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-24
//
//
#include "storage/mem_table_snapshot.h"

#include "base/file_util.h"
#include "base/strings.h"
#include "base/slice.h"
#include "base/count_down_latch.h"
#include "log/sequential_file.h"
#include "log/log_reader.h"
#include "proto/tablet.pb.h"
#include "gflags/gflags.h"
#include "logging.h"
#include "timer.h"
#include "thread_pool.h"
#include <unistd.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <boost/bind.hpp>
#include "base/taskpool.hpp"

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_uint64(gc_on_table_recover_count);
DECLARE_int32(binlog_name_length);
DECLARE_uint32(make_snapshot_max_deleted_keys);
DECLARE_uint32(load_table_batch);
DECLARE_uint32(load_table_thread_num);
DECLARE_uint32(load_table_queue_size);

namespace rtidb {
namespace storage {

const std::string SNAPSHOT_SUBFIX=".sdb";
const uint32_t KEY_NUM_DISPLAY = 1000000;
const std::string MANIFEST = "MANIFEST";

MemTableSnapshot::MemTableSnapshot(uint32_t tid, uint32_t pid, LogParts* log_part,
        const std::string& db_root_path): Snapshot(tid, pid),
     log_part_(log_part), db_root_path_(db_root_path) {}

bool MemTableSnapshot::Init() {
    snapshot_path_ = db_root_path_ + "/" + std::to_string(tid_) + "_" + std::to_string(pid_) + "/snapshot/";
    log_path_ = db_root_path_ + "/" + std::to_string(tid_) + "_" + std::to_string(pid_) + "/binlog/";
    if (!::rtidb::base::MkdirRecur(snapshot_path_)) {
        PDLOG(WARNING, "fail to create db meta path %s", snapshot_path_.c_str());
        return false;
    }
    if (!::rtidb::base::MkdirRecur(log_path_)) {
        PDLOG(WARNING, "fail to create db meta path %s", log_path_.c_str());
        return false;
    }
    return true;
}

bool MemTableSnapshot::Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) {
    ::rtidb::api::Manifest manifest;
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

void MemTableSnapshot::RecoverFromSnapshot(const std::string& snapshot_name, uint64_t expect_cnt, std::shared_ptr<Table> table) {
    std::string full_path = snapshot_path_ + "/" + snapshot_name;
    std::atomic<uint64_t> g_succ_cnt(0);
    std::atomic<uint64_t> g_failed_cnt(0);
    RecoverSingleSnapshot(full_path, table, &g_succ_cnt, &g_failed_cnt);
    PDLOG(INFO, "[Recover] progress done stat: success count %lu, failed count %lu",
                    g_succ_cnt.load(std::memory_order_relaxed),
                    g_failed_cnt.load(std::memory_order_relaxed));
    if (g_succ_cnt.load(std::memory_order_relaxed) != expect_cnt) {
        PDLOG(WARNING, "snapshot %s , expect cnt %lu but succ_cnt %lu", snapshot_name.c_str(),
                expect_cnt, g_succ_cnt.load(std::memory_order_relaxed));
    }
}


void MemTableSnapshot::RecoverSingleSnapshot(const std::string& path, std::shared_ptr<Table> table,
                                     std::atomic<uint64_t>* g_succ_cnt,
                                     std::atomic<uint64_t>* g_failed_cnt) {
    ::rtidb::base::TaskPool load_pool_(FLAGS_load_table_thread_num, FLAGS_load_table_batch);
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
        ::rtidb::log::SequentialFile* seq_file = ::rtidb::log::NewSeqFile(path, fd);
        ::rtidb::log::Reader reader(seq_file, NULL, false, 0);
        std::string buffer;
        // second
        uint64_t consumed = ::baidu::common::timer::now_time();
        std::vector<std::string*> recordPtr;
        recordPtr.reserve(FLAGS_load_table_batch);

        while (true) {
            buffer.clear();
            ::rtidb::base::Slice record;
            ::rtidb::base::Status status = reader.ReadRecord(&record, &buffer);
            if (status.IsWaitRecord() || status.IsEof()) {
                consumed = ::baidu::common::timer::now_time() - consumed;
                PDLOG(INFO, "read path %s for table tid %u pid %u completed, succ_cnt %lu, failed_cnt %lu, consumed %us",
                      path.c_str(), tid_, pid_, succ_cnt.load(std::memory_order_relaxed), failed_cnt.load(std::memory_order_relaxed), consumed);
                break;
            }

            if (!status.ok()) {
                PDLOG(WARNING, "fail to read record for tid %u, pid %u with error %s", tid_, pid_, status.ToString().c_str());
                failed_cnt.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            std::string* sp = new std::string(record.data(), record.size());
            recordPtr.push_back(sp);
            if (recordPtr.size() >= FLAGS_load_table_batch) {
                load_pool_.AddTask(boost::bind(&MemTableSnapshot::Put, this, path, table, recordPtr, &succ_cnt, &failed_cnt));
                recordPtr.clear();
            }
        }
        if (recordPtr.size() > 0) {
            load_pool_.AddTask(boost::bind(&MemTableSnapshot::Put, this, path, table, recordPtr, &succ_cnt, &failed_cnt));
        }
        // will close the fd atomic
        delete seq_file;
        if (g_succ_cnt) {
            g_succ_cnt->fetch_add(succ_cnt, std::memory_order_relaxed);
        }
        if (g_failed_cnt) {
            g_failed_cnt->fetch_add(failed_cnt, std::memory_order_relaxed);
        }
    }while(false);
    load_pool_.Stop();
}

void MemTableSnapshot::Put(std::string& path, std::shared_ptr<Table>& table, std::vector<std::string*> recordPtr, std::atomic<uint64_t>* succ_cnt, std::atomic<uint64_t>* failed_cnt) {
    ::rtidb::api::LogEntry entry;
    for (auto it = recordPtr.cbegin(); it != recordPtr.cend(); it++) {
        bool ok = entry.ParseFromString(**it);
        if (!ok) {
            failed_cnt->fetch_add(1, std::memory_order_relaxed);
            delete *it;
            continue;
        }
        auto scount = succ_cnt->fetch_add(1, std::memory_order_relaxed);
        if (scount % 100000 == 0) {
            PDLOG(INFO, "load snapshot %s with succ_cnt %lu, failed_cnt %lu", path.c_str(),
                  scount, failed_cnt->load(std::memory_order_relaxed));
        }
        table->Put(entry);
        delete *it;
    }
}

int MemTableSnapshot::TTLSnapshot(std::shared_ptr<Table> table, const ::rtidb::api::Manifest& manifest, WriteHandle* wh, 
            uint64_t& count, uint64_t& expired_key_num, uint64_t& deleted_key_num) {
    std::string full_path = snapshot_path_ + manifest.name();
    FILE* fd = fopen(full_path.c_str(), "rb");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to open path %s for error %s", full_path.c_str(), strerror(errno));
        return -1;
    }
    ::rtidb::log::SequentialFile* seq_file = ::rtidb::log::NewSeqFile(manifest.name(), fd);
    ::rtidb::log::Reader reader(seq_file, NULL, false, 0);

    std::string buffer;
    ::rtidb::api::LogEntry entry;
    bool has_error = false;
    while (true) {
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = reader.ReadRecord(&record, &buffer);
        if (status.IsEof()) {
            break;
        }
        if (!status.ok()) {
            PDLOG(WARNING, "fail to read record for tid %u, pid %u with error %s", tid_, pid_, status.ToString().c_str());
            has_error = true;        
            break;
        }
        if (!entry.ParseFromString(record.ToString())) {
            PDLOG(WARNING, "fail parse record for tid %u, pid %u with value %s", tid_, pid_,
                    ::rtidb::base::DebugString(record.ToString()).c_str());
            has_error = true;        
            break;
        }
        if (entry.dimensions_size() == 0) {
            std::string combined_key = entry.pk() + "|0";
            if (deleted_keys_.find(combined_key) != deleted_keys_.end()) {
                deleted_key_num++;
                continue;
            }
        } else {
            std::set<int> deleted_pos_set;
            for (int pos = 0; pos < entry.dimensions_size(); pos++) {
                std::string combined_key = entry.dimensions(pos).key() + "|" + 
                        std::to_string(entry.dimensions(pos).idx());
                if (deleted_keys_.find(combined_key) != deleted_keys_.end()) {
                    deleted_pos_set.insert(pos);
                }
            }
            if (!deleted_pos_set.empty()) {
                if ((int)deleted_pos_set.size() == entry.dimensions_size()) {
                    deleted_key_num++;
                    continue;
                } else {
                    ::rtidb::api::LogEntry tmp_entry(entry);
                    entry.clear_dimensions();
                    for (int pos = 0; pos < tmp_entry.dimensions_size(); pos++) {
                        if (deleted_pos_set.find(pos) == deleted_pos_set.end()) {
                            ::rtidb::api::Dimension* dimension = entry.add_dimensions();
                            dimension->CopyFrom(tmp_entry.dimensions(pos));
                        }
                    }
                    std::string tmp_buf;
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
        status = wh->Write(record);
        if (!status.ok()) {
            PDLOG(WARNING, "fail to write snapshot. status[%s]", 
                          status.ToString().c_str());
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
        PDLOG(WARNING, "key num not match! total key num[%lu] load key num[%lu] ttl key num[%lu]",
                    manifest.count(), count, expired_key_num);
        has_error = true;
    }
    if (has_error) {
        return -1;
    }    
    PDLOG(INFO, "load snapshot success. load key num[%lu] ttl key num[%lu]", count, expired_key_num); 
    return 0;
}

uint64_t MemTableSnapshot::CollectDeletedKey() {
    deleted_keys_.clear();
    ::rtidb::log::LogReader log_reader(log_part_, log_path_);
    log_reader.SetOffset(offset_);
    uint64_t cur_offset = offset_;
    std::string buffer;
    while (true) {
        if (deleted_keys_.size() >= FLAGS_make_snapshot_max_deleted_keys) {
            PDLOG(WARNING, "deleted_keys map size reach the make_snapshot_max_deleted_keys %u, tid %u pid %u",
                            FLAGS_make_snapshot_max_deleted_keys, tid_, pid_);
            break;
        }
        buffer.clear();
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::rtidb::api::LogEntry entry;
            if (!entry.ParseFromString(record.ToString())) {
                PDLOG(WARNING, "fail to parse LogEntry. record[%s] size[%ld]",
                        ::rtidb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                break;
            }
            if (entry.log_index() <= cur_offset) {
                continue;
            }
            if (cur_offset + 1 != entry.log_index()) {
                PDLOG(WARNING, "log missing expect offset %lu but %ld", cur_offset + 1, entry.log_index());
                continue;
            }
            cur_offset = entry.log_index();
            if (entry.has_method_type() && entry.method_type() == ::rtidb::api::MethodType::kDelete) {
                if (entry.dimensions_size() == 0) {
                    PDLOG(WARNING, "no dimesion. tid %u pid %u offset %lu", tid_, pid_, cur_offset);
                    continue;
                }
                std::string combined_key = entry.dimensions(0).key() + "|" + std::to_string(entry.dimensions(0).idx());
                deleted_keys_[combined_key] = cur_offset;
                PDLOG(DEBUG, "insert key %s offset %lu. tid %u pid %u", combined_key.c_str(), cur_offset, tid_, pid_);
            }
        } else if (status.IsEof()) {
            continue;
        } else if (status.IsWaitRecord()) {
            int end_log_index = log_reader.GetEndLogIndex();
            int cur_log_index = log_reader.GetLogIndex();
            // judge end_log_index greater than cur_log_index
            if (end_log_index >= 0 && end_log_index > cur_log_index) {
                log_reader.RollRLogFile();
                PDLOG(WARNING, "read new binlog file. tid[%u] pid[%u] cur_log_index[%d] end_log_index[%d] cur_offset[%lu]", 
                                tid_, pid_, cur_log_index, end_log_index, cur_offset);
                continue;
            }
            PDLOG(DEBUG, "has read all record!");
            break;
        } else {
            PDLOG(WARNING, "fail to get record. status is %s", status.ToString().c_str());
            break;
        }
    }
    return cur_offset;
}

int MemTableSnapshot::MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset) {
    if (making_snapshot_.load(std::memory_order_acquire)) {
        PDLOG(INFO, "snapshot is doing now!");
        return 0;
    }
    making_snapshot_.store(true, std::memory_order_release);
    std::string now_time = ::rtidb::base::GetNowTime();
    std::string snapshot_name = now_time.substr(0, now_time.length() - 2) + ".sdb";
    std::string snapshot_name_tmp = snapshot_name + ".tmp";
    std::string full_path = snapshot_path_ + snapshot_name;
    std::string tmp_file_path = snapshot_path_ + snapshot_name_tmp;
    FILE* fd = fopen(tmp_file_path.c_str(), "ab+");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to create file %s", tmp_file_path.c_str());
        making_snapshot_.store(false, std::memory_order_release);
        return -1;
    }
    uint64_t collected_offset = CollectDeletedKey();
    uint64_t start_time = ::baidu::common::timer::now_time();
    WriteHandle* wh = new WriteHandle(snapshot_name_tmp, fd);
    ::rtidb::api::Manifest manifest;
    bool has_error = false;
    uint64_t write_count = 0;
    uint64_t expired_key_num = 0;
    uint64_t deleted_key_num = 0;
    uint64_t last_term = 0;
    int result = GetLocalManifest(snapshot_path_ + MANIFEST, manifest);
    if (result == 0) {
        // filter old snapshot
        if (TTLSnapshot(table, manifest, wh, write_count, expired_key_num, deleted_key_num) < 0) {
            has_error = true;
        }
        last_term = manifest.term();
        PDLOG(DEBUG, "old manifest term is %lu", last_term);
    } else if (result < 0) {
        // parse manifest error
        has_error = true;
    }

    ::rtidb::log::LogReader log_reader(log_part_, log_path_);
    log_reader.SetOffset(offset_);
    uint64_t cur_offset = offset_;
    std::string buffer;
    while (!has_error && cur_offset < collected_offset) {
        buffer.clear();
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::rtidb::api::LogEntry entry;
            if (!entry.ParseFromString(record.ToString())) {
                PDLOG(WARNING, "fail to parse LogEntry. record[%s] size[%ld]",
                        ::rtidb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                has_error = true;
                break;
            }
            if (entry.log_index() <= cur_offset) {
                continue;
            }
            if (cur_offset + 1 != entry.log_index()) {
                PDLOG(WARNING, "log missing expect offset %lu but %ld", cur_offset + 1, entry.log_index());
                continue;
            }
            cur_offset = entry.log_index();
            if (entry.has_method_type() && entry.method_type() == ::rtidb::api::MethodType::kDelete) { 
                continue;
            }
            if (entry.has_term()) {
                last_term = entry.term();
            }
            if (entry.dimensions_size() == 0) {
                std::string combined_key = entry.pk() + "|0";
                auto iter = deleted_keys_.find(combined_key);
                if (iter != deleted_keys_.end() && cur_offset <= iter->second) {
                    PDLOG(DEBUG, "delete key %s  offset %lu", entry.pk().c_str(), entry.log_index());
                    deleted_key_num++;
                    continue;
                }
            } else {
                std::set<int> deleted_pos_set;
                for (int pos = 0; pos < entry.dimensions_size(); pos++) {
                    std::string combined_key = entry.dimensions(pos).key() + "|" + 
                            std::to_string(entry.dimensions(pos).idx());
                    auto iter = deleted_keys_.find(combined_key);
                    if (iter != deleted_keys_.end() && cur_offset <= iter->second) {
                        deleted_pos_set.insert(pos);
                    }
                }
                if (!deleted_pos_set.empty()) {
                    if ((int)deleted_pos_set.size() == entry.dimensions_size()) {
                        deleted_key_num++;
                        continue;
                    } else {
                        ::rtidb::api::LogEntry tmp_entry(entry);
                        entry.clear_dimensions();
                        for (int pos = 0; pos < tmp_entry.dimensions_size(); pos++) {
                            if (deleted_pos_set.find(pos) == deleted_pos_set.end()) {
                                ::rtidb::api::Dimension* dimension = entry.add_dimensions();
                                dimension->CopyFrom(tmp_entry.dimensions(pos));
                            }
                        }
                        std::string tmp_buf;
                        entry.SerializeToString(&tmp_buf);
                        record.reset(tmp_buf.data(), tmp_buf.size());
                    }
                }
            }
            if (table->IsExpire(entry)) {
                expired_key_num++;
                continue;
            }
            ::rtidb::base::Status status = wh->Write(record);
            if (!status.ok()) {
                PDLOG(WARNING, "fail to write snapshot. path[%s] status[%s]",
                tmp_file_path.c_str(), status.ToString().c_str());
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
                PDLOG(WARNING, "read new binlog file. tid[%u] pid[%u] cur_log_index[%d] end_log_index[%d] cur_offset[%lu]", 
                                tid_, pid_, cur_log_index, end_log_index, cur_offset);
                continue;
            }
            PDLOG(DEBUG, "has read all record!");
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
                    PDLOG(DEBUG, "old snapshot[%s] has deleted", manifest.name().c_str());
                    unlink((snapshot_path_ + manifest.name()).c_str());
                }
                uint64_t consumed = ::baidu::common::timer::now_time() - start_time;
                PDLOG(INFO, "make snapshot[%s] success. update offset from %lu to %lu."
                            "use %lu second. write key %lu expired key %lu deleted key %lu",
                          snapshot_name.c_str(), offset_, cur_offset, consumed, write_count, expired_key_num, deleted_key_num);
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

}
}
