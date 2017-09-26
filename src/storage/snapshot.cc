//
// snapshot.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-24
//
//
#include "storage/snapshot.h"

#include "base/file_util.h"
#include "base/strings.h"
#include "base/slice.h"
#include "base/count_down_latch.h"
#include "log/sequential_file.h"
#include "log/log_reader.h"
#include "boost/lexical_cast.hpp"
#include "proto/tablet.pb.h"
#include "gflags/gflags.h"
#include "logging.h"
#include "timer.h"
#include "thread_pool.h"
#include <unistd.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_string(db_root_path);
DECLARE_int32(recover_table_thread_size);

namespace rtidb {
namespace storage {

const std::string SNAPSHOT_SUBFIX=".sdb";
const std::string MANIFEST = "MANIFEST";
const uint32_t MAX_LINE = 1024;

Snapshot::Snapshot(uint32_t tid, uint32_t pid, LogParts* log_part):tid_(tid), pid_(pid),
     log_part_(log_part) {
    offset_ = 0;
}

Snapshot::~Snapshot() {
}

bool Snapshot::Init() {
    snapshot_path_ = FLAGS_db_root_path + "/" + boost::lexical_cast<std::string>(tid_) + "_" + boost::lexical_cast<std::string>(pid_) + "/snapshot/";
    log_path_ = FLAGS_db_root_path + "/" + boost::lexical_cast<std::string>(tid_) + "_" + boost::lexical_cast<std::string>(pid_) + "/binlog/";
    if (!::rtidb::base::MkdirRecur(snapshot_path_)) {
        LOG(WARNING, "fail to create db meta path %s", snapshot_path_.c_str());
        return false;
    }
    if (!::rtidb::base::MkdirRecur(log_path_)) {
        LOG(WARNING, "fail to create db meta path %s", log_path_.c_str());
        return false;
    }
    making_snapshot_.store(false, boost::memory_order_release);
    return true;
}

bool Snapshot::Recover(Table* table, uint64_t& latest_offset) {
    ::rtidb::api::Manifest manifest;
    manifest.set_offset(0);
    int ret = GetSnapshotRecord(manifest);
    if (ret == -1) {
        return false;
    }
    if (ret == 0) {
        RecoverFromSnapshot(manifest.name(), manifest.count(), table);
        latest_offset = manifest.offset();
    }
    return RecoverFromBinlog(table, manifest.offset(), latest_offset);
}

bool Snapshot::RecoverFromBinlog(Table* table, uint64_t offset,
                                 uint64_t& latest_offset) {
    LOG(INFO, "start recover table tid %u, pid %u from binlog with start offset %lu",
            table->GetId(), table->GetPid(), offset);
    ::rtidb::log::LogReader log_reader(log_part_, log_path_);
    log_reader.SetOffset(offset);
    ::rtidb::api::LogEntry entry;
    uint64_t cur_offset = offset;
    std::string buffer;
    uint64_t succ_cnt = 0;
    uint64_t failed_cnt = 0;
    uint64_t consumed = ::baidu::common::timer::now_time();
    while (true) {
        buffer.clear();
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.IsWaitRecord()) {
            consumed = ::baidu::common::timer::now_time() - consumed;
            LOG(INFO, "table tid %u pid %u completed, succ_cnt %lu, failed_cnt %lu, consumed %us",
                       tid_, pid_, succ_cnt, failed_cnt, consumed);
            break;
        }

        if (status.IsEof()) {
            continue;
        }

        if (!status.ok()) {
            failed_cnt++;
            continue;
        }

        bool ok = entry.ParseFromString(record.ToString());
        if (!ok) {
            LOG(WARNING, "fail parse record for tid %u, pid %u with value %s", tid_, pid_,
                        ::rtidb::base::DebugString(record.ToString()).c_str());
            failed_cnt++;
            continue;
        }

        if (cur_offset <= entry.log_index()) {
            continue;
        }

        if (cur_offset + 1 != entry.log_index()) {
            LOG(WARNING, "missing log entry cur_offset %lu , new entry offset %lu for tid %u, pid %u",
                    cur_offset, entry.log_index(), tid_, pid_);
        }
        table->Put(entry.pk(), entry.ts(), entry.value().c_str(), entry.value().size());
        cur_offset = entry.log_index();
        succ_cnt++;
        if (succ_cnt % 100000 == 0) {
            LOG(INFO, "[Recover] load data from binlog succ_cnt %lu, failed_cnt %lu for tid %u, pid %u",
                    succ_cnt, failed_cnt, tid_, pid_);
        }
    }
    latest_offset = cur_offset;
    return true;
}

void Snapshot::RecoverFromSnapshot(const std::string& snapshot_name, uint64_t expect_cnt, Table* table) {
    std::string full_path = snapshot_path_ + "/" + snapshot_name;
    std::atomic<uint64_t> g_succ_cnt(0);
    std::atomic<uint64_t> g_failed_cnt(0);
    RecoverSingleSnapshot(full_path, table, &g_succ_cnt, &g_failed_cnt);
    LOG(INFO, "[Recover] progress done stat: success count %lu, failed count %lu", 
                    g_succ_cnt.load(std::memory_order_relaxed),
                    g_failed_cnt.load(std::memory_order_relaxed));
    if (g_succ_cnt.load(std::memory_order_relaxed) != expect_cnt) {
        LOG(WARNING, "snapshot %s , expect cnt %lu but succ_cnt %lu", snapshot_name.c_str(),
                expect_cnt, g_succ_cnt.load(std::memory_order_relaxed));
    }
}


void Snapshot::RecoverSingleSnapshot(const std::string& path, Table* table, 
                                     std::atomic<uint64_t>* g_succ_cnt,
                                     std::atomic<uint64_t>* g_failed_cnt) {
    do {
        if (table == NULL) {
            LOG(WARNING, "table input is NULL");
            break;
        }
        FILE* fd = fopen(path.c_str(), "rb+");
        if (fd == NULL) {
            LOG(WARNING, "fail to open path %s for error %s", path.c_str(), strerror(errno));
            break;
        }
        ::rtidb::log::SequentialFile* seq_file = ::rtidb::log::NewSeqFile(path, fd);
        ::rtidb::log::Reader reader(seq_file, NULL, false, 0);
        std::string buffer;
        ::rtidb::api::LogEntry entry;
        uint64_t succ_cnt = 0;
        uint64_t failed_cnt = 0;
        // second
        uint64_t consumed = ::baidu::common::timer::now_time();
        while (true) {
            ::rtidb::base::Slice record;
            ::rtidb::base::Status status = reader.ReadRecord(&record, &buffer);
            if (status.IsWaitRecord() || status.IsEof()) {
                consumed = ::baidu::common::timer::now_time() - consumed;
                LOG(INFO, "read path %s for table tid %u pid %u completed, succ_cnt %lu, failed_cnt %lu, consumed %us",
                        path.c_str(), tid_, pid_, succ_cnt, failed_cnt, consumed);
                break;
            }
            if (!status.ok()) {
                LOG(WARNING, "fail to read record for tid %u, pid %u with error %s", tid_, pid_, status.ToString().c_str());
                failed_cnt++;
                continue;
            }
            bool ok = entry.ParseFromString(record.ToString());
            if (!ok) {
                LOG(WARNING, "fail parse record for tid %u, pid %u with value %s", tid_, pid_,
                        ::rtidb::base::DebugString(record.ToString()).c_str());
                failed_cnt++;
                continue;
            }
            succ_cnt++;
            if (succ_cnt % 100000 == 0) { 
                LOG(INFO, "load snapshot %s with succ_cnt %lu, failed_cnt %lu", path.c_str(),
                        succ_cnt, failed_cnt);
            }
            table->Put(entry.pk(), entry.ts(), entry.value().c_str(), entry.value().size());
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
}

int Snapshot::MakeSnapshot() {
    if (making_snapshot_.load(boost::memory_order_acquire)) {
        LOG(INFO, "snapshot is doing now!");
        return 0;
    }
    making_snapshot_.store(true, boost::memory_order_release);
    std::string now_time = ::rtidb::base::GetNowTime();
    std::string snapshot_name = now_time.substr(0, now_time.length() - 2) + ".sdb";
    std::string snapshot_name_tmp = snapshot_name + ".tmp";
    std::string full_path = snapshot_path_ + snapshot_name;
    std::string tmp_file_path = snapshot_path_ + snapshot_name_tmp;
    FILE* fd = fopen(tmp_file_path.c_str(), "ab+");
    if (fd == NULL) {
        LOG(WARNING, "fail to create file %s", tmp_file_path.c_str());
        making_snapshot_.store(false, boost::memory_order_release);
        return -1;
    }
    WriteHandle* wh = new WriteHandle(snapshot_name_tmp, fd);
    ::rtidb::log::LogReader log_reader(log_part_, log_path_);
    log_reader.SetOffset(offset_);
    uint64_t cur_offset = offset_;
    std::string buffer;
    uint64_t write_count = 0;
    bool has_error = false;
    while (1) {
        buffer.clear();
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::rtidb::api::LogEntry entry;
            if (!entry.ParseFromString(record.ToString())) {
                LOG(WARNING, "fail to parse LogEntry. record[%s] size[%ld]", 
                        ::rtidb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                has_error = true;        
                break;
            }
            if (entry.log_index() <= cur_offset) {
                continue;
            }
            ::rtidb::base::Status status = wh->Write(record);
            if (!status.ok()) {
                LOG(WARNING, "fail to write snapshot. path[%s] status[%s]", 
                tmp_file_path.c_str(), status.ToString().c_str());
                has_error = true;        
                break;
            }
            cur_offset = entry.log_index();
            write_count++;
        } else if (status.IsEof()) {
            continue;
        } else if (status.IsWaitRecord()) {
            LOG(DEBUG, "has read all record!");
            break;
        } else {
            LOG(WARNING, "fail to get record. status is %s", status.ToString().c_str());
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
        if (unlink(tmp_file_path.c_str()) < 0) {
            LOG(WARNING, "delete file [%s] failed", tmp_file_path.c_str());
        }
        ret = -1;
    } else {
        if (rename(tmp_file_path.c_str(), full_path.c_str()) == 0) {
            if (RecordOffset(snapshot_name, write_count, cur_offset) == 0) {
                LOG(INFO, "make snapshot[%s] success. update offset from[%lu] to [%lu]", 
                          snapshot_name.c_str(), offset_, cur_offset);
                offset_ = cur_offset;
            } else {
                LOG(WARNING, "RecordOffset failed. delete snapshot file[%s]", full_path.c_str());
                if (unlink(full_path.c_str()) < 0) {
                    LOG(WARNING, "delete file [%s] failed", full_path.c_str());
                }
                ret = -1;
            }
        } else {
            LOG(WARNING, "rename[%s] failed", snapshot_name.c_str());
            if (unlink(tmp_file_path.c_str()) < 0) {
                LOG(WARNING, "delete file [%s] failed", tmp_file_path.c_str());
            }
            ret = -1;
        }
    }
    making_snapshot_.store(false, boost::memory_order_release);
    return ret;
}

int Snapshot::GetSnapshotRecord(::rtidb::api::Manifest& manifest) {
    std::string full_path = snapshot_path_ + MANIFEST;
    int fd = open(full_path.c_str(), O_RDONLY);
    std::string manifest_info;
    if (fd < 0) {
        LOG(INFO, "[%s] is not exisit", MANIFEST.c_str());
        return 1;
    } else {
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        if (!google::protobuf::TextFormat::Parse(&fileInput, &manifest)) {
            LOG(WARNING, "parse manifest failed");
            return -1;
        }
    }
    return 0;
}

int Snapshot::RecordOffset(const std::string& snapshot_name, uint64_t key_count, uint64_t offset) {
    LOG(DEBUG, "record offset[%lu]. add snapshot[%s] key_count[%lu]",
                offset, snapshot_name.c_str(), key_count);
    std::string full_path = snapshot_path_ + MANIFEST;
    std::string tmp_file = snapshot_path_ + MANIFEST + ".tmp";
    ::rtidb::api::Manifest manifest;
    std::string manifest_info;
    manifest.set_offset(offset);
    manifest.set_name(snapshot_name);
    manifest.set_count(key_count);
    manifest_info.clear();
    google::protobuf::TextFormat::PrintToString(manifest, &manifest_info);
    FILE* fd_write = fopen(tmp_file.c_str(), "w");
    if (fd_write == NULL) {
        LOG(WARNING, "fail to open file %s", tmp_file.c_str());
        return -1;
    }
    bool io_error = false;
    if (fputs(manifest_info.c_str(), fd_write) == EOF) {
        LOG(WARNING, "write error. path[%s]", tmp_file.c_str());
        io_error = true;
    }
    if (!io_error && ((fflush(fd_write) == EOF) || fsync(fileno(fd_write)) == -1)) {
        LOG(WARNING, "flush error. path[%s]", tmp_file.c_str());
        io_error = true;
    }
    fclose(fd_write);
    if (!io_error && rename(tmp_file.c_str(), full_path.c_str()) == 0) {
        LOG(DEBUG, "%s generate success. path[%s]", MANIFEST.c_str(), full_path.c_str());
        return 0;
    }
    if (unlink(tmp_file.c_str()) < 0) {
        LOG(WARNING, "delete tmp file[%s] failed", tmp_file.c_str());
    }
    return -1;
}

}
}



