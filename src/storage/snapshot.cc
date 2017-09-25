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
#include "base/status.h"
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

bool Snapshot::Recover(Table* table, RecoverStat& stat) {
    ::rtidb::api::Manifest manifest;
    int ret = ReadManifest(&manifest);
    if (ret == -1) {
        return false;
    }
    bool ok = RecoverFromSnapshot(table, stat);
    if (!ok) {
        LOG(WARNING, "fail to recover from snapshot for tid %u, pid %u", tid_, pid_);
        return false;
    }
    ok = RecoverFromBinlog(Table* table, RecoverStat& stat, manifest.offset());
    return true;
}

bool Snapshot::RecoverFromBinlog(Table* table, RecoverStat& stat, uint64_t offset) {
    ::rtidb::log::LogReader log_reader(log_part_, log_path_);
    log_reader.SetOffset(offset);

}

bool Snapshot::RecoverFromSnapshot(Table* table, RecoverStat& stat) {
    std::vector<std::string> file_list;
    int ok = ::rtidb::base::GetFileName(snapshot_path_, file_list);
    if (ok != 0) {
        LOG(WARNING, "fail to get snapshot list from path %s for tid %u, pid %u", snapshot_path_.c_str(), tid_, pid_);
        return false;
    }
    std::vector<std::string> snapshots;
    GetSnapshots(file_list, snapshots);
    if (snapshots.size() <= 0) {
        LOG(INFO, "no snapshots to recover for tid %u, pid %u from path %s, file list size %u", tid_, pid_, 
                snapshot_path_.c_str(),
                file_list.size());
        return true;
    }
    std::atomic<uint64_t> g_succ_cnt(0);
    std::atomic<uint64_t> g_failed_cnt(0);
    ::baidu::common::ThreadPool pool(FLAGS_recover_table_thread_size);
    for (uint32_t i = 0; i <= snapshots.size() / FLAGS_recover_table_thread_size; i++) {
        uint32_t start = i;
        uint32_t end = (i + 1) * FLAGS_recover_table_thread_size;
        if (end > snapshots.size()) {
            end = snapshots.size();
        }
        ::rtidb::base::CountDownLatch latch(end - start);
        for (uint32_t j = start; j < end; j++) {
            pool.AddTask(boost::bind(&Snapshot::RecoverSingleSnapshot, this, snapshots[j], table,
                        &g_succ_cnt, &g_failed_cnt, &latch));
        }
        while (latch.GetCount() > 0) {
            latch.TimeWait(8000);
            LOG(INFO, "[Recover] progressing stat: success count %lu, failed count %lu", 
                    g_succ_cnt.load(std::memory_order_relaxed),
                    g_failed_cnt.load(std::memory_order_relaxed));
        }
    }
    LOG(INFO, "[Recover] progress done stat: success count %lu, failed count %lu", 
                    g_succ_cnt.load(std::memory_order_relaxed),
                    g_failed_cnt.load(std::memory_order_relaxed));
    stat.succ_cnt = g_succ_cnt.load(std::memory_order_relaxed);
    stat.failed_cnt = g_failed_cnt.load(std::memory_order_relaxed);
    return true;
}

void Snapshot::GetSnapshots(const std::vector<std::string>& files,
                            std::vector<std::string>& snapshots) {
    for (uint32_t i = 0; i < files.size(); i++) {
        const std::string& path = files[i];
        // check subfix .sdb
        if (path.length() <= SNAPSHOT_SUBFIX.length()) {
            LOG(DEBUG, "not valid snapshot %s", path.c_str());
            continue;
        }
        std::string subfix = path.substr(path.length() - SNAPSHOT_SUBFIX.length(),
                                         std::string::npos);
        if (subfix != SNAPSHOT_SUBFIX) {
            LOG(DEBUG, "not valid snapshot %s", path.c_str());
            continue;
        }
        snapshots.push_back(path);
        LOG(DEBUG, "valid snapshot %s", path.c_str());
    }
}

void Snapshot::RecoverSingleSnapshot(const std::string& path, Table* table, 
                                     std::atomic<uint64_t>* g_succ_cnt,
                                     std::atomic<uint64_t>* g_failed_cnt,
                                     ::rtidb::base::CountDownLatch* latch) {
    if (latch == NULL) {
        LOG(WARNING, "latch input is NULL");
        return;
    }
    do {
        if (table == NULL) {
            LOG(WARNING, "table input is NULL");
            break;
        }
        FILE* fd = fopen(path.c_str(), "r+");
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
    latch->CountDown();
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
    while (1) {
        buffer.clear();
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::rtidb::api::LogEntry entry;
            if (!entry.ParseFromString(record.ToString())) {
                LOG(WARNING, "fail to parse LogEntry. record[%s] size[%ld]", 
                        ::rtidb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                break;
            }
            if (entry.log_index() <= cur_offset) {
                continue;
            }
            ::rtidb::base::Status status = wh->Write(record);
            if (!status.ok()) {
                LOG(WARNING, "fail to write snapshot. path[%s] status[%s]", 
                tmp_file_path.c_str(), status.ToString().c_str());
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
            break;
        }
    }
    if (wh != NULL) {
        wh->EndLog();
        delete wh;
        wh = NULL;
    }
    int ret = 0;
    if (rename(tmp_file_path.c_str(), full_path.c_str()) == 0) {
        if (RecordOffset(snapshot_name, write_count, cur_offset) == 0) {
            LOG(INFO, "make snapshot[%s] success. update offset from[%lu] to [%lu]", 
                      snapshot_name.c_str(), offset_, cur_offset);
            offset_ = cur_offset;
        } else {
            ret = -1;
        }
    } else {
        LOG(WARNING, "rename[%s] failed", snapshot_name.c_str());
        ret = -1;
    }
    making_snapshot_.store(false, boost::memory_order_release);
    return ret;
}

int Snapshot::ReadManifest(::rtidb::api::Manifest* manifest) {
    if (manifest == NULL) {
        return -1;
    }
    std::string full_path = snapshot_path_ + MANIFEST;
    int fd = open(full_path.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(INFO, "[%s] is not exisit", MANIFEST.c_str());
        return 1;
    } else {
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        bool ok = google::protobuf::TextFormat::Parse(&fileInput, manifest);
        if (!ok) {
            LOG(WARNING, "fail to open MANIFEST for tid %u, pid %u", tid_, pid_);
            return -1;
        }
        return 0;
    }

}

int Snapshot::RecordOffset(const std::string& snapshot_name, uint64_t key_count, uint64_t offset) {
    LOG(DEBUG, "record offset[%lu]. add snapshot[%s] key_count[%lu]",
                offset, snapshot_name.c_str(), key_count);
    std::string full_path = snapshot_path_ + MANIFEST;
    std::string tmp_file = snapshot_path_ + MANIFEST + ".tmp";
    ::rtidb::api::Manifest manifest;
    int ret = ReadManifest(&manifest);
    if (ret == -1) {
        return -1;
    }
    manifest.set_offset(offset);
    std::string manifest_info;
    ::rtidb::api::SnapshotInfo* snap_info = manifest.add_snapshot_infos();
    snap_info->set_name(snapshot_name);
    snap_info->set_count(key_count);
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
    return -1;
}

}
}



