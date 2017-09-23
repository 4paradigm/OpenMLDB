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


using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_string(db_root_path);
DECLARE_int32(recover_table_thread_size);

namespace rtidb {
namespace storage {

const std::string META_NAME="MANIFEST";
const std::string SNAPSHOT_SUBFIX=".sdb";

Snapshot::Snapshot(uint32_t tid, uint32_t pid):tid_(tid), pid_(pid),
     offset_(0), path_() {}

Snapshot::~Snapshot() {}

bool Snapshot::Init() {
    path_ = FLAGS_db_root_path + "/" + boost::lexical_cast<std::string>(tid_) + "_" + boost::lexical_cast<std::string>(pid_) + "/snapshots";
    bool ok = ::rtidb::base::MkdirRecur(path_);
    if (!ok) {
        LOG(WARNING, "fail to create db meta path %s", path_.c_str());
        return false;
    }
    return true;
}

bool Snapshot::Recover(Table* table, RecoverStat& stat) {
    std::vector<std::string> file_list;
    int ok = ::rtidb::base::GetFileName(path_, file_list);
    if (ok != 0) {
        LOG(WARNING, "fail to get snapshot list from path %s for tid %u, pid %u", path_.c_str(), tid_, pid_);
        return false;
    }
    std::vector<std::string> snapshots;
    GetSnapshots(file_list, snapshots);
    if (snapshots.size() <= 0) {
        LOG(INFO, "no snapshots to recover for tid %u, pid %u from path %s, file list size %u", tid_, pid_, 
                path_.c_str(),
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

}
}



