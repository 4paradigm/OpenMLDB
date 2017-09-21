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
#include "log/sequential_file.h"
#include "log/log_reader.h"
#include "boost/lexical_cast.hpp"
#include "proto/tablet.pb.h"
#include "gflags/gflags.h"
#include "logging.h"
#include "timer.h"

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_string(db_root_path);
DECLARE_int32(recover_table_thread_size);

namespace rtidb {
namespace storage {

Snapshot::Snapshot(uint32_t tid, uint32_t pid):tid_(tid), pid_(pid),
     offset_(0) {}

Snapshot::~Snapshot() {}

bool Snapshot::Init() {
    std::string snapshot_path = FLAGS_db_root_path + "/" + boost::lexical_cast<std::string>(tid_) + "_" + boost::lexical_cast<std::string>(pid_);
    bool ok = ::rtidb::base::MkdirRecur(snapshot_path);
    if (!ok) {
        LOG(WARNING, "fail to create db meta path %s", snapshot_path.c_str());
        return false;
    }
    return true;
}

bool Snapshot::Recover(Table* table) {
    std::vector<std::string> file_list;
    std::string snapshot_path = FLAGS_db_root_path + "/" + boost::lexical_cast<std::string>(tid_) + "_" + boost::lexical_cast<std::string>(pid_);
    int ok = ::rtidb::base::GetFileName(snapshot_path, file_list);
    if (ok != 0) {
        LOG(WARNING, "fail to get snapshot list for tid %u, pid %u", tid_, pid_);
        return false;
    }
    return true;
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
            if (status.IsWaitRecord()) {
                consumed = ::baidu::common::timer::now_time();
                LOG(INFO, "read path %s for table tid %u pid %u completed, succ_cnt %lu, failed_cnt %lu, consumed %u",
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



