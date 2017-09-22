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
#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"
#include "logging.h"
#include "timer.h"
#include <unistd.h>

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_string(db_root_path);

namespace rtidb {
namespace storage {

const uint32_t MAX_LINE = 1024;
const std::string MANIFEST = "MANIFEST";

Snapshot::Snapshot(uint32_t tid, uint32_t pid):tid_(tid), pid_(pid),
     offset_(0) {}

Snapshot::~Snapshot() {}

bool Snapshot::Init() {
    snapshot_path_ = FLAGS_db_root_path + "/" + boost::lexical_cast<std::string>(tid_) + "_" + boost::lexical_cast<std::string>(pid_) + "/snapshot/";
    bool ok = ::rtidb::base::MkdirRecur(snapshot_path_);
    if (!ok) {
        LOG(WARNING, "fail to create db meta path %s", snapshot_path_.c_str());
        return false;
    }
    return true;
}

bool Snapshot::Recover(Table* table) {
    //TODO multi thread recover
    return true;
}

int Snapshot::MakeSnapshot() {
    /*if (making_snapshot_.load(boost::memory_order_acquire)) {
        LOG(INFO, "snapshot is doing now!");
        return 0;
    }
    std::string now_time = ::rtidb::base::GetNowTime();
    std::string snapshot_name = now_time.substr(0, now_time.length() - 2) + ".sdb";
    std::string full_path = snapshot_path_ + snapshot_name;
    FILE* fd = fopen(full_path.c_str(), "ab+");
    if (fd == NULL) {
        LOG(WARNING, "fail to create file %s", full_path.c_str());
        return -1;
    }
    wh_ = new ::rtidb::log::WriteHandle(snapshot_name, fd);
    
    std::string buffer;
    std::string last_record;
    uint64_t write_count = 0;
    while (1) {
        buffer.clear();
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = log_reader_->ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::rtidb::base::Status status = wh_->Write(record);
            if (!status.ok()) {
                LOG(WARNING, "fail to write snapshot. path[%s] status[%s]", 
                full_path.c_str(), status.ToString().c_str());
                break;
            }
            write_count++;
            if (record.empty()) {
                last_record = record.ToString(); 
            }
        } else if (status.IsEof()) {
            continue;
        } else if (status.IsWaitRecord()) {
            LOG(DEBUG, "has read all recored!");
            break;
        } else {
            LOG(WARNING, "fail to get record. status is %s", status.ToString().c_str());
            break;
        }
    }
    if (wh_ != NULL) {
        wh_->EndLog();
        delete wh_;
        wh_ = NULL;
    }

    RecordOffset(last_record, write_count);
    making_snapshot_.store(false, boost::memory_order_release);*/
    return 0;
}

int Snapshot::RecordOffset(const std::string& log_entry, 
        const std::string& snapshot_name, uint64_t key_count) {
    ::rtidb::api::LogEntry entry;
    if (!entry.ParseFromString(log_entry)) {
        LOG(WARNING, "fail to parse LogEntry");
        return -1;
    }
    offset_ = entry.log_index();
    std::string full_path = snapshot_path_ + MANIFEST;
    std::string tmp_file = snapshot_path_ + MANIFEST + ".tmp";
    FILE* fd_write = fopen(tmp_file.c_str(), "w");
    if (fd_write == NULL) {
        LOG(WARNING, "fail to open file %s", tmp_file.c_str());
        return -1;
    }
    char buffer[MAX_LINE];
    snprintf(buffer, MAX_LINE, "offset:%lu\n", offset_);
    bool io_error = false;
    if (fputs(buffer, fd_write) == EOF || 
        fputs(std::string(snapshot_name + ":" + 
            boost::lexical_cast<std::string>(key_count) + "\n").c_str(), fd_write) == EOF) {
        LOG(WARNING, "write error. path[%s]", tmp_file.c_str());
        io_error = true;
    }
    FILE* fd_read = fopen(full_path.c_str(), "r");
    if (fd_read == NULL) {
        LOG(INFO, "[%s] is not exisit", MANIFEST.c_str());
    } else if (!io_error) {
        while (!feof(fd_read)) {
            if (fgets(buffer, MAX_LINE, fd_read) == NULL && !feof(fd_read)) {
                LOG(WARNING, "read error. path[%s]", full_path.c_str());
                io_error = true;
                break;
            }
            if (fputs(buffer, fd_write) == EOF) {
                LOG(WARNING, "write error. path[%s]", tmp_file.c_str());
                io_error = true;
                break;
            }
        }
        fclose(fd_read);
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



