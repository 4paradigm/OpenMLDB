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
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_string(db_root_path);

namespace rtidb {
namespace storage {

const uint32_t MAX_LINE = 1024;
const std::string MANIFEST = "MANIFEST";

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

bool Snapshot::Recover(Table* table) {
    //TODO multi thread recover
    return true;
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
    std::string tmp_file = snapshot_path_ + MANIFEST + ".tmp";
    int fd = open(full_path.c_str(), O_RDONLY);
    std::string manifest_info;
    if (fd < 0) {
        LOG(INFO, "[%s] is not exisit", MANIFEST.c_str());
        return -1;
    } else {
        google::protobuf::io::FileInputStream fileInput(fd);
        // will close fd when destruct
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



