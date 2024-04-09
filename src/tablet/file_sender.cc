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

#include "tablet/file_sender.h"

#include <thread>  // NOLINT
#include <vector>

#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "boost/algorithm/string/predicate.hpp"
#include "common/timer.h"
#include "gflags/gflags.h"

DECLARE_int32(send_file_max_try);
DECLARE_uint32(stream_block_size);
DECLARE_int32(stream_bandwidth_limit);
DECLARE_uint32(stream_block_size);
DECLARE_int32(stream_bandwidth_limit);
DECLARE_int32(stream_close_wait_time_ms);
DECLARE_int32(retry_send_file_wait_time_ms);
DECLARE_int32(request_max_retry);
DECLARE_int32(request_timeout_ms);

namespace openmldb {
namespace tablet {

FileSender::FileSender(uint32_t tid, uint32_t pid, common::StorageMode storage_mode, const std::string& endpoint)
    : tid_(tid),
      pid_(pid),
      storage_mode_(storage_mode),
      endpoint_(endpoint),
      cur_try_time_(0),
      max_try_time_(FLAGS_send_file_max_try),
      limit_time_(0),
      channel_(NULL),
      stub_(NULL) {}

FileSender::~FileSender() {
    delete channel_;
    delete stub_;
}

bool FileSender::Init() {
    // compute the used time(microseconds) that send a block by limit bandwidth.
    // limit_time = (FLAGS_stream_block_size / FLAGS_stream_bandwidth_limit) *
    // 1000 * 1000
    if (FLAGS_stream_bandwidth_limit > 0) {
        limit_time_ = (FLAGS_stream_block_size * 1000000) / FLAGS_stream_bandwidth_limit;
    }
    channel_ = new brpc::Channel();
    brpc::ChannelOptions options;
    options.auth = &client_authenticator_;
    options.timeout_ms = FLAGS_request_timeout_ms;
    options.connect_timeout_ms = FLAGS_request_timeout_ms;
    options.max_retry = FLAGS_request_max_retry;
    if (channel_->Init(endpoint_.c_str(), "", &options) != 0) {
        PDLOG(WARNING, "init channel failed. endpoint[%s]", endpoint_.c_str());
        return false;
    }
    stub_ = new ::openmldb::api::TabletServer_Stub(channel_);
    return true;
}

int FileSender::WriteData(const std::string& file_name, const std::string& dir_name, const char* buffer, size_t len,
                          uint64_t block_id) {
    if (buffer == NULL) {
        return -1;
    }
    uint64_t cur_time = ::baidu::common::timer::get_micros();
    ::openmldb::api::SendDataRequest request;
    request.set_tid(tid_);
    request.set_pid(pid_);
    request.set_storage_mode(storage_mode_);
    request.set_file_name(file_name);
    if (!dir_name.empty()) {
        request.set_dir_name(dir_name);
    }
    request.set_block_id(block_id);
    request.set_block_size(len);
    brpc::Controller cntl;
    if (block_id > 0) {
        cntl.request_attachment().append(buffer, len);
    }
    if (len > 0 && len < FLAGS_stream_block_size) {
        request.set_eof(true);
    }
    ::openmldb::api::GeneralResponse response;
    stub_->SendData(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        PDLOG(WARNING, "send data failed. tid %u pid %u file %s error msg %s", tid_, pid_, file_name.c_str(),
              cntl.ErrorText().c_str());
        return -1;
    } else if (response.code() != 0) {
        PDLOG(WARNING, "send data failed. tid %u pid %u file %s error msg %s", tid_, pid_, file_name.c_str(),
              response.msg().c_str());
        return -1;
    }
    uint64_t time_used = ::baidu::common::timer::get_micros() - cur_time;
    if (limit_time_ > time_used && len > FLAGS_stream_block_size / 2) {
        uint64_t sleep_time = limit_time_ - time_used;
        DEBUGLOG("sleep %lu us, limit_time %lu time_used %lu", sleep_time, limit_time_, time_used);
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
    }
    return 0;
}

int FileSender::SendFile(const std::string& file_name, const std::string& full_path) {
    return SendFile(file_name, "", full_path);
}

int FileSender::SendFile(const std::string& file_name, const std::string& dir_name, const std::string& full_path) {
    if (!boost::ends_with(full_path, file_name)) {
        PDLOG(WARNING, "invalid file[%s] path[%s]", file_name.c_str(), full_path.c_str());
        return -1;
    }
    uint64_t file_size = 0;
    if (!::openmldb::base::GetFileSize(full_path, file_size)) {
        PDLOG(WARNING, "get size failed. file[%s]", full_path.c_str());
        return -1;
    }
    PDLOG(INFO, "send file %s to %s. size[%lu]", full_path.c_str(), endpoint_.c_str(), file_size);
    int try_times = FLAGS_send_file_max_try;
    do {
        if (try_times < FLAGS_send_file_max_try) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds((FLAGS_send_file_max_try - try_times) * FLAGS_retry_send_file_wait_time_ms));
            PDLOG(INFO, "retry to send file %s to %s. total size[%lu]", full_path.c_str(), endpoint_.c_str(),
                  file_size);
        }
        try_times--;
        if (SendFileInternal(file_name, dir_name, full_path, file_size) < 0) {
            continue;
        }
        if (CheckFile(file_name, dir_name, file_size) < 0) {
            continue;
        }
        return 0;
    } while (try_times > 0);
    return -1;
}

int FileSender::SendFileInternal(const std::string& file_name, const std::string& dir_name,
                                 const std::string& full_path, uint64_t file_size) {
    FILE* file = fopen(full_path.c_str(), "rb");
    if (file == NULL) {
        PDLOG(WARNING, "fail to open file %s", full_path.c_str());
        return -1;
    }
    char buffer[FLAGS_stream_block_size];

    uint64_t block_num = file_size / FLAGS_stream_block_size + 1;
    uint64_t report_block_num = block_num / 100;
    int ret = 0;
    uint64_t block_count = 0;
    do {
        if (block_count == 0 && WriteData(file_name, dir_name, buffer, 0, block_count) < 0) {
            PDLOG(WARNING, "Init file receiver failed. tid[%u] pid[%u] file %s", tid_, pid_, file_name.c_str());
            ret = -1;
            break;
        }
        block_count++;

#ifdef __APPLE__
        size_t len = fread(buffer, 1, FLAGS_stream_block_size, file);
#else
        size_t len = fread_unlocked(buffer, 1, FLAGS_stream_block_size, file);
#endif
        if (len < FLAGS_stream_block_size) {
            if (feof(file)) {
                if (len > 0) {
                    ret = WriteData(file_name, dir_name, buffer, len, block_count);
                }
                break;
            }
            PDLOG(WARNING, "read file %s error. error message: %s", file_name.c_str(), strerror(errno));
            ret = -1;
            break;
        }
        if (WriteData(file_name, dir_name, buffer, len, block_count) < 0) {
            PDLOG(WARNING, "data write failed. tid[%u] pid[%u] file %s", tid_, pid_, file_name.c_str());
            ret = -1;
            break;
        }
        if (report_block_num == 0 || block_count % report_block_num == 0) {
            PDLOG(INFO,
                  "send block num[%lu] total block num[%lu]. tid[%u] pid[%u] "
                  "file[%s] endpoint[%s]",
                  block_count, block_num, tid_, pid_, file_name.c_str(), endpoint_.c_str());
        }
    } while (true);
    fclose(file);
    std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_stream_close_wait_time_ms));
    return ret;
}

int FileSender::CheckFile(const std::string& file_name, const std::string& dir_name, uint64_t file_size) {
    ::openmldb::api::CheckFileRequest check_request;
    ::openmldb::api::GeneralResponse response;
    check_request.set_tid(tid_);
    check_request.set_pid(pid_);
    check_request.set_file(file_name);
    check_request.set_storage_mode(storage_mode_);
    if (!dir_name.empty()) {
        check_request.set_dir_name(dir_name);
    }
    check_request.set_size(file_size);
    brpc::Controller cntl;
    stub_->CheckFile(&cntl, &check_request, &response, NULL);
    if (cntl.Failed()) {
        PDLOG(WARNING, "check file[%s] request failed. tid[%u] pid[%u] error msg: %s", file_name.c_str(), tid_, pid_,
              cntl.ErrorText().c_str());
        return -1;
    }
    if (response.code() != 0) {
        PDLOG(WARNING, "check file[%s] failed. tid[%u] pid[%u]", file_name.c_str(), tid_, pid_);
        return -1;
    }
    PDLOG(INFO, "send file[%s] success. tid[%u] pid[%u]", file_name.c_str(), tid_, pid_);
    return 0;
}

int FileSender::SendDir(const std::string& dir_name, const std::string& full_path) {
    std::vector<std::string> file_vec;
    ::openmldb::base::GetFileName(full_path, file_vec);
    for (const std::string& file : file_vec) {
        if (SendFile(file.substr(file.find_last_of("/") + 1), dir_name, file) < 0) {
            return -1;
        }
    }
    return 0;
}

}  // namespace tablet
}  // namespace openmldb
