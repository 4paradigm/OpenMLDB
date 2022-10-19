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

#include "storage/binlog.h"

#include <map>
#include <set>
#include <utility>
#include <vector>

#include "base/glog_wrapper.h"
#include "base/hash.h"
#include "base/strings.h"
#include "codec/schema_codec.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "log/log_writer.h"
#include "log/status.h"

DECLARE_uint64(gc_on_table_recover_count);
DECLARE_int32(binlog_name_length);

namespace openmldb {
namespace storage {

Binlog::Binlog(LogParts* log_part, const std::string& binlog_path) : log_part_(log_part), log_path_(binlog_path) {}

bool Binlog::RecoverFromBinlog(std::shared_ptr<Table> table, uint64_t offset, uint64_t& latest_offset) {
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    PDLOG(INFO, "start recover table tid %u, pid %u from binlog with start offset %lu", tid, pid, offset);
    ::openmldb::log::LogReader log_reader(log_part_, log_path_, false);
    log_reader.SetOffset(offset);
    ::openmldb::api::LogEntry entry;
    uint64_t cur_offset = offset;
    std::string buffer;
    uint64_t succ_cnt = 0;
    uint64_t failed_cnt = 0;
    uint64_t consumed = ::baidu::common::timer::now_time();
    int last_log_index = log_reader.GetLogIndex();
    bool reach_end_log = true;
    while (true) {
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
                      tid, pid, cur_log_index, end_log_index, cur_offset);
                continue;
            }
            consumed = ::baidu::common::timer::now_time() - consumed;
            PDLOG(INFO,
                  "table tid %u pid %u completed, succ_cnt %lu, failed_cnt "
                  "%lu, consumed %us",
                  tid, pid, succ_cnt, failed_cnt, consumed);
            reach_end_log = false;
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
        bool ok = entry.ParseFromString(record.ToString());
        if (!ok) {
            PDLOG(WARNING, "fail parse record for tid %u, pid %u with value %s", tid, pid,
                  ::openmldb::base::DebugString(record.ToString()).c_str());
            failed_cnt++;
            continue;
        }

        if (cur_offset >= entry.log_index()) {
            DEBUGLOG("offset %lu has been made snapshot", entry.log_index());
            continue;
        }

        if (cur_offset + 1 != entry.log_index()) {
            PDLOG(WARNING,
                  "missing log entry cur_offset %lu , new entry offset %lu for "
                  "tid %u, pid %u",
                  cur_offset, entry.log_index(), tid, pid);
        }

        if (entry.has_method_type() && entry.method_type() == ::openmldb::api::MethodType::kDelete) {
            if (entry.dimensions_size() == 0) {
                PDLOG(WARNING, "no dimesion. tid %u pid %u offset %lu", tid, pid, entry.log_index());
            } else {
                if (entry.has_ts()) {
                    table->Delete(entry.dimensions(0).key(), entry.dimensions(0).idx(), entry.ts());
                } else {
                    table->Delete(entry.dimensions(0).key(), entry.dimensions(0).idx());
                }
            }
        } else {
            table->Put(entry);
        }
        cur_offset = entry.log_index();
        succ_cnt++;
        if (succ_cnt % 100000 == 0) {
            PDLOG(INFO,
                  "[Recover] load data from binlog succ_cnt %lu, failed_cnt "
                  "%lu for tid %u, pid %u",
                  succ_cnt, failed_cnt, tid, pid);
        }
        if (succ_cnt % FLAGS_gc_on_table_recover_count == 0) {
            table->SchedGc();
        }
    }
    latest_offset = cur_offset;
    if (!reach_end_log) {
        int log_index = log_reader.GetLogIndex();
        if (log_index < 0) {
            PDLOG(INFO, "no binglog available. tid[%u] pid[%u]", tid, pid);
            return true;
        }
        uint64_t pos = log_reader.GetLastRecordEndOffset();
        DEBUGLOG("last record end offset[%lu] tid[%u] pid[%u]", pos, tid, pid);
        std::string full_path =
            log_path_ + "/" + ::openmldb::base::FormatToString(log_index, FLAGS_binlog_name_length) + ".log";
        FILE* fd = fopen(full_path.c_str(), "rb+");
        if (fd == NULL) {
            PDLOG(WARNING, "fail to open file %s", full_path.c_str());
            return false;
        }
        if (fseek(fd, pos, SEEK_SET) != 0) {
            PDLOG(WARNING, "fail to seek. file[%s] pos[%lu]", full_path.c_str(), pos);
            return false;
        }
        ::openmldb::log::WriteHandle wh("off", full_path, fd, pos);
        wh.EndLog();
        PDLOG(INFO, "append endlog record ok. file[%s]", full_path.c_str());
    }
    return true;
}

}  // namespace storage
}  // namespace openmldb
