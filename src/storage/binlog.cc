
#include "base/status.h"
#include "base/schema_codec.h"
#include "base/hash.h"
#include "base/strings.h"
#include "storage/binlog.h"
#include "logging.h"
#include "timer.h"
#include "gflags/gflags.h"
#include "base/kv_iterator.h"
#include "base/flat_array.h"
#include "base/display.h"
#include "log/log_writer.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_uint64(gc_on_table_recover_count);
DECLARE_int32(binlog_name_length);

namespace rtidb {
namespace storage {

Binlog::Binlog(LogParts* log_part, const std::string& binlog_path) : 
        log_part_(log_part), log_path_(binlog_path) {}

bool Binlog::RecoverFromBinlog(std::shared_ptr<Table> table, uint64_t offset, uint64_t& latest_offset) {
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    PDLOG(INFO, "start recover table tid %u, pid %u from binlog with start offset %lu", tid, pid, offset);
    ::rtidb::log::LogReader log_reader(log_part_, log_path_);
    log_reader.SetOffset(offset);
    ::rtidb::api::LogEntry entry;
    uint64_t cur_offset = offset;
    std::string buffer;
    uint64_t succ_cnt = 0;
    uint64_t failed_cnt = 0;
    uint64_t consumed = ::baidu::common::timer::now_time();
    int last_log_index = log_reader.GetLogIndex();
    bool reach_end_log = true;
    while (true) {
        buffer.clear();
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.IsWaitRecord()) {
            int end_log_index = log_reader.GetEndLogIndex();
            int cur_log_index = log_reader.GetLogIndex();
            if (end_log_index >= 0 && end_log_index > cur_log_index) {
                log_reader.RollRLogFile();
                PDLOG(WARNING, "read new binlog file. tid[%u] pid[%u] cur_log_index[%d] end_log_index[%d] cur_offset[%lu]", 
                                tid, pid, cur_log_index, end_log_index, cur_offset);
                continue;
            }
            consumed = ::baidu::common::timer::now_time() - consumed;
            PDLOG(INFO, "table tid %u pid %u completed, succ_cnt %lu, failed_cnt %lu, consumed %us",
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
            PDLOG(WARNING, "fail parse record for tid %u, pid %u with value %s", 
                        tid, pid, ::rtidb::base::DebugString(record.ToString()).c_str());
            failed_cnt++;
            continue;
        }

        if (cur_offset >= entry.log_index()) {
            PDLOG(DEBUG, "offset %lu has been made snapshot", entry.log_index());
            continue;
        }

        if (cur_offset + 1 != entry.log_index()) {
            PDLOG(WARNING, "missing log entry cur_offset %lu , new entry offset %lu for tid %u, pid %u",
                  cur_offset, entry.log_index(), tid, pid);
        }
        
        if (entry.has_method_type() && entry.method_type() == ::rtidb::api::MethodType::kDelete) {
            if (entry.dimensions_size() == 0) {
                PDLOG(WARNING, "no dimesion. tid %u pid %u offset %lu", tid, pid, entry.log_index());
            } else {
                table->Delete(entry.dimensions(0).key(), entry.dimensions(0).idx());
            }
        } else {
            table->Put(entry);
        }
        cur_offset = entry.log_index();
        succ_cnt++;
        if (succ_cnt % 100000 == 0) {
            PDLOG(INFO, "[Recover] load data from binlog succ_cnt %lu, failed_cnt %lu for tid %u, pid %u",
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
        PDLOG(DEBUG, "last record end offset[%lu] tid[%u] pid[%u]", pos, tid, pid);
        std::string full_path = log_path_ + "/" +
                            ::rtidb::base::FormatToString(log_index, FLAGS_binlog_name_length) + ".log";
        FILE* fd = fopen(full_path.c_str(), "rb+");
        if (fd == NULL) {
            PDLOG(WARNING, "fail to open file %s", full_path.c_str());
            return false;
        }
        if (fseek(fd, pos, SEEK_SET) != 0) {
            PDLOG(WARNING, "fail to seek. file[%s] pos[%lu]", full_path.c_str(), pos);
            return false;
        }
        ::rtidb::log::WriteHandle wh(full_path, fd, pos);
        wh.EndLog();
        PDLOG(INFO, "append endlog record ok. file[%s]", full_path.c_str());
    }
    return true;
}

bool Binlog::DumpBinlogIndexData(std::shared_ptr<Table>& table, const ::rtidb::common::ColumnKey& column_key, uint32_t idx, std::vector<::rtidb::log::WriteHandle*>& whs, uint64_t offset) {
    uint32_t tid = table->GetId();
    uint32_t pid = table->GetPid();
    ::rtidb::log::LogReader log_reader(log_part_, log_path_);
    log_reader.SetOffset(offset);
    ::rtidb::api::LogEntry entry;
    uint64_t cur_offset = offset;
    std::string buffer;
    uint64_t succ_cnt = 0;
    uint64_t failed_cnt = 0;
    uint64_t consumed = ::baidu::common::timer::now_time();
    int last_log_index = log_reader.GetLogIndex();
    bool reach_end_log = true;
    uint32_t partition_num = whs.size();
    std::string schema = table->GetSchema();
    std::vector<::rtidb::base::ColumnDesc> columns;
    if (!schema.empty()) {
        ::rtidb::base::SchemaCodec codec;
        codec.Decode(schema, columns);
    } else {
        return true;
    }
    std::map<std::string, uint32_t> column_desc_map;
    for (uint32_t i = 0; i < columns.size(); ++i) {
        column_desc_map.insert(std::make_pair(columns[i].name, i));
    }
    std::vector<uint32_t> index_cols;
    for (const auto& name : column_key.col_name()) {
        if (column_desc_map.count(name)) {
            index_cols.push_back(column_desc_map[name]);
        } else {
            PDLOG(WARNING, "fail to find column_desc %s", name.c_str());
            return false;
        }
    }
    if (index_cols.size() < 1) {
        return true;
    }

    while (true) {
        buffer.clear();
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = log_reader.ReadNextRecord(&record, &buffer);
        if (status.IsWaitRecord()) {
            int end_log_index = log_reader.GetEndLogIndex();
            int cur_log_index = log_reader.GetLogIndex();
            if (end_log_index >= 0 && end_log_index > cur_log_index) {
                log_reader.RollRLogFile();
                PDLOG(WARNING, "read new binlog file. tid[%u] pid[%u] cur_log_index[%d] end_log_index[%d] cur_offset[%lu]", 
                                tid, pid, cur_log_index, end_log_index, cur_offset);
                continue;
            }
            consumed = ::baidu::common::timer::now_time() - consumed;
            PDLOG(INFO, "table tid %u pid %u completed, succ_cnt %lu, failed_cnt %lu, consumed %us",
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
            PDLOG(WARNING, "fail parse record for tid %u, pid %u with value %s", 
                        tid, pid, ::rtidb::base::DebugString(record.ToString()).c_str());
            failed_cnt++;
            continue;
        }

        if (cur_offset >= entry.log_index()) {
            PDLOG(DEBUG, "offset %lu has been made snapshot", entry.log_index());
            continue;
        }

        if (cur_offset + 1 != entry.log_index()) {
            PDLOG(WARNING, "missing log entry cur_offset %lu , new entry offset %lu for tid %u, pid %u",
                  cur_offset, entry.log_index(), tid, pid);
        }
        
        std::set<uint32_t> pid_set;
        for (const auto& dim : entry.dimensions()) {
            pid_set.insert(::rtidb::base::hash64(dim.key())%partition_num);
        }
        std::string buff;
        if (table->GetCompressType() == ::rtidb::api::kSnappy) {
            std::string uncompressed;
            ::snappy::Uncompress(entry.value().c_str(), entry.value().size(), &buff);
        } else {
            buff = entry.value();
        }
        std::vector<std::string> row;
        ::rtidb::base::FillTableRow(columns, buff.c_str(), buff.size(), row);
        std::string cur_key;
        for (uint32_t i : index_cols) {
            if (cur_key.empty()) {
                cur_key = row[i];
            } else {
                cur_key += "|" + row[i];
            }
        }
        uint32_t index_pid = ::rtidb::base::hash64(cur_key)%partition_num;
        if (!pid_set.count(index_pid)) {
            ::rtidb::api::Dimension* dim = entry.add_dimensions();
            dim->set_key(cur_key);
            dim->set_idx(idx);
            ::rtidb::base::Status status = whs[index_pid]->Write(record);
            if (!status.ok()) {
                PDLOG(WARNING, "fail to dump index entrylog in binlog to pid[%u].", index_pid);
                return false;
            }
        }

        cur_offset = entry.log_index();
        succ_cnt++;
    }

    return true;
}

}
}
