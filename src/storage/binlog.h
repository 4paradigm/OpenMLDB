
// binlog.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-09-11
//

#pragma once

#include <memory>
#include <string>
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "storage/table.h"

typedef ::rtidb::base::Skiplist<uint32_t, uint64_t, ::rtidb::base::DefaultComparator> LogParts;

namespace rtidb {
namespace storage {

class Binlog {
public:
    Binlog(LogParts* log_part, const std::string& binlog_path);
    ~Binlog() = default;
    bool RecoverFromBinlog(std::shared_ptr<Table> table, uint64_t offset, uint64_t& latest_offset);

    bool DumpBinlogIndexData(std::shared_ptr<Table>& table, const ::rtidb::common::ColumnKey& column_key, std::vector<::rtidb::log::WriteHandle*>& whs, uint64_t& lasest_offset);

private:
    LogParts* log_part_;
    std::string log_path_;

};

}
}
