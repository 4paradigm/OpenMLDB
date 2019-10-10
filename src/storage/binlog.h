
// binlog.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-09-11
//

#pragma once

#include <memory>
#include <string>
#include "log/log_reader.h"
#include "storage/table.h"

typedef ::rtidb::base::Skiplist<uint32_t, uint64_t, ::rtidb::base::DefaultComparator> LogParts;

namespace rtidb {
namespace storage {

class Binlog {
public:
    Binlog(LogParts* log_part, const std::string& binlog_path);
    ~Binlog() = default;
    bool RecoverFromBinlog(std::shared_ptr<Table> table, uint64_t offset, uint64_t& latest_offset);

private:
    LogParts* log_part_;
    std::string log_path_;

};

}
}
