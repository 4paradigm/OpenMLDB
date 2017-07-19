//
// mem_log.h 
// Copyright 2017 elasticlog <elasticlog01@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RTIDB_MEM_LOG_H
#define RTIDB_MEM_LOG_H

#include <boost/atomic.hpp>
#include "base/skiplist.h"
#include "proto/tablet.pb.h"

using ::rtidb::base::Skiplist;
using ::rtidb::api::LogEntry;

namespace rtidb {
namespace replica {

// the asc comparator
struct LogOffsetComparator {
    int operator()(const uint64_t& a, const uint64_t& b) const {
        if (a < b) {
            return -1;
        }else if (a == b) {
            return 0;
        }
        return 1;
    }
};

typedef Skiplist<uint64_t, LogEntry*, LogOffsetComparator> Logs;

class MemLog {

public:
    MemLog();
    ~MemLog();

    // Need external synchroniztion
    void AppendLog(LogEntry* entry, uint64_t offset);
    void Ref();
    void UnRef();
    uint64_t GetHeadOffset();
    uint64_t GetTailOffset();
    Logs::Iterator* NewIterator();
private:
    Logs logs_;
    boost::atomic<uint64_t> refs_;
    boost::atomic<uint64_t> head_;
    boost::atomic<uint64_t> end_;
};

}
}
#endif /* !RTIDB_MEM_LOG_H */
